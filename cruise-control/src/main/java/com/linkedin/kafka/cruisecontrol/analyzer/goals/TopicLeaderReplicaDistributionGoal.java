/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Soft goal to balance collocations of leader replicas of the same topic over alive brokers not excluded for replica moves.
 * <ul>
 * <li>Under: (the average number of topic leader replicas per broker) * (1 + topic leader replica count balance percentage)</li>
 * <li>Above: (the average number of topic leader replicas per broker) * Math.max(0, 1 - topic leader replica count balance percentage)</li>
 * </ul>
 *
 * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG
 * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG
 * @see #balancePercentageWithMargin(OptimizationOptions)
 * <p>
 * Behavior notes:
 * - Prefers leadership transfers over replica moves to reduce data movement.
 * - In fix-offline-replicas-only mode, relaxes balance limits to evacuate offline replicas/leaders.
 * - Brokers excluded for replica moves can still shed replicas and gain leadership via transfer, but cannot receive new replicas.
 */
public class TopicLeaderReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG =
      LoggerFactory.getLogger(TopicLeaderReplicaDistributionGoal.class);

  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the replica balance limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;
  // Cached sort name for the tracked leaders-only view used by this goal.
  private final String _sortName = replicaSortName(this, false, true);

  private final Map<String, Set<Integer>> _brokerIdsAboveBalanceUpperLimitByTopic;
  private final Map<String, Set<Integer>> _brokerIdsUnderBalanceLowerLimitByTopic;
  // Must contain only the topics to be rebalanced.
  private final Map<String, Double> _avgTopicLeaderReplicasOnAliveBroker;
  // Must contain all topics to ensure that the lower priority goals work w/o an NPE.
  private final Map<String, Integer> _balanceUpperLimitByTopic;
  private final Map<String, Integer> _balanceLowerLimitByTopic;
  // This is used to identify brokers not excluded for replica moves.
  private Set<Integer> _brokersAllowedReplicaMove;

  /**
   * A soft goal to balance collocations of replicas of the same topic.
   */
  public TopicLeaderReplicaDistributionGoal() {
    _brokerIdsAboveBalanceUpperLimitByTopic = new HashMap<>();
    _brokerIdsUnderBalanceLowerLimitByTopic = new HashMap<>();
    _avgTopicLeaderReplicasOnAliveBroker = new HashMap<>();
    _balanceUpperLimitByTopic = new HashMap<>();
    _balanceLowerLimitByTopic = new HashMap<>();
  }

  public TopicLeaderReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
  }

  /**
   * Convert the configured balance percentage (multiplicative factor > 1.0) into an effective fractional slack
   * around the average, applying optional widening for violation runs and a hysteresis margin to reduce thrash.
   * Example: if percentage=1.15 and margin=0.9, effective slack τ = (1.15 - 1) * 0.9 = 0.135 (~13.5%).
   * This τ is used to compute raw lower/upper, which are then clamped by integer gap settings.
   *
   * @param optimizationOptions Options to widen the band when triggered by goal-violation detector.
   * @return Effective fractional slack τ used to compute raw lower/upper bounds.
   */
  private double balancePercentageWithMargin(OptimizationOptions optimizationOptions) {
    double balancePercentage = _balancingConstraint.topicLeaderReplicaBalancePercentage();
    if (optimizationOptions.isTriggeredByGoalViolation()) {
      balancePercentage *= _balancingConstraint.goalViolationDistributionThresholdMultiplier();
    }
    return (balancePercentage - 1) *
        _balancingConstraint.topicLeaderReplicaDistributionGoalBalanceMargin();
  }

  /**
   * Clamp a raw per-broker limit (computed from percentage band) into the integer interval defined by
   * gap settings around the average.
   * - Lower clamping is anchored at floor(avg): [floor(avg)-maxGap, floor(avg)-minGap] (never < 0)
   * - Upper clamping is anchored at ceil(avg):  [ceil(avg)+minGap, ceil(avg)+maxGap]
   *
   * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_DOC
   * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_DOC
   */
  private int clamp(int value, int lo, int hi) {
    return Math.max(lo, Math.min(value, hi));
  }

  private int clampLower(int computed, int floorAvg) {
    int minGap = _balancingConstraint.topicLeaderReplicaBalanceMinGap();
    int maxGap = _balancingConstraint.topicLeaderReplicaBalanceMaxGap();
    int minLimit = Math.max(0, floorAvg - maxGap);
    int maxLimit = Math.max(0, floorAvg - minGap);
    return clamp(computed, minLimit, maxLimit);
  }

  private int clampUpper(int computed, int ceilAvg) {
    int minGap = _balancingConstraint.topicLeaderReplicaBalanceMinGap();
    int maxGap = _balancingConstraint.topicLeaderReplicaBalanceMaxGap();
    int minLimit = ceilAvg + minGap;
    int maxLimit = ceilAvg + maxGap;
    return clamp(computed, minLimit, maxLimit);
  }

  /**
   * @param topic               Topic for which the upper limit is requested.
   * @param optimizationOptions Options to adjust balance upper limit in case goal optimization is triggered by goal
   *                            violation detector.
   * @return The topic replica balance upper threshold in number of topic replicas.
   */
  private int balanceUpperLimit(String topic, OptimizationOptions optimizationOptions) {
    double avg = _avgTopicLeaderReplicasOnAliveBroker.get(topic);
    int computedUpperLimit =
        (int) Math.ceil(avg * (1 + balancePercentageWithMargin(optimizationOptions)));
    return clampUpper(computedUpperLimit, (int) Math.ceil(avg));
  }

  /**
   * @param topic               Topic for which the lower limit is requested.
   * @param optimizationOptions Options to adjust balance lower limit in case goal optimization is triggered by goal
   *                            violation detector.
   * @return The replica balance lower threshold in number of topic replicas.
   */
  private int balanceLowerLimit(String topic, OptimizationOptions optimizationOptions) {
    double avg = _avgTopicLeaderReplicasOnAliveBroker.get(topic);
    int computedLowerLimit =
        (int) Math.floor(avg * Math.max(0, (1 - balancePercentageWithMargin(optimizationOptions))));
    return clampLower(computedLowerLimit, (int) Math.floor(avg));
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of topic replicas at
   * (1) the source broker does not go under the allowed limit -- unless the source broker is excluded for replica moves.
   * (2) the destination broker does not go over the allowed limit.
   *
   * @param action       Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();
    Replica sourceReplica = sourceBroker.replica(action.topicPartition());

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        String destinationTopic = action.destinationTopic();
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        if (sourceTopic.equals(destinationTopic) && sourceReplica.isLeader() &&
            destinationReplica.isLeader()) {
          return ACCEPT;
        }
        if (!sourceReplica.isLeader() && !destinationReplica.isLeader()) {
          return ACCEPT;
        }
        if (sourceReplica.isLeader() && !destinationReplica.isLeader()) {
          return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker) ?
              ACCEPT : REPLICA_REJECT;
        }
        if (!sourceReplica.isLeader() && destinationReplica.isLeader()) {
          return isLeadershipGoalSatisfiable(destinationTopic, destinationBroker, sourceBroker) ?
              ACCEPT : REPLICA_REJECT;
        }
        // Both replicas are leaders but for different topics
        return (isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker)
            && isLeadershipGoalSatisfiable(destinationTopic, destinationBroker, sourceBroker)) ?
            ACCEPT : REPLICA_REJECT;
      case LEADERSHIP_MOVEMENT:
        return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker) ? ACCEPT :
            REPLICA_REJECT;
      case INTER_BROKER_REPLICA_MOVEMENT:
        if (!sourceReplica.isLeader()) {
          return ACCEPT;
        }
        return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker) ? ACCEPT :
            REPLICA_REJECT;
      default:
        throw new IllegalArgumentException(
            "Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isLeadershipGoalSatisfiable(String sourceLeaderTopic, Broker sourceBroker,
                                              Broker destinationBroker) {
    return isTopicLeaderCountUnderUpperLimitAfterChange(sourceLeaderTopic, destinationBroker)
        && (isExcludedForReplicaMove(sourceBroker)
        || isTopicLeaderCountAboveLowerLimitAfterChange(sourceLeaderTopic, sourceBroker));
  }

  private boolean isTopicLeaderCountUnderUpperLimitAfterChange(String topic,
                                                               Broker broker) {
    int numTopicLeaders = broker.numLeadersFor(topic);
    int brokerBalanceUpperLimit = broker.isAlive() ? _balanceUpperLimitByTopic.get(topic) : 0;

    return numTopicLeaders + 1 <= brokerBalanceUpperLimit;
  }

  private boolean isTopicLeaderCountAboveLowerLimitAfterChange(String topic,
                                                               Broker broker) {
    int numTopicLeaders = broker.numLeadersFor(topic);
    int brokerBalanceLowerLimit = broker.isAlive() ? _balanceLowerLimitByTopic.get(topic) : 0;

    return numTopicLeaders - 1 >= brokerBalanceLowerLimit;
  }

  /**
   * Check whether the given broker is excluded for replica moves.
   * Such a broker cannot receive replicas, but can give them away.
   *
   * @param broker Broker to check for exclusion from replica moves.
   * @return {@code true} if the given broker is excluded for replica moves, {@code false} otherwise.
   */
  private boolean isExcludedForReplicaMove(Broker broker) {
    return !_brokersAllowedReplicaMove.contains(broker.id());
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new GoalUtils.HardGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public String name() {
    return TopicLeaderReplicaDistributionGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    // Soft goal: other hard goals may constrain moves; this goal aims to improve distribution with minimal churn.
    return false;
  }

  /**
   * Initiates this goal.
   *
   * @param clusterModel        The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override

  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    Set<String> topicsToRebalance = GoalUtils.topicsToRebalance(clusterModel, excludedTopics);
    LOG.info(
        "[{}] initGoalState starting. fixOfflineReplicasOnly={}, onlyMoveImmigrantReplicas={}, excludedTopics={}",
        name(), _fixOfflineReplicasOnly, optimizationOptions.onlyMoveImmigrantReplicas(),
        excludedTopics);

    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    _brokersAllowedReplicaMove =
        GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (_brokersAllowedReplicaMove.isEmpty()) {
      // Leadership-only balancing is still possible; proceed without replica movements.
      LOG.info(
          "[{}] All alive brokers are excluded from replica moves; proceeding with leadership-only balancing.",
          name());
    }
    // Initialize the average replicas on an alive broker.
    final Map<String, Integer> numTopicLeadersMap =
        clusterModel.numLeadersPerTopic(clusterModel.topics());
    for (String topic : clusterModel.topics()) {
      int numTopicLeaders = numTopicLeadersMap.get(topic);
      _avgTopicLeaderReplicasOnAliveBroker.put(topic,
          (numTopicLeaders / (double) _brokersAllowedReplicaMove.size()));
      _balanceUpperLimitByTopic.put(topic, balanceUpperLimit(topic, optimizationOptions));
      _balanceLowerLimitByTopic.put(topic, balanceLowerLimit(topic, optimizationOptions));
      // Retain only the topics to rebalance in _avgTopicReplicasOnAliveBroker
      if (!topicsToRebalance.contains(topic)) {
        _avgTopicLeaderReplicasOnAliveBroker.remove(topic);
      }
    }
    // Log leader distribution per topic for debugging
    for (String t : _avgTopicLeaderReplicasOnAliveBroker.keySet()) {
      Map<Integer, Integer> leadersPerBroker = new TreeMap<>();
      for (Broker b : clusterModel.brokers()) {
        leadersPerBroker.put(b.id(), b.numLeadersFor(t));
      }
      LOG.info("[{}] Topic '{}' leaders per broker: {} | avg={} lowerLimit={} upperLimit={}",
          name(), t, leadersPerBroker, _avgTopicLeaderReplicasOnAliveBroker.get(t),
          _balanceLowerLimitByTopic.get(t), _balanceUpperLimitByTopic.get(t));
    }

    // Filter out replicas to be considered for replica movement.
    for (Broker broker : clusterModel.brokers()) {
      new SortedReplicasHelper().maybeAddSelectionFunc(
              ReplicaSortFunctionFactory.selectImmigrants(),
              optimizationOptions.onlyMoveImmigrantReplicas())
          .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrantOrOfflineReplicas(),
              !clusterModel.selfHealingEligibleReplicas().isEmpty() && broker.isAlive())
          .addSelectionFunc(
              ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
          .addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
          .trackSortedReplicasFor(_sortName, broker);
    }

    _fixOfflineReplicasOnly = false;
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action       Action containing information about potential modification to the given cluster model. Assumed to be
   *                     of type {@link ActionType#INTER_BROKER_REPLICA_MOVEMENT}.
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // The action must be executed if currently fixing offline replicas only and the offline source replica is proposed
    // to be moved to another broker.
    if (_fixOfflineReplicasOnly &&
        sourceBroker.replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }

    //Check that destination and source would not become unbalanced.
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    return isLeadershipGoalSatisfiable(sourceTopic, sourceBroker, destinationBroker);
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   * @param clusterModel        The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (!_brokerIdsAboveBalanceUpperLimitByTopic.isEmpty()) {
      _brokerIdsAboveBalanceUpperLimitByTopic.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimitByTopic.isEmpty()) {
      _brokerIdsUnderBalanceLowerLimitByTopic.clear();
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        LOG.warn(
            "[{}] Still seeing offline replicas during updateGoalState while fixOfflineReplicasOnly={}, will escalate.",
            name(), _fixOfflineReplicasOnly);

        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.info("Ignoring topic replica balance limit to move replicas from dead brokers/disks.");
      return;
    }
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  private static boolean skipBrokerRebalance(Broker broker,
                                             ClusterModel clusterModel,
                                             Collection<Replica> leaderReplicas,
                                             boolean requireLessReplicas,
                                             boolean requireMoreReplicas,
                                             boolean hasOfflineTopicReplicas,
                                             boolean moveImmigrantReplicaOnly) {
    if (shouldSkipBecauseWithinLimits(broker, requireLessReplicas, requireMoreReplicas)) {
      return true;
    }
    if (shouldSkipBecauseNewBrokerPlacement(clusterModel, broker, requireLessReplicas)) {
      return true;
    }
    if (shouldSkipInSelfHealing(clusterModel, broker, leaderReplicas, requireLessReplicas,
        hasOfflineTopicReplicas)) {
      return true;
    }
    if (shouldSkipDueToImmigrantOnly(moveImmigrantReplicaOnly, broker, leaderReplicas,
        requireLessReplicas)) {
      return true;
    }
    return false;
  }

  private static boolean shouldSkipBecauseWithinLimits(Broker broker, boolean requireLess,
                                                       boolean requireMore) {
    if (broker.isAlive() && !requireMore && !requireLess) {
      LOG.trace("Skip rebalance: Broker {} is already within the limit for replicas.", broker);
      return true;
    }
    return false;
  }

  private static boolean shouldSkipBecauseNewBrokerPlacement(ClusterModel clusterModel,
                                                             Broker broker, boolean requireLess) {
    if (!clusterModel.newBrokers().isEmpty() && !broker.isNew() && !requireLess) {
      LOG.trace(
          "Skip rebalance: Cluster has new brokers and this broker {} is not new, but does not require less load.",
          broker);
      return true;
    }
    return false;
  }

  private static boolean shouldSkipInSelfHealing(ClusterModel clusterModel,
                                                 Broker broker,
                                                 Collection<Replica> leaderReplicas,
                                                 boolean requireLess,
                                                 boolean hasOffline) {
    boolean hasImmigrant = leaderReplicas.stream()
        .anyMatch(replica -> replica.isLeader() && broker.immigrantReplicas().contains(replica));
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty() && requireLess && !hasOffline &&
        !hasImmigrant) {
      LOG.trace(
          "Skip rebalance: Self-healing active; broker {} requires less, but none of its current offline or immigrant replicas are from the topic.",
          broker);
      return true;
    }
    return false;
  }

  private static boolean shouldSkipDueToImmigrantOnly(boolean moveImmigrantOnly,
                                                      Broker broker,
                                                      Collection<Replica> leaderReplicas,
                                                      boolean requireLess) {
    boolean hasImmigrant = leaderReplicas.stream()
        .anyMatch(replica -> replica.isLeader() && broker.immigrantReplicas().contains(replica));
    if (moveImmigrantOnly && requireLess && !hasImmigrant) {
      LOG.trace(
          "Skip rebalance: Only immigrant replicas can be moved, but broker {} has none for this topic.",
          broker);
      return true;
    }
    return false;
  }

  private boolean isTopicExcludedFromRebalance(String topic) {
    return _avgTopicLeaderReplicasOnAliveBroker.get(topic) == null;
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker              Broker to be balanced.
   * @param clusterModel        The state of the cluster.
   * @param optimizedGoals      Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(),
        _balanceLowerLimitByTopic,
        _balanceUpperLimitByTopic);

    for (String topic : broker.topics()) {
      if (isTopicExcludedFromRebalance(topic)) {
        continue;
      }

      Collection<Replica> leaderReplicas = broker.trackedSortedReplicas(_sortName)
          .sortedReplicas(false).stream()
          .filter(Replica::isLeader)
          .filter(r -> r.topicPartition().topic().equals(topic))
          .collect(Collectors.toList());
      long numTopicLeaders = leaderReplicas.size();
      int numOfflineTopicLeaders =
          GoalUtils.retainCurrentOfflineBrokerReplicas(broker, leaderReplicas).size();
      boolean isExcludedForReplicaMove = isExcludedForReplicaMove(broker);

      boolean requireLessLeaders =
          numOfflineTopicLeaders > 0 || numTopicLeaders > _balanceUpperLimitByTopic.get(topic)
              || isExcludedForReplicaMove;
      boolean requireMoreLeaders = !isExcludedForReplicaMove && broker.isAlive()
          && numTopicLeaders - numOfflineTopicLeaders < _balanceLowerLimitByTopic.get(topic);

      if (skipBrokerRebalance(broker, clusterModel, leaderReplicas, requireLessLeaders,
          requireMoreLeaders, numOfflineTopicLeaders > 0,
          optimizationOptions.onlyMoveImmigrantReplicas())) {
        continue;
      }

      // Update broker ids over the balance limit for logging purposes.
      if (requireLessLeaders &&
          rebalanceByMovingLeadersOut(broker, topic, clusterModel, optimizedGoals,
              optimizationOptions)) {
        _brokerIdsAboveBalanceUpperLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>())
            .add(broker.id());
        LOG.debug(
            "Failed to sufficiently decrease leaders of topic {} in broker {} with replica movements. Replicas: {}.",
            topic, broker.id(), broker.numLeadersFor(topic));
      }
      if (requireMoreLeaders &&
          rebalanceByMovingLeadersIn(broker, topic, clusterModel, optimizedGoals,
              optimizationOptions)) {
        _brokerIdsUnderBalanceLowerLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>())
            .add(broker.id());
        LOG.debug(
            "Failed to sufficiently increase leaders of topic {} in broker {} with replica movements. Replicas: {}.",
            topic, broker.id(), broker.numLeadersFor(topic));
      }
      if (!_brokerIdsAboveBalanceUpperLimitByTopic.getOrDefault(topic, Collections.emptySet())
          .contains(broker.id())
          && !_brokerIdsUnderBalanceLowerLimitByTopic.getOrDefault(topic, Collections.emptySet())
          .contains(broker.id())) {
        LOG.debug(
            "Successfully balanced leaders of topic {} in broker {} by moving replicas. Replicas: {}",
            topic, broker.id(), broker.numLeadersFor(topic));
      }
    }
  }

  private Set<Replica> leadersOfTopicInBroker(Broker broker, String topic) {
    return broker.replicasOfTopicInBroker(topic).stream().filter(Replica::isLeader)
        .collect(Collectors.toSet());
  }

  private SortedSet<Replica> replicasToMoveOut(Broker broker, String topic) {
    SortedSet<Replica> replicasToMoveOut = new TreeSet<>(broker.replicaComparator());
    replicasToMoveOut.addAll(leadersOfTopicInBroker(broker, topic));
    replicasToMoveOut.retainAll(broker.trackedSortedReplicas(_sortName).sortedReplicas(false));
    return replicasToMoveOut;
  }

  private boolean rebalanceByMovingLeadersOut(Broker broker,
                                              String topic,
                                              ClusterModel clusterModel,
                                              Set<Goal> optimizedGoals,
                                              OptimizationOptions optimizationOptions) {
    // Get the eligible brokers.
    // Get the eligible brokers. Prefer those with fewer leaders of the topic, fewer leaders overall, then lower id.
    SortedSet<Broker> candidateBrokers = new TreeSet<>(
        Comparator.comparingInt((Broker b) -> b.numLeadersFor(topic))
            .thenComparingInt(b -> b.leaderReplicas().size())
            .thenComparingInt(Broker::id));

    candidateBrokers.addAll(_fixOfflineReplicasOnly ? clusterModel.aliveBrokers() : clusterModel
        .aliveBrokers()
        .stream()
        .filter(b -> b.numLeadersFor(topic) < _balanceUpperLimitByTopic.get(topic))
        .collect(Collectors.toSet()));

    Collection<Replica> leadersOfTopicInBroker = leadersOfTopicInBroker(broker, topic);
    int numLeadersOfTopicInBroker = leadersOfTopicInBroker.size();
    int numOfflineTopicReplicas =
        GoalUtils.retainCurrentOfflineBrokerReplicas(broker, leadersOfTopicInBroker).size();
    // Do not force-drain excluded brokers; respect the same upper limit.
    Integer upperObj = _balanceUpperLimitByTopic.get(topic);
    if (upperObj == null) {
      LOG.warn("No upper limit for topic {} found when moving leaders out; skipping.", topic);
      return false;
    }
    int balanceUpperLimitForSourceBroker = upperObj;

    boolean wasUnableToMoveOfflineReplica = false;
    for (Replica replica : replicasToMoveOut(broker, topic)) {
      if (wasUnableToMoveOfflineReplica && !replica.isCurrentOffline() &&
          numLeadersOfTopicInBroker <= balanceUpperLimitForSourceBroker) {
        // Was unable to move offline replicas from the broker, and remaining replica count is under the balance limit.
        return false;
      }

      boolean wasOffline = replica.isCurrentOffline();
      // Build destination candidates by action type for this partition.
      Set<Broker> leadershipDests = candidateBrokers.stream()
          .filter(dest -> clusterModel.partition(replica.topicPartition()).followerBrokers()
              .contains(dest))
          .collect(Collectors.toSet());
      Set<Broker> replicaDests = candidateBrokers.stream()
          .filter(dest -> dest.replica(replica.topicPartition()) == null)
          .collect(Collectors.toSet());
      // Prefer cheaper leadership transfer first, then fall back to replica movement.
      Broker b = maybeApplyBalancingAction(clusterModel, replica, leadershipDests,
          ActionType.LEADERSHIP_MOVEMENT,
          optimizedGoals, optimizationOptions);
      if (b == null) {
        b = maybeApplyBalancingAction(clusterModel, replica, replicaDests,
            ActionType.INTER_BROKER_REPLICA_MOVEMENT,
            optimizedGoals, optimizationOptions);
      }
      // Only check if we successfully moved something.
      if (b != null) {
        if (wasOffline) {
          numOfflineTopicReplicas--;
        }
        if (--numLeadersOfTopicInBroker <=
            (numOfflineTopicReplicas == 0 ? balanceUpperLimitForSourceBroker : 0)) {
          return false;
        }

        // Remove and reinsert the broker so the order is correct.
        // Because a TreeSet is used here, and lookups are by comparator first, I'm seeing failed deletes
        final int brokerId = b.id();
        boolean isRemoved = candidateBrokers.removeIf(cb -> cb.id() == brokerId);
        LOG.debug("Removed broker {} from candidateBrokers: {}", b.id(), isRemoved);
        if (b.numLeadersFor(topic) < _balanceUpperLimitByTopic.get(topic) ||
            _fixOfflineReplicasOnly) {
          candidateBrokers.add(b);
        }
      } else if (wasOffline) {
        wasUnableToMoveOfflineReplica = true;
      }
    }
    // All the topic replicas has been moved away from the broker.
    return !leadersOfTopicInBroker(broker, topic).isEmpty();
  }

  private boolean rebalanceByMovingLeadersIn(Broker aliveDestBroker,
                                             String topic,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             OptimizationOptions optimizationOptions) {
    // Precompute per-broker stats for comparator efficiency
    Map<Integer, Integer> offlineByBroker = new HashMap<>();
    Map<Integer, Integer> topicLeadersByBroker = new HashMap<>();
    for (Broker b : clusterModel.brokers()) {
      Collection<Replica> leaders = leadersOfTopicInBroker(b, topic);
      topicLeadersByBroker.put(b.id(), leaders.size());
      offlineByBroker.put(b.id(), GoalUtils.retainCurrentOfflineBrokerReplicas(b, leaders).size());
    }

    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>((b1, b2) -> {
      int resultByOfflineLeaders = Integer.compare(offlineByBroker.getOrDefault(b2.id(), 0),
          offlineByBroker.getOrDefault(b1.id(), 0));
      if (resultByOfflineLeaders == 0) {
        int resultByTopicLeaders = Integer.compare(topicLeadersByBroker.getOrDefault(b2.id(), 0),
            topicLeadersByBroker.getOrDefault(b1.id(), 0));
        if (resultByTopicLeaders == 0) {
          int resultByAllLeaders =
              Integer.compare(b2.leaderReplicas().size(), b1.leaderReplicas().size());
          return resultByAllLeaders == 0 ? Integer.compare(b2.id(), b1.id()) : resultByAllLeaders;
        } else {
          return resultByTopicLeaders;
        }
      }
      return resultByOfflineLeaders;
    });

    // Source broker can be dead, alive, or may have bad disks.
    if (_fixOfflineReplicasOnly) {
      clusterModel.brokers().stream()
          .filter(sourceBroker -> sourceBroker.id() != aliveDestBroker.id())
          .forEach(eligibleBrokers::add);
    } else {
      for (Broker sourceBroker : clusterModel.brokers()) {
        if (sourceBroker.numLeadersFor(topic) > _balanceLowerLimitByTopic.get(topic)
            || !sourceBroker.currentOfflineReplicas().isEmpty() ||
            isExcludedForReplicaMove(sourceBroker)) {
          eligibleBrokers.add(sourceBroker);
        }
      }
    }

    Collection<Replica> leadersOfTopicInBroker = leadersOfTopicInBroker(aliveDestBroker, topic);
    int numLeadersOfTopicInBroker = leadersOfTopicInBroker.size();

    Set<Broker> candidateBrokers = Collections.singleton(aliveDestBroker);

    // Stop when no topic replicas can be moved in anymore.
    while (!eligibleBrokers.isEmpty()) {
      Broker sourceBroker = eligibleBrokers.poll();
      SortedSet<Replica> replicasToMove = replicasToMoveOut(sourceBroker, topic);
      int numOfflineTopicReplicas =
          GoalUtils.retainCurrentOfflineBrokerReplicas(sourceBroker, replicasToMove).size();

      for (Replica replica : replicasToMove) {
        boolean wasOffline = replica.isCurrentOffline();
        ActionType action;
        if (isExcludedForReplicaMove(aliveDestBroker)) {
          if (aliveDestBroker.replica(replica.topicPartition()) == null) {
            continue; // leadership transfer impossible; try next
          }
          action = ActionType.LEADERSHIP_MOVEMENT;
        } else {
          action = (aliveDestBroker.replica(replica.topicPartition()) == null)
              ? ActionType.INTER_BROKER_REPLICA_MOVEMENT : ActionType.LEADERSHIP_MOVEMENT;
        }
        Broker b = maybeApplyBalancingAction(clusterModel, replica, candidateBrokers, action,
            optimizedGoals, optimizationOptions);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (wasOffline) {
            numOfflineTopicReplicas--;
            offlineByBroker.computeIfPresent(sourceBroker.id(), (k, v) -> Math.max(0, v - 1));
          }
          topicLeadersByBroker.computeIfPresent(sourceBroker.id(), (k, v) -> Math.max(0, v - 1));
          topicLeadersByBroker.compute(aliveDestBroker.id(), (k, v) -> v == null ? 1 : v + 1);

          if (++numLeadersOfTopicInBroker >= _balanceLowerLimitByTopic.get(topic)) {
            return false;
          }
          if (!eligibleBrokers.isEmpty() && numOfflineTopicReplicas == 0
              && sourceBroker.numLeadersFor(topic) < eligibleBrokers.peek().numLeadersFor(topic)) {
            eligibleBrokers.add(sourceBroker);
            break;
          }
        }
      }
    }
    return true;
  }
}
