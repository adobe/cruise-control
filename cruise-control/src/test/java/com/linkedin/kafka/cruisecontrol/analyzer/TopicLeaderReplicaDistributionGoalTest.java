/*
 * Copyright 2025 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicLeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TopicLeaderReplicaDistributionGoalTest {

  private ClusterModel makeSimpleClusterModel(int numBrokers, BiFunction<Integer, Integer, Integer> leaderReplicaAssigner) {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    BrokerCapacityInfo brokerCapacity = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, "");
    Map<String, AtomicInteger> topicPartitionCnt = new HashMap<>();
    Function<Integer, String> rackIdGetter = bidx -> "rack" + (bidx % 3);
    for (int bidx = 0; bidx < numBrokers; bidx++) {
      String rackId = rackIdGetter.apply(bidx);
      clusterModel.createRack(rackId);
      clusterModel.createBroker(rackId, "broker" + bidx, bidx, brokerCapacity, false);
    }
    for (int bidx = 0; bidx < numBrokers; bidx++) {
      Broker broker = clusterModel.broker(bidx);
      for (int tidx = 0; tidx < 2; tidx++) {
        String topic = "T" + tidx;
        final int numPartitions = leaderReplicaAssigner.apply(bidx, tidx);
        for (int pidx = 0; pidx < numPartitions; pidx++) {
          int offset = topicPartitionCnt.computeIfAbsent(topic, k -> new AtomicInteger()).getAndIncrement();
          TopicPartition tp = new TopicPartition(topic, offset);
          clusterModel.createReplica(broker.rack().id(), broker.id(), tp, 0, true);
          AggregatedMetricValues aggregatedMetricValues = getAggregatedMetricValues(1.0, 10.0, 13.0, 5.0);
          clusterModel.setReplicaLoad(broker.rack().id(), broker.id(), tp, aggregatedMetricValues, Collections.singletonList(3L));
          final int nextBrokerId = (broker.id() + 1) % numBrokers;
          final String followerRackId = rackIdGetter.apply(nextBrokerId);
          clusterModel.createReplica(followerRackId, nextBrokerId, tp, 1, false);
          clusterModel.setReplicaLoad(followerRackId, nextBrokerId, tp, aggregatedMetricValues, Collections.singletonList(3L));
        }
      }
    }
    return clusterModel;
  }

  @Test
  public void testGoalNoopOnSatisfiable() throws Exception {
    final ClusterModel clusterModel = makeSimpleClusterModel(6, (bidx, tid) -> 2);
    final OptimizerResult result = getOptimizerResult(clusterModel);
    assertTrue(result.violatedGoalsBeforeOptimization().isEmpty());
    assertTrue(result.violatedGoalsAfterOptimization().isEmpty());
    int avgT0 = clusterModel.brokers().stream().mapToInt(bx -> bx.numLeadersFor("T0")).sum() / clusterModel.brokers().size();
    int avgT1 = clusterModel.brokers().stream().mapToInt(bx -> bx.numLeadersFor("T1")).sum() / clusterModel.brokers().size();
    for (Broker b : clusterModel.brokers()) {
      assertEquals(avgT0, b.numLeadersFor("T0"));
      assertEquals(avgT1, b.numLeadersFor("T1"));
    }
  }

  @Test
  public void testGoalLinearLeaderGrowth() throws Exception {
    final ClusterModel clusterModel = makeSimpleClusterModel(6, (bidx, tidx) -> 2 * bidx);
    final OptimizerResult result = getOptimizerResult(clusterModel);

    assertFalse(result.violatedGoalsBeforeOptimization().isEmpty());
    assertTrue(result.violatedGoalsAfterOptimization().isEmpty());
    int avgT0 = clusterModel.brokers().stream().mapToInt(bx -> bx.numLeadersFor("T0")).sum() / clusterModel.brokers().size();
    int avgT1 = clusterModel.brokers().stream().mapToInt(bx -> bx.numLeadersFor("T1")).sum() / clusterModel.brokers().size();
    for (Broker b : clusterModel.brokers()) {
      assertEquals(avgT0, b.numLeadersFor("T0"));
      assertEquals(avgT1, b.numLeadersFor("T1"));
    }
  }

  @Test
  public void testGoalPreferBrokerWithHigherTotalLeaderOnEquality() throws Exception {
    List<int[]> topicLeaderAssignment = new ArrayList<>();
    topicLeaderAssignment.add(new int[]{6, 6, 4});
    topicLeaderAssignment.add(new int[]{4, 5, 5});
    BiFunction<Integer, Integer, Integer> replicaAssigner = (brokerId, topicId) -> {
      if (topicId >= topicLeaderAssignment.size()) {
        return 5;
      } else if (brokerId >= topicLeaderAssignment.get(topicId).length) {
        return 5;
      } else {
        return topicLeaderAssignment.get(topicId)[brokerId];
      }
    };
    final ClusterModel clusterModel = makeSimpleClusterModel(6, replicaAssigner);
    final OptimizerResult result = getOptimizerResult(clusterModel);

    assertFalse(result.violatedGoalsBeforeOptimization().isEmpty());
    assertTrue(result.violatedGoalsAfterOptimization().isEmpty());
    int avgT0 = clusterModel.brokers().stream().mapToInt(bx -> bx.numLeadersFor("T0")).sum() / clusterModel.brokers().size();
    int avgT1 = clusterModel.brokers().stream().mapToInt(bx -> bx.numLeadersFor("T1")).sum() / clusterModel.brokers().size();
    for (Broker b : clusterModel.brokers()) {
      assertTrue(avgT0 - 1 <= b.numLeadersFor("T0") && b.numLeadersFor("T0") <= avgT0 + 1);
      assertTrue(avgT1 - 1 <= b.numLeadersFor("T1") && b.numLeadersFor("T1") <= avgT1 + 1);
    }
  }

  /**
   * Regression test for the self-leadership-move bug: when brokers holding all replicas of some
   * partitions are removed, immigrant leader replicas on the surviving broker can end up as both
   * source and destination of a leadership move, causing an IllegalArgumentException.
   *
   * Scenario: brokers 0 and 1 are dead (being removed). Broker 2 has 3 original leaders and
   * receives 2 immigrants (from dead-broker-only partitions). With the default gap config the
   * goal computes requireMoreLeaders=true for broker 2 (tracked count 2 < lower 3) while also
   * adding broker 2 to eligibleBrokers (raw count 5 > lower 3), triggering the self-move.
   */
  @Test
  public void testNoSelfLeadershipMoveWhenRemovingBrokers() throws Exception {
    ClusterModel clusterModel = buildClusterModelForSelfMoveBug();
    clusterModel.setBrokerState(0, Broker.State.DEAD);
    clusterModel.setBrokerState(1, Broker.State.DEAD);

    // Use default gap config (minGap=2, maxGap=10) — NOT the test override of 0/0 — because
    // the bug only manifests when lower > 0, which requires floorAvg >= 3.
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    BalancingConstraint balancingConstraint = new BalancingConstraint(kafkaCruiseControlConfig);
    GoalOptimizer goalOptimizer = new GoalOptimizer(kafkaCruiseControlConfig,
        null,
        Time.SYSTEM,
        new MetricRegistry(),
        EasyMock.mock(Executor.class),
        EasyMock.mock(AdminClient.class));
    List<Goal> goals = Collections.singletonList(new TopicLeaderReplicaDistributionGoal(balancingConstraint));
    // Before the fix this threw IllegalArgumentException: "Cannot relocate leadership … from
    // broker 2 to broker 2 because the destination replica is a leader."
    goalOptimizer.optimizations(clusterModel, goals, new OperationProgress());
  }

  /**
   * Build a cluster that reproduces the self-leadership-move bug:
   * <pre>
   *   Brokers 0, 1  – will be marked DEAD
   *   Brokers 2, 3  – alive
   *
   *   Topic "T": 10 partitions
   *     P0..P2  leader=2, follower=3   (broker 2: 3 original leaders)
   *     P3..P7  leader=3, follower=2   (broker 3: 5 original leaders)
   *     P8      leader=0, RF=1         (leader-only on dead broker → moved to broker 2)
   *     P9      leader=1, RF=1         (leader-only on dead broker → moved to broker 2)
   *
   *   RF=1 for P8/P9 prevents dead brokers from having orphaned follower replicas that
   *   TopicLeaderReplicaDistributionGoal cannot move (it only handles leaders).
   *
   *   After the dead brokers are removed:
   *     avg leaders/alive-broker = 10/2 = 5 → lower=3, upper=7
   *     broker 2 has 3 original + 2 immigrant leaders = 5 raw, 2 tracked
   *     requireMoreLeaders(2): tracked 2 < lower 3  → true
   *     eligibleBrokers includes 2: raw 5 > lower 3 → true  ← self-move source==dest==2
   * </pre>
   * @return ClusterModel set up to trigger the self-leadership-move bug
   */
  private ClusterModel buildClusterModelForSelfMoveBug() {
    ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0L), 1.0);
    BrokerCapacityInfo brokerCapacity = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, "");
    clusterModel.createRack("r0");
    clusterModel.createRack("r1");
    clusterModel.createBroker("r0", "b0", 0, brokerCapacity, false);
    clusterModel.createBroker("r1", "b1", 1, brokerCapacity, false);
    clusterModel.createBroker("r0", "b2", 2, brokerCapacity, false);
    clusterModel.createBroker("r1", "b3", 3, brokerCapacity, false);

    String topic = "T";
    AggregatedMetricValues mv = getAggregatedMetricValues(1.0, 10.0, 13.0, 5.0);
    List<Long> windows = Collections.singletonList(3L);

    // P0..P2: leader=2, follower=3
    for (int p = 0; p < 3; p++) {
      TopicPartition tp = new TopicPartition(topic, p);
      clusterModel.createReplica("r0", 2, tp, 0, true);
      clusterModel.setReplicaLoad("r0", 2, tp, mv, windows);
      clusterModel.createReplica("r1", 3, tp, 1, false);
      clusterModel.setReplicaLoad("r1", 3, tp, mv, windows);
    }
    // P3..P7: leader=3, follower=2
    for (int p = 3; p < 8; p++) {
      TopicPartition tp = new TopicPartition(topic, p);
      clusterModel.createReplica("r1", 3, tp, 0, true);
      clusterModel.setReplicaLoad("r1", 3, tp, mv, windows);
      clusterModel.createReplica("r0", 2, tp, 1, false);
      clusterModel.setReplicaLoad("r0", 2, tp, mv, windows);
    }
    // P8: leader=0, RF=1  (leader-only on soon-to-be-dead broker; no follower to leave stranded)
    TopicPartition tp8 = new TopicPartition(topic, 8);
    clusterModel.createReplica("r0", 0, tp8, 0, true);
    clusterModel.setReplicaLoad("r0", 0, tp8, mv, windows);
    // P9: leader=1, RF=1  (leader-only on soon-to-be-dead broker; no follower to leave stranded)
    TopicPartition tp9 = new TopicPartition(topic, 9);
    clusterModel.createReplica("r1", 1, tp9, 0, true);
    clusterModel.setReplicaLoad("r1", 1, tp9, mv, windows);

    return clusterModel;
  }

  private static OptimizerResult getOptimizerResult(ClusterModel clusterModel) throws Exception {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MIN_GAP_CONFIG, "0");
    props.setProperty(AnalyzerConfig.TOPIC_LEADER_REPLICA_COUNT_BALANCE_MAX_GAP_CONFIG, "0");
    KafkaCruiseControlConfig kafkaCruiseControlConfig = new KafkaCruiseControlConfig(props);
    BalancingConstraint balancingConstraint = new BalancingConstraint(kafkaCruiseControlConfig);
    GoalOptimizer goalOptimizer = new GoalOptimizer(kafkaCruiseControlConfig,
        null,
        Time.SYSTEM,
        new MetricRegistry(),
        EasyMock.mock(Executor.class),
        EasyMock.mock(AdminClient.class));
    List<Goal> goals = Collections.singletonList(new TopicLeaderReplicaDistributionGoal(balancingConstraint));
    return goalOptimizer.optimizations(clusterModel, goals, new OperationProgress());
  }
}
