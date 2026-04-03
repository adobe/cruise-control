/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.TopicMinIsrCache.MinIsrWithTime;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencyManager;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeMinIsrWithOfflineReplicasStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.StrategyOptions;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartitionReplica;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.assertEquals;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC3;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

/**
 * Unit test class for execution task planner
 */
public class ExecutionTaskPlannerTest {
  private static final int MAX_BROKER_CONCURRENCY = ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_MOVEMENTS_CONFIG - 1;
  private static final int MIN_BROKER_CONCURRENCY = 1;
  private final ReplicaPlacementInfo _r0 = new ReplicaPlacementInfo(0);
  private final ReplicaPlacementInfo _r1 = new ReplicaPlacementInfo(1);
  private final ReplicaPlacementInfo _r2 = new ReplicaPlacementInfo(2);
  private final ReplicaPlacementInfo _r3 = new ReplicaPlacementInfo(3);
  private final ReplicaPlacementInfo _r4 = new ReplicaPlacementInfo(4);
  private final ReplicaPlacementInfo _r5 = new ReplicaPlacementInfo(5);

  private final ExecutionProposal _leaderMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 0), 0, _r1, Arrays.asList(_r1, _r0), Arrays.asList(_r0, _r1));
  private final ExecutionProposal _leaderMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 1), 0, _r1, Arrays.asList(_r1, _r0), Arrays.asList(_r0, _r1));
  private final ExecutionProposal _leaderMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 2), 0, _r1, Arrays.asList(_r1, _r2), Arrays.asList(_r2, _r1));
  private final ExecutionProposal _leaderMovement4 =
      new ExecutionProposal(new TopicPartition(TOPIC1, 3), 0, _r3, Arrays.asList(_r3, _r2), Arrays.asList(_r2, _r3));

  private final ExecutionProposal _partitionMovement0 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 0), 10, _r0, Arrays.asList(_r0, _r2), Arrays.asList(_r2, _r1));
  private final ExecutionProposal _partitionMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 1), 30, _r1, Arrays.asList(_r1, _r3), Arrays.asList(_r3, _r2));
  private final ExecutionProposal _partitionMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 2), 20, _r2, Arrays.asList(_r2, _r1), Arrays.asList(_r1, _r3));
  private final ExecutionProposal _partitionMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC2, 3), 10, _r3, Arrays.asList(_r3, _r2), Arrays.asList(_r2, _r0));

  private final ExecutionProposal _rf4PartitionMovement0 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 0), 10, _r0, List.of(_r0, _r2, _r4, _r3), List.of(_r2, _r1, _r3, _r5));
  private final ExecutionProposal _rf4PartitionMovement1 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 1), 30, _r1, List.of(_r1, _r3, _r5, _r4), List.of(_r3, _r2, _r0, _r4));
  private final ExecutionProposal _rf4PartitionMovement2 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 2), 20, _r2, List.of(_r2, _r1, _r4, _r5), List.of(_r1, _r3, _r4, _r0));
  private final ExecutionProposal _rf4PartitionMovement3 =
      new ExecutionProposal(new TopicPartition(TOPIC3, 3), 10, _r3, List.of(_r3, _r2, _r5, _r1), List.of(_r2, _r0, _r5, _r1));

  private final List<Node> _expectedNodes = Arrays.asList(new Node(0, "null", -1),
                                                          new Node(1, "null", -1),
                                                          new Node(2, "null", -1),
                                                          new Node(3, "null", -1));
  private final List<Node> _rf4ExpectedNodes = List.of(new Node(0, "null", -1),
                                                       new Node(1, "null", -1),
                                                       new Node(2, "null", -1),
                                                       new Node(3, "null", -1),
                                                       new Node(4, "null", -1),
                                                       new Node(5, "null", -1));
  private final int _defaultPartitionsMaxCap = ExecutorConfig.DEFAULT_MAX_NUM_CLUSTER_PARTITION_MOVEMENTS_CONFIG;
  private final int _defaultEmptyPartitionConcurrency = ExecutorConfig.DEFAULT_NUM_CONCURRENT_EMPTY_PARTITION_MOVEMENTS_PER_BROKER;

  @Test
  public void testGetLeaderMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG, "");

    Set<PartitionInfo> partitions = new HashSet<>();
    for (ExecutionProposal proposal : proposals) {
      partitions.add(generatePartitionInfo(proposal, false));
    }

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.emptySet(),
                                          Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();
    ExecutionConcurrencyManager manager = new ExecutionConcurrencyManager(new KafkaCruiseControlConfig(props));

    // 1: there should be no throttle if there is enough concurrency at both cluster level and broker level.
    manager.setExecutionConcurrencyForAllBrokersOrCluster(4, ConcurrencyType.LEADERSHIP_CLUSTER);
    manager.setExecutionConcurrencyForBroker(0, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(1, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(2, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(3, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("4 of the leader movements should return in one batch", 4, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement1);
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement2);
    assertEquals(leaderMovementTasks.get(2).proposal(), _leaderMovement3);
    assertEquals(leaderMovementTasks.get(3).proposal(), _leaderMovement4);

    // 2: task is throttled by cluster concurrency:
    // Cluster concurrency is 3 and there are 4 tasks. Only 3 tasks can be executed at once, and 1 task should be throttled.
    manager.setExecutionConcurrencyForAllBrokersOrCluster(3, ConcurrencyType.LEADERSHIP_CLUSTER);
    manager.setExecutionConcurrencyForBroker(0, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(1, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(2, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(3, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);
    planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("3 of the leader movements should return in one batch", 3, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement1);
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement2);
    assertEquals(leaderMovementTasks.get(2).proposal(), _leaderMovement3);

    // 3: task is throttled by broker concurrency
    // The broker 1 have concurrency 1, but 3 of the tasks involves this broker. In this case, 2 tasks should be throttled.
    manager.setExecutionConcurrencyForAllBrokersOrCluster(3, ConcurrencyType.LEADERSHIP_CLUSTER);
    manager.setExecutionConcurrencyForBroker(0, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(1, MIN_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(2, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    manager.setExecutionConcurrencyForBroker(3, MAX_BROKER_CONCURRENCY, ConcurrencyType.LEADERSHIP_BROKER);
    proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_leaderMovement2);
    proposals.add(_leaderMovement3);
    proposals.add(_leaderMovement4);

    planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("2 of the leader movements should return in one batch", 2, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement1);
    assertEquals(leaderMovementTasks.get(1).proposal(), _leaderMovement4);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("1 of the leader movements should return in one batch", 1, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement2);
    leaderMovementTasks = planner.getLeadershipMovementTasks(manager);
    assertEquals("1 of the leader movements should return in one batch", 1, leaderMovementTasks.size());
    assertEquals(leaderMovementTasks.get(0).proposal(), _leaderMovement3);
  }

  @Test
  public void testGetInterBrokerPartitionMovementTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_partitionMovement0);
    proposals.add(_partitionMovement1);
    proposals.add(_partitionMovement2);
    proposals.add(_partitionMovement3);
    // Test different execution strategies.
    ExecutionTaskPlanner basePlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    // Create postponeUrpPlanner
    Properties postponeUrpProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    postponeUrpProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                 PostponeUrpReplicaMovementStrategy.class.getName());
    ExecutionTaskPlanner postponeUrpPlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(postponeUrpProps));

    // Create prioritizeLargeMovementPlanner
    Properties prioritizeLargeMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeLargeMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                             String.format("%s,%s", PrioritizeLargeReplicaMovementStrategy.class.getName(),
                                                           BaseReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeLargeMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeLargeMovementProps));

    // Create prioritizeSmallMovementPlanner
    Properties prioritizeSmallMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeSmallMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                             String.format("%s,%s", PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                           BaseReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeSmallMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeSmallMovementProps));

    // Create smallUrpMovementPlanner
    Properties smallUrpMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    smallUrpMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                      String.format("%s,%s", PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                    PostponeUrpReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner smallUrpMovementPlanner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(smallUrpMovementProps));

    // Create contradictingMovementPlanner containing both small and large replica movements
    Properties contradictingMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    contradictingMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                           String.format("%s,%s,%s", PrioritizeSmallReplicaMovementStrategy.class.getName(),
                                                         PostponeUrpReplicaMovementStrategy.class.getName(),
                                                         PrioritizeLargeReplicaMovementStrategy.class.getName()));
    ExecutionTaskPlanner contradictingMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(contradictingMovementProps));

    // Create prioritizeMinIsrMovementPlanner
    Properties prioritizeMinIsrMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeMinIsrMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                              PrioritizeMinIsrWithOfflineReplicasStrategy.class.getName());
    ExecutionTaskPlanner prioritizeMinIsrMovementPlanner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeMinIsrMovementProps));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_partitionMovement0, true));
    partitions.add(generatePartitionInfo(_partitionMovement1, false));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_partitionMovement2, true));
    partitions.add(generatePartitionInfo(_partitionMovement3, false));

    Cluster expectedCluster = new Cluster(null, _expectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    // This ensures that the _partitionMovement0 and _partitionMovement2 are AtMinISR, while the other partitions are not.
    Map<String, MinIsrWithTime> minIsrWithTimeByTopic
        = Collections.singletonMap(TOPIC2, new MinIsrWithTime((short) (_partitionMovement0.oldReplicas().size() - 1), 0));
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).minIsrWithTimeByTopic(minIsrWithTimeByTopic).build();

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 14);
    readyBrokers.put(1, 14);
    readyBrokers.put(2, 14);
    readyBrokers.put(3, 14);

    basePlanner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> partitionMovementTasks = basePlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                                _defaultPartitionsMaxCap,
                                                                                                _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement1, partitionMovementTasks.get(2).proposal());

    postponeUrpPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = postponeUrpPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                   _defaultPartitionsMaxCap,
                                                                                   _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());

    prioritizeLargeMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = prioritizeLargeMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                               _defaultPartitionsMaxCap,
                                                                                               _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement2, partitionMovementTasks.get(2).proposal());

    prioritizeSmallMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = prioritizeSmallMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                               _defaultPartitionsMaxCap,
                                                                                               _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement3, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement1, partitionMovementTasks.get(3).proposal());

    smallUrpMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = smallUrpMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                        _defaultPartitionsMaxCap,
                                                                                        _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement3, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement1, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement2, partitionMovementTasks.get(3).proposal());

    contradictingMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = contradictingMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                             _defaultPartitionsMaxCap,
                                                                                             _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement3, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement1, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement2, partitionMovementTasks.get(3).proposal());

    prioritizeMinIsrMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    partitionMovementTasks = prioritizeMinIsrMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                                _defaultPartitionsMaxCap,
                                                                                                _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement1, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement3, partitionMovementTasks.get(3).proposal());
  }

  @Test
  public void testGetInterBrokerPartitionMovementWithMinIsrTasks() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_rf4PartitionMovement0);
    proposals.add(_rf4PartitionMovement1);
    proposals.add(_rf4PartitionMovement2);
    proposals.add(_rf4PartitionMovement3);
    // Test PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy execution strategies.
    // Create prioritizeOneAboveMinIsrMovementPlanner, chain after prioritizeMinIsr strategy
    Properties prioritizeOneAboveMinIsrMovementProps = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    prioritizeOneAboveMinIsrMovementProps.setProperty(ExecutorConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                                      String.format("%s,%s", PrioritizeMinIsrWithOfflineReplicasStrategy.class.getName(),
                                                                    PrioritizeOneAboveMinIsrWithOfflineReplicasStrategy.class.getName()));
    ExecutionTaskPlanner prioritizeOneAboveMinIsrMovementPlanner
        = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(prioritizeOneAboveMinIsrMovementProps));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(_rf4PartitionMovement0, false));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_rf4PartitionMovement1, 1));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_rf4PartitionMovement2, 3));
    partitions.add(generatePartitionInfoWithUrpHavingOfflineReplica(_rf4PartitionMovement3, 2));

    Cluster expectedCluster = new Cluster(null, _rf4ExpectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    // Setting topic min ISR to 2
    Map<String, MinIsrWithTime> minIsrWithTimeByTopic
        = Collections.singletonMap(TOPIC3, new MinIsrWithTime((short) 2, 0));
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).minIsrWithTimeByTopic(minIsrWithTimeByTopic).build();

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 5);
    readyBrokers.put(1, 6);
    readyBrokers.put(2, 6);
    readyBrokers.put(3, 6);
    readyBrokers.put(4, 5);
    readyBrokers.put(5, 6);
    prioritizeOneAboveMinIsrMovementPlanner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> partitionMovementTasks
        = prioritizeOneAboveMinIsrMovementPlanner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                     _defaultPartitionsMaxCap, _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _rf4PartitionMovement2, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _rf4PartitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _rf4PartitionMovement1, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _rf4PartitionMovement0, partitionMovementTasks.get(3).proposal());
  }

  @Test
  public void testDynamicConfigReplicaMovementStrategy() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_partitionMovement0);
    proposals.add(_partitionMovement1);
    proposals.add(_partitionMovement2);
    proposals.add(_partitionMovement3);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    Set<PartitionInfo> partitions = new HashSet<>();
    partitions.add(generatePartitionInfo(_partitionMovement0, true));
    partitions.add(generatePartitionInfo(_partitionMovement1, false));
    partitions.add(generatePartitionInfo(_partitionMovement2, true));
    partitions.add(generatePartitionInfo(_partitionMovement3, false));

    Cluster expectedCluster = new Cluster(null, _expectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 8);
    readyBrokers.put(1, 8);
    readyBrokers.put(2, 8);
    readyBrokers.put(3, 8);
    planner.addExecutionProposals(proposals, strategyOptions, null);
    List<ExecutionTask> partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                                            _defaultPartitionsMaxCap,
                                                                                            _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement1, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, strategyOptions, new PostponeUrpReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                        _defaultPartitionsMaxCap,
                                                                        _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement0, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, strategyOptions, new PrioritizeLargeReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                        _defaultPartitionsMaxCap,
                                                                        _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement1, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement3, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement2, partitionMovementTasks.get(2).proposal());

    planner.addExecutionProposals(proposals, strategyOptions, new PrioritizeSmallReplicaMovementStrategy());
    partitionMovementTasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                        _defaultPartitionsMaxCap,
                                                                        _defaultEmptyPartitionConcurrency);
    assertEquals("First task", _partitionMovement0, partitionMovementTasks.get(0).proposal());
    assertEquals("Second task", _partitionMovement2, partitionMovementTasks.get(1).proposal());
    assertEquals("Third task", _partitionMovement3, partitionMovementTasks.get(2).proposal());
    assertEquals("Fourth task", _partitionMovement1, partitionMovementTasks.get(3).proposal());
  }

  @Test
  public void testGetIntraBrokerPartitionMovementTasks() {
    ReplicaPlacementInfo r0d0 = new ReplicaPlacementInfo(0, "d0");
    ReplicaPlacementInfo r0d1 = new ReplicaPlacementInfo(0, "d1");
    ReplicaPlacementInfo r1d0 = new ReplicaPlacementInfo(1, "d0");
    ReplicaPlacementInfo r1d1 = new ReplicaPlacementInfo(1, "d1");

    List<ExecutionProposal> proposals = Collections.singletonList(new ExecutionProposal(new TopicPartition(TOPIC2, 0),
                                                                                        4,
                                                                                        r0d0,
                                                                                        Arrays.asList(r0d0, r1d1),
                                                                                        Arrays.asList(r1d0, r0d1)));

    TopicPartitionReplica tpr0 = new TopicPartitionReplica(TOPIC2, 0, 0);
    TopicPartitionReplica tpr1 = new TopicPartitionReplica(TOPIC2, 0, 1);

    //Mock adminClient
    AdminClient mockAdminClient = EasyMock.mock(AdminClient.class);
    try {
      // Reflectively set constructors from package private to public.
      Constructor<DescribeReplicaLogDirsResult> constructor1 = DescribeReplicaLogDirsResult.class.getDeclaredConstructor(Map.class);
      constructor1.setAccessible(true);
      Constructor<ReplicaLogDirInfo> constructor2 =
          ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
      constructor2.setAccessible(true);

      Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
      futureByReplica.put(tpr0, completedFuture(constructor2.newInstance("d0", 0L, null, -1L)));
      futureByReplica.put(tpr1, completedFuture(constructor2.newInstance("d1", 0L, null, -1L)));

      EasyMock.expect(mockAdminClient.describeReplicaLogDirs(anyObject()))
              .andReturn(constructor1.newInstance(futureByReplica))
              .anyTimes();
      EasyMock.replay(mockAdminClient);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      // Let it go.
    }

    Set<PartitionInfo> partitions = new HashSet<>();
    Node[] isrArray = generateExpectedReplicas(proposals.get(0));
    partitions.add(new PartitionInfo(proposals.get(0).topicPartition().topic(),
                                     proposals.get(0).topicPartition().partition(),
                                     isrArray[0], isrArray, isrArray));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.emptySet(),
                                          Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();

    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(mockAdminClient, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    planner.addExecutionProposals(proposals, strategyOptions, null);
    assertEquals(1, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingIntraBrokerReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingIntraBrokerReplicaMovements().size());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testZeroByteTasksExceedNormalConcurrency() {
    // Create 10 zero-byte proposals all moving from broker 0 to broker 1.
    List<ExecutionProposal> proposals = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      proposals.add(new ExecutionProposal(new TopicPartition(TOPIC1, i), 0, _r0,
                                          Arrays.asList(_r0, _r2), Arrays.asList(_r2, _r1)));
    }

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));

    Set<PartitionInfo> partitions = new HashSet<>();
    for (ExecutionProposal p : proposals) {
      partitions.add(generatePartitionInfo(p, false));
    }
    Cluster cluster = new Cluster(null, _expectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(cluster).build();

    planner.addExecutionProposals(proposals, strategyOptions, null);

    // Normal concurrency is 2 per broker -- too low for 10 proposals.
    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 2);
    readyBrokers.put(1, 2);
    readyBrokers.put(2, 2);

    // With empty-partition concurrency of 50, all 10 zero-byte tasks should be scheduled.
    List<ExecutionTask> tasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                           _defaultPartitionsMaxCap, 50);
    assertEquals("All 10 zero-byte tasks should be scheduled", 10, tasks.size());
  }

  @Test
  public void testMixedZeroByteAndDataBearingTasks() {
    // Data-bearing proposals added first so they consume the normal slot,
    // then zero-byte proposals which should overflow into empty-partition slots.
    List<ExecutionProposal> proposals = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      proposals.add(new ExecutionProposal(new TopicPartition(TOPIC1, i), 100, _r0,
                                          Arrays.asList(_r0, _r2), Arrays.asList(_r2, _r1)));
    }
    for (int i = 3; i < 6; i++) {
      proposals.add(new ExecutionProposal(new TopicPartition(TOPIC1, i), 0, _r0,
                                          Arrays.asList(_r0, _r2), Arrays.asList(_r2, _r1)));
    }

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));

    Set<PartitionInfo> partitions = new HashSet<>();
    for (ExecutionProposal p : proposals) {
      partitions.add(generatePartitionInfo(p, false));
    }
    Cluster cluster = new Cluster(null, _expectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(cluster).build();

    planner.addExecutionProposals(proposals, strategyOptions, null);

    // Normal concurrency of 1 per broker.
    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 1);
    readyBrokers.put(1, 1);
    readyBrokers.put(2, 1);

    List<ExecutionTask> tasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                           _defaultPartitionsMaxCap, 50);

    int dataBearingCount = 0;
    int zeroByteCount = 0;
    for (ExecutionTask t : tasks) {
      if (t.proposal().dataToMoveInMB() == 0) {
        zeroByteCount++;
      } else {
        dataBearingCount++;
      }
    }
    // The first data-bearing proposal consumes the single normal slot per broker.
    // After that, remaining data-bearing tasks can't be scheduled (no normal slots, not zero-byte).
    // All 3 zero-byte tasks are then scheduled via the empty-partition slots.
    assertEquals("Only 1 data-bearing task should be scheduled with normal concurrency 1", 1, dataBearingCount);
    assertEquals("All 3 zero-byte tasks should be scheduled via empty partition slots", 3, zeroByteCount);
  }

  @Test
  public void testClusterWideCapRespectedForZeroByteTasks() {
    // Create 20 zero-byte proposals.
    List<ExecutionProposal> proposals = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      proposals.add(new ExecutionProposal(new TopicPartition(TOPIC1, i), 0, _r0,
                                          Arrays.asList(_r0, _r2), Arrays.asList(_r2, _r1)));
    }

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    ExecutionTaskPlanner planner = new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(props));

    Set<PartitionInfo> partitions = new HashSet<>();
    for (ExecutionProposal p : proposals) {
      partitions.add(generatePartitionInfo(p, false));
    }
    Cluster cluster = new Cluster(null, _expectedNodes, partitions, Collections.emptySet(), Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(cluster).build();

    planner.addExecutionProposals(proposals, strategyOptions, null);

    Map<Integer, Integer> readyBrokers = new HashMap<>();
    readyBrokers.put(0, 2);
    readyBrokers.put(1, 2);
    readyBrokers.put(2, 2);

    // Set cluster-wide cap to 5 -- should stop at 5 even though empty concurrency allows more.
    int clusterWideCap = 5;
    List<ExecutionTask> tasks = planner.getInterBrokerReplicaMovementTasks(readyBrokers, Collections.emptySet(),
                                                                           clusterWideCap, 100);
    assertEquals("Cluster-wide cap should be respected", clusterWideCap, tasks.size());
  }

  @Test
  public void testClear() {
    List<ExecutionProposal> proposals = new ArrayList<>();
    proposals.add(_leaderMovement1);
    proposals.add(_partitionMovement0);
    ExecutionTaskPlanner planner =
        new ExecutionTaskPlanner(null, new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));

    Set<PartitionInfo> partitions = new HashSet<>();

    partitions.add(generatePartitionInfo(_leaderMovement1, false));
    partitions.add(generatePartitionInfo(_partitionMovement0, false));

    Cluster expectedCluster = new Cluster(null,
                                          _expectedNodes,
                                          partitions,
                                          Collections.emptySet(),
                                          Collections.emptySet());
    StrategyOptions strategyOptions = new StrategyOptions.Builder(expectedCluster).build();

    planner.addExecutionProposals(proposals, strategyOptions, null);
    assertEquals(2, planner.remainingLeadershipMovements().size());
    assertEquals(2, planner.remainingInterBrokerReplicaMovements().size());
    planner.clear();
    assertEquals(0, planner.remainingLeadershipMovements().size());
    assertEquals(0, planner.remainingInterBrokerReplicaMovements().size());
  }

  private Node[] generateExpectedReplicas(ExecutionProposal proposal) {
    int i = 0;
    Node[] expectedProposalReplicas = new Node[proposal.oldReplicas().size()];
    for (ReplicaPlacementInfo oldId: proposal.oldReplicas()) {
      expectedProposalReplicas[i++] = new Node(oldId.brokerId(), "null", -1);
    }
    return expectedProposalReplicas;
  }

  private PartitionInfo generatePartitionInfoWithUrpHavingOfflineReplica(ExecutionProposal proposal, int urpCount) {
    Node[] isrArray = generateExpectedReplicas(proposal);

    Node[] offlineReplicas;
    if (urpCount > 0) {
      offlineReplicas = new Node[urpCount];
      System.arraycopy(isrArray, isrArray.length - urpCount, offlineReplicas, 0, urpCount);
    } else {
      offlineReplicas = new Node[0];
    }

    return new PartitionInfo(proposal.topicPartition().topic(),
                             proposal.topicPartition().partition(), isrArray[0], isrArray,
                             urpCount > 0 ? Arrays.copyOf(isrArray, isrArray.length - urpCount) : isrArray,
                             offlineReplicas);
  }

  private PartitionInfo generatePartitionInfoWithUrpHavingOfflineReplica(ExecutionProposal proposal, boolean isPartitionURP) {
    return generatePartitionInfoWithUrpHavingOfflineReplica(proposal, isPartitionURP ? 1 : 0);
  }

  private PartitionInfo generatePartitionInfo(ExecutionProposal proposal, boolean isPartitionURP) {
    Node[] isrArray = generateExpectedReplicas(proposal);
    return new PartitionInfo(proposal.topicPartition().topic(),
                             proposal.topicPartition().partition(), isrArray[0], isrArray,
                             isPartitionURP ? Arrays.copyOf(isrArray, 1) : isrArray);
  }
}
