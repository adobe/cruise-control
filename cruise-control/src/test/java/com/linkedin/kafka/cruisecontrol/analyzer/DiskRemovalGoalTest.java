/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskRemovalGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.*;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig.DEFAULT_REMOVE_DISKS_REMAINING_SIZE_ERROR_MARGIN;
import static org.junit.Assert.assertEquals;

public class DiskRemovalGoalTest {
    private static final TopicPartition T0P0 = new TopicPartition(TOPIC0, 0);
    private static final TopicPartition T0P1 = new TopicPartition(TOPIC0, 1);
    private static final boolean POPULATE_DISK_INFO = true;
    private static final String RACK = "r0";
    private static final String HOST = "h0";
    private static final int BROKER_ID = 0;
    private static final int INDEX = 0;
    private static final double HIGH_DISK_USAGE = 0.8;
    private static final double LOW_DISK_USAGE = 0.3;
    private static final double MEDIUM_DISK_USAGE = 0.5;
    private static final double NO_DISK_USAGE = 0;

    @Test
    public void testMoveReplicasToAnotherLogDir() {
        ClusterModel clusterModel = createClusterModel();
        createReplicaAndSetLoad(clusterModel, LOGDIR0, T0P0, true, LOW_DISK_USAGE);
        createReplicaAndSetLoad(clusterModel, LOGDIR1, T0P1, false, LOW_DISK_USAGE);
        Map<Integer, Set<String>> brokerIdAndLogDirs = new HashMap<>();
        brokerIdAndLogDirs.put(0, new HashSet<>(Collections.singletonList(LOGDIR0)));

        runOptimization(clusterModel, brokerIdAndLogDirs);

        assertEquals(clusterModel.broker(0).disk(LOGDIR0).replicas().size(), 0);
    }

    @Test
    public void testMoveReplicasToAlreadyUsedDisk() {
        ClusterModel clusterModel = createClusterModel();
        createReplicaAndSetLoad(clusterModel, LOGDIR0, T0P0, true, LOW_DISK_USAGE);
        createReplicaAndSetLoad(clusterModel, LOGDIR1, T0P1, false, MEDIUM_DISK_USAGE);
        Map<Integer, Set<String>> brokerIdAndLogDirs = new HashMap<>();
        brokerIdAndLogDirs.put(0, new HashSet<>(Collections.singletonList(LOGDIR0)));

        runOptimization(clusterModel, brokerIdAndLogDirs);

        assertEquals(clusterModel.broker(0).disk(LOGDIR0).replicas().size(), 0);
    }

    @Test
    public void testMoveLargeDiskToEmptyDisk() {
        ClusterModel clusterModel = createClusterModel();
        createReplicaAndSetLoad(clusterModel, LOGDIR0, T0P0, true, HIGH_DISK_USAGE);
        createReplicaAndSetLoad(clusterModel, LOGDIR1, T0P1, false, NO_DISK_USAGE);
        Map<Integer, Set<String>> brokerIdAndLogDirs = new HashMap<>();
        brokerIdAndLogDirs.put(0, new HashSet<>(Collections.singletonList(LOGDIR0)));

        runOptimization(clusterModel, brokerIdAndLogDirs);

        assertEquals(clusterModel.broker(0).disk(LOGDIR0).replicas().size(), 0);
    }

    @Test
    public void testReplicasStayIsDestinationHasInsufficientCapacity() {
        ClusterModel clusterModel = createClusterModel();
        createReplicaAndSetLoad(clusterModel, LOGDIR0, T0P0, true, MEDIUM_DISK_USAGE);
        createReplicaAndSetLoad(clusterModel, LOGDIR1, T0P1, false, MEDIUM_DISK_USAGE);
        Map<Integer, Set<String>> brokerIdAndLogDirs = new HashMap<>();
        brokerIdAndLogDirs.put(0, new HashSet<>(Collections.singletonList(LOGDIR0)));

        runOptimization(clusterModel, brokerIdAndLogDirs);

        assertEquals(clusterModel.broker(0).disk(LOGDIR0).replicas().size(), 1);
    }

    @Test
    public void testReplicasStayIfDiskAlmostFull() {
        ClusterModel clusterModel = createClusterModel();
        createReplicaAndSetLoad(clusterModel, LOGDIR0, T0P0, true, LOW_DISK_USAGE);
        createReplicaAndSetLoad(clusterModel, LOGDIR1, T0P1, false, HIGH_DISK_USAGE);
        Map<Integer, Set<String>> brokerIdAndLogDirs = new HashMap<>();
        brokerIdAndLogDirs.put(0, new HashSet<>(Collections.singletonList(LOGDIR0)));

        runOptimization(clusterModel, brokerIdAndLogDirs);

        assertEquals(clusterModel.broker(0).disk(LOGDIR0).replicas().size(), 1);
    }

    private void runOptimization(ClusterModel clusterModel, Map<Integer, Set<String>> brokerIdAndLogDirs) {
        DiskRemovalGoal goal = new DiskRemovalGoal(brokerIdAndLogDirs, DEFAULT_REMOVE_DISKS_REMAINING_SIZE_ERROR_MARGIN);
        // Before the optimization, goals are expected to be undecided wrt their provision status.
        assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
        goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet()));
        // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
        assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
    }

    private ClusterModel createClusterModel() {
        ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0), 1.0);
        clusterModel.createRack(RACK);
        BrokerCapacityInfo capacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY, null, TestConstants.DISK_CAPACITY);
        clusterModel.createBroker(RACK, HOST, BROKER_ID, capacityInfo, POPULATE_DISK_INFO);
        return clusterModel;
    }

    private void createReplicaAndSetLoad(ClusterModel clusterModel,
                                         String logdir,
                                         TopicPartition tp,
                                         boolean isLeader,
                                         double diskUsage) {
        clusterModel.createReplica(RACK, BROKER_ID, tp, INDEX, isLeader, false, logdir, false);
        MetricValues defaultMetricValues = new MetricValues(1);
        MetricValues diskMetricValues = new MetricValues(1);
        double[] diskMetric = {DISK_CAPACITY.get(logdir) * diskUsage};
        diskMetricValues.add(diskMetric);
        Map<Short, MetricValues> metricValuesByResource = new HashMap<>();
        Resource.cachedValues().forEach(r -> {
            for (short id : KafkaMetricDef.resourceToMetricIds(r)) {
                if (r.equals(Resource.DISK)) {
                    metricValuesByResource.put(id, diskMetricValues);
                } else {
                    metricValuesByResource.put(id, defaultMetricValues);
                }
            }
        });
        clusterModel.setReplicaLoad(RACK, BROKER_ID, tp, new AggregatedMetricValues(metricValuesByResource),
                Collections.singletonList(1L));
    }
}
