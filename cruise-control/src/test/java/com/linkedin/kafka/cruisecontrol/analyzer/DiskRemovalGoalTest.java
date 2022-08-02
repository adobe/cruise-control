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
import java.util.Arrays;
import java.util.Collections;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.*;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig.DEFAULT_REMOVE_DISKS_REMAINING_SIZE_ERROR_MARGIN;
import static org.junit.Assert.assertEquals;

public class DiskRemovalGoalTest {
    private static final TopicPartition T0P0 = new TopicPartition(TOPIC0, 0);
    private static final TopicPartition T0P1 = new TopicPartition(TOPIC0, 1);

    @Test
    public void testMoveReplicasToAnotherLogDir() {
        ClusterModel clusterModel = createClusterModel(false);
        Map<Integer, Set<String>> brokerIdAndLogDirs = new HashMap<>();
        brokerIdAndLogDirs.put(0, new HashSet<>(Arrays.asList(LOGDIR0)));

        DiskRemovalGoal goal = new DiskRemovalGoal(brokerIdAndLogDirs, DEFAULT_REMOVE_DISKS_REMAINING_SIZE_ERROR_MARGIN);
        // Before the optimization, goals are expected to be undecided wrt their provision status.
        assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
        goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet()));
        // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
        assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

        assertEquals(clusterModel.broker(0).disk(LOGDIR0).replicas().size(), 0);
    }

    @Test
    public void testReplicaStaysIfDiskUtilizationIsHigh() {
        ClusterModel clusterModel = createClusterModel(true);
        Map<Integer, Set<String>> brokerIdAndLogDirs = new HashMap<>();
        brokerIdAndLogDirs.put(0, new HashSet<>(Arrays.asList(LOGDIR0)));

        DiskRemovalGoal goal = new DiskRemovalGoal(brokerIdAndLogDirs, DEFAULT_REMOVE_DISKS_REMAINING_SIZE_ERROR_MARGIN);
        // Before the optimization, goals are expected to be undecided wrt their provision status.
        assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());
        goal.optimize(clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet()));
        // After the optimization, PreferredLeaderElectionGoal is expected to be undecided wrt its provision status.
        assertEquals(ProvisionStatus.UNDECIDED, goal.provisionResponse().status());

        assertEquals(clusterModel.broker(0).disk(LOGDIR0).replicas().size(), 1);
    }

    private ClusterModel createClusterModel(boolean hasHighDiskUsage) {
        boolean populateDiskInfo = true;
        String rack = "r0";
        String host = "h0";
        int brokerId = 0;
        int index = 0;

        ClusterModel clusterModel = new ClusterModel(new ModelGeneration(0, 0),
                1.0);

        clusterModel.createRack(rack);

        BrokerCapacityInfo commonBrokerCapacityInfo = new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY,
                null,
                TestConstants.DISK_CAPACITY);
        clusterModel.createBroker(rack, host, brokerId, commonBrokerCapacityInfo, populateDiskInfo);

        createReplicaAndSetLoad(clusterModel, rack, brokerId, LOGDIR0, T0P0, index, true, hasHighDiskUsage);
        createReplicaAndSetLoad(clusterModel, rack, brokerId, LOGDIR1, T0P1, index, false, hasHighDiskUsage);
        return clusterModel;
    }

    private void createReplicaAndSetLoad(ClusterModel clusterModel,
                                         String rack,
                                         int brokerId,
                                         String logdir,
                                         TopicPartition tp,
                                         int index,
                                         boolean isLeader,
                                         boolean hasHighDiskUsage) {
        clusterModel.createReplica(rack, brokerId, tp, index, isLeader, false, logdir, false);
        MetricValues defaultMetricValues = new MetricValues(1);
        MetricValues diskMetricValues = new MetricValues(1);
        double diskUsage = hasHighDiskUsage ? 0.8 : 0.3;
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
        clusterModel.setReplicaLoad(rack, brokerId, tp, new AggregatedMetricValues(metricValuesByResource),
                Collections.singletonList(1L));
    }
}
