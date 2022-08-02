/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * Soft goal to move the replicas to different log dir.
 */
public class DiskRemovalGoal implements Goal {
    private static final Logger LOG = LoggerFactory.getLogger(DiskRemovalGoal.class);
    private final ProvisionResponse _provisionResponse;

    protected final Map<Integer, Set<String>> _brokerIdAndLogdirs;

    protected final double _errorMargin;

    public DiskRemovalGoal(Map<Integer, Set<String>> brokerIdAndLogdirs, double errorMargin) {
        _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
        _brokerIdAndLogdirs = brokerIdAndLogdirs;
        _errorMargin = errorMargin;
    }

    private void sanityCheckOptimizationOptions(OptimizationOptions optimizationOptions) {
        if (optimizationOptions.isTriggeredByGoalViolation()) {
            throw new IllegalArgumentException(String.format("%s goal does not support use by goal violation detector.", name()));
        }
    }

    @Override
    public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) {
        sanityCheckOptimizationOptions(optimizationOptions);

        for (Map.Entry<Integer, Set<String>> brokerIdLogDirs : _brokerIdAndLogdirs.entrySet()) {
            Integer brokerId = brokerIdLogDirs.getKey();
            Set<String> logDirsToRemove = brokerIdLogDirs.getValue();
            relocateBrokerLogDirs(clusterModel, brokerId, logDirsToRemove);
        }

        return true;
    }

    /**
     * This method relocates the replicas on the provided log dirs to other log dirs of the same broker.
     * @param clusterModel the cluster model
     * @param brokerId the id of the broker where the movement will take place
     * @param logDirsToRemove the set of log dirs to be removed from the broker
     */
    private void relocateBrokerLogDirs(ClusterModel clusterModel, Integer brokerId, Set<String> logDirsToRemove) {
        Broker currentBroker = clusterModel.broker(brokerId);
        List<Disk> remainingDisks = new ArrayList<>();
        currentBroker.disks().stream().filter(disk -> !logDirsToRemove.contains(disk.logDir())).forEach(remainingDisks::add);

        for (String logDirToRemove : logDirsToRemove) {
            Set<Replica> replicasToMove = currentBroker.disk(logDirToRemove).replicas();
            for (int i = 0; i < replicasToMove.size(); i++) {
                Replica replica = replicasToMove.iterator().next();
                relocateReplicaIfPossible(clusterModel, brokerId, remainingDisks, replica);
                LOG.info(String.format("Could not move replica %s to any of the remaining disks.", replica));
            }
        }
    }

    /**
     * This methods relocates the given replica on one of the candidate disks if there is enough space on any of them
     * @param clusterModel the cluster model
     * @param brokerId the broker id where the replica movement occurs
     * @param remainingDisks the candidate disks on which to move the replica
     * @param replica the replica to move
     */
    private void relocateReplicaIfPossible(ClusterModel clusterModel, Integer brokerId, List<Disk> remainingDisks, Replica replica) {
        for (Disk disk : remainingDisks) {
            if (isEnoughSpace(disk, replica)) {
                clusterModel.relocateReplica(replica.topicPartition(), brokerId, disk.logDir());
            }
        }
    }

    /**
     * This method checks if the usage on the disk that the replica will be moved to is lower than the disk capacity
     * including the error margin.
     * @param disk the disk on which the replica can be moved
     * @param replica the replica to move
     * @return boolean which reflects if there is enough disk space to move the replica
     */
    private boolean isEnoughSpace(Disk disk, Replica replica) {
        double futureUsage = disk.utilization() + replica.load().expectedUtilizationFor(Resource.DISK);
        return (1 - (futureUsage / disk.capacity())) >= _errorMargin;
    }

    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
        return ACCEPT;
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new ClusterModelStatsComparator() {
            @Override
            public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
                return 0;
            }

            @Override
            public String explainLastComparison() {
                return String.format("Comparison for the %s is irrelevant.", name());
            }
        };
    }

    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
        return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0, true);
    }

    @Override
    public String name() {
        return DiskRemovalGoal.class.getSimpleName();
    }

    @Override
    public void finish() {

    }

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    public ProvisionStatus provisionStatus() {
        // Provision status computation is not relevant to PLE goal.
        return provisionResponse().status();
    }

    @Override
    public ProvisionResponse provisionResponse() {
        return _provisionResponse;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
