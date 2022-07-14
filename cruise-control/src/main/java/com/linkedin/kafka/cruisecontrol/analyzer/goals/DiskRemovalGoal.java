/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionResponse;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionStatus;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
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

    public DiskRemovalGoal() {
        this(Collections.emptyMap());
    }

    public DiskRemovalGoal(Map<Integer, Set<String>> brokerIdAndLogdirs) {
        _provisionResponse = new ProvisionResponse(ProvisionStatus.UNDECIDED);
        _brokerIdAndLogdirs = brokerIdAndLogdirs;
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
            Set<String> logDirs = brokerIdLogDirs.getValue();

            Broker currentBroker = clusterModel.broker(brokerId);
            List<Disk> remainingDisks = new ArrayList<>();
            currentBroker.disks().stream().filter(disk -> !logDirs.contains(disk.logDir())).forEach(remainingDisks::add);

            int step = 0;
            int size = remainingDisks.size();
            while (!logDirs.isEmpty()) {
                String logDirToRemove = (String) logDirs.toArray()[0];

                Set<Replica> replicasToMove = currentBroker.disk(logDirToRemove).replicas();
                while (!replicasToMove.isEmpty()) {
                    Replica replica = (Replica) replicasToMove.toArray()[0];
                    clusterModel.relocateReplica(replica.topicPartition(), brokerId, remainingDisks.get(step % size).logDir());
                }

                logDirs.remove(logDirToRemove);
                step++;
            }
        }

        return true;
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
