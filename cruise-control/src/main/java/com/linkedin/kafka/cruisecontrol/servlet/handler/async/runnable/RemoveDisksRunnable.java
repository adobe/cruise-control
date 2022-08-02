/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskRemovalGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RemoveDisksParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig.REMOVE_DISKS_REMAINING_SIZE_ERROR_MARGIN;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.*;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;

public class RemoveDisksRunnable extends GoalBasedOperationRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveDisksRunnable.class);
    protected final Map<Integer, Set<String>> _brokerIdAndLogdirs;

    protected final double _errorMargin;

    public RemoveDisksRunnable(KafkaCruiseControl kafkaCruiseControl,
                               OperationFuture future,
                               RemoveDisksParameters parameters,
                               String uuid) {
        super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck(),
                uuid, parameters::reason);
        _brokerIdAndLogdirs = parameters.brokerIdAndLogdirs();
        _errorMargin = (double) _kafkaCruiseControl.config().mergedConfigValues().get(REMOVE_DISKS_REMAINING_SIZE_ERROR_MARGIN);
    }

    @Override
    protected OptimizationResult getResult() throws Exception {
        return new OptimizationResult(computeResult(), _kafkaCruiseControl.config());
    }

    @Override
    protected void init() {
        _kafkaCruiseControl.sanityCheckDryRun(_dryRun, _stopOngoingExecution);
        _goalsByPriority = new ArrayList<>(1);
        _goalsByPriority.add(new DiskRemovalGoal(_brokerIdAndLogdirs, _errorMargin));

        _operationProgress = _future.operationProgress();
        if (_stopOngoingExecution) {
            maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, _operationProgress);
        }
        _combinedCompletenessRequirements = _goalsByPriority.get(0).clusterModelCompletenessRequirements();
    }

    @Override
    protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, NotEnoughValidWindowsException, TimeoutException {
        Set<Integer> brokersToCheckPresence = new HashSet<>(_brokerIdAndLogdirs.keySet());
        _kafkaCruiseControl.sanityCheckBrokerPresence(brokersToCheckPresence);
        ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(
                DEFAULT_START_TIME_FOR_CLUSTER_MODEL,
                _kafkaCruiseControl.timeMs(),
                _combinedCompletenessRequirements,
                true,
                _allowCapacityEstimation,
                _operationProgress
        );

        checkCanRemoveDisks(_brokerIdAndLogdirs, clusterModel);

        OptimizationOptions optimizationOptions = computeOptimizationOptions(clusterModel,
                false,
                _kafkaCruiseControl,
                Collections.emptySet(),
                _dryRun,
                _excludeRecentlyDemotedBrokers,
                _excludeRecentlyRemovedBrokers,
                _excludedTopics,
                Collections.emptySet(),
                false,
                _fastMode
        );

        OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress, null, optimizationOptions);
        if (!_dryRun) {
            _kafkaCruiseControl.executeProposals(
                    result.goalProposals(),
                    Collections.emptySet(),
                    isKafkaAssignerMode(_goals),
                    0,
                    0,
                    1,
                    1,
                    SELF_HEALING_EXECUTION_PROGRESS_CHECK_INTERVAL_MS,
                    SELF_HEALING_REPLICA_MOVEMENT_STRATEGY,
                    _kafkaCruiseControl.config().getLong(ExecutorConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG),
                    _isTriggeredByUserRequest,
                    _uuid,
                    false
            );
        }

        return result;
    }

    private void checkCanRemoveDisks(Map<Integer, Set<String>> brokerIdAndLogdirs, ClusterModel clusterModel) {
        for (Map.Entry<Integer, Set<String>> entry : brokerIdAndLogdirs.entrySet()) {
            Integer brokerId = entry.getKey();
            Set<String> logDirs = entry.getValue();
            Broker broker = clusterModel.broker(brokerId);
            Set<String> brokerLogDirs = broker.disks().stream().map(Disk::logDir).collect(Collectors.toSet());
            if (!brokerLogDirs.containsAll(logDirs)) {
                throw new IllegalArgumentException(String.format("Invalid log dirs provided for broker %d.", brokerId));
            }
            if (broker.disks().size() < logDirs.size()) {
                throw new IllegalArgumentException(String.format("Too many log dirs provided for broker %d.", brokerId));
            } else if (broker.disks().size() == logDirs.size()) {
                throw new IllegalArgumentException(String.format("No log dir remaining to move replicas to for broker %d.", brokerId));
            }

            double removedUsage = 0.0;
            double remainingCapacity = 0.0;
            double currentUsage = 0.0;
            for (Disk disk : broker.disks()) {
                if (logDirs.contains(disk.logDir())) {
                    removedUsage += disk.utilization();
                } else {
                    remainingCapacity += disk.capacity();
                    currentUsage += disk.utilization();
                }
            }
            double futureUsage = removedUsage + currentUsage;
            if ((1 - (futureUsage / remainingCapacity)) < _errorMargin) {
                throw new IllegalArgumentException("Not enough remaining capacity to move replicas to.");
            }
        }
    }

    @Override
    protected boolean shouldWorkWithClusterModel() {
        return true;
    }

    @Override
    protected OptimizerResult workWithoutClusterModel() {
        return null;
    }
}
