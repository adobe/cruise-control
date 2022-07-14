/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Collections;
import java.util.Set;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.*;

public class RemoveDisksParameters extends GoalBasedOptimizationParameters {
    protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
    static {
        SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        validParameterNames.add(BROKER_ID_AND_LOGDIRS_PARAM);
        validParameterNames.add(DRY_RUN_PARAM);
        validParameterNames.add(REASON_PARAM);
        validParameterNames.add(SKIP_HARD_GOAL_CHECK_PARAM);
        validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
        validParameterNames.addAll(GoalBasedOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
        CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
    }
    protected boolean _dryRun;
    protected boolean _skipHardGoalCheck;
    protected String _reason;
    protected boolean _stopOngoingExecution;
    protected Map<Integer, Set<String>> _logdirByBrokerId;

    public RemoveDisksParameters() {
        super();
    }

    @Override
    protected void initParameters() throws UnsupportedEncodingException {
        super.initParameters();
        _logdirByBrokerId = ParameterUtils.brokerIdAndLogdirs(_request);
        _dryRun = ParameterUtils.getDryRun(_request);
        _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_request);
        boolean requestReasonRequired = _config.getBoolean(ExecutorConfig.REQUEST_REASON_REQUIRED_CONFIG);
        _reason = ParameterUtils.reason(_request, requestReasonRequired && !_dryRun);
        _stopOngoingExecution = ParameterUtils.stopOngoingExecution(_request);
        if (_stopOngoingExecution && _dryRun) {
            throw new UserRequestException(String.format("%s and %s cannot both be set to true.", STOP_ONGOING_EXECUTION_PARAM, DRY_RUN_PARAM));
        }
    }

    public Map<Integer, Set<String>> brokerIdAndLogdirs() {
        return _logdirByBrokerId;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
    }

    @Override
    public SortedSet<String> caseInsensitiveParameterNames() {
        return CASE_INSENSITIVE_PARAMETER_NAMES;
    }
    public String reason() {
        return _reason;
    }
    public boolean dryRun() {
        return _dryRun;
    }
    public boolean skipHardGoalCheck() {
        return _skipHardGoalCheck;
    }
    public boolean stopOngoingExecution() {
        return _stopOngoingExecution;
    }
}
