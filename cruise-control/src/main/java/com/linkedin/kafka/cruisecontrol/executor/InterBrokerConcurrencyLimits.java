/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

/**
 * Immutable holder for the two concurrency limits passed to
 * {@link ExecutionTaskPlanner#getInterBrokerReplicaMovementTasks}: the cluster-wide cap on normal inter-broker
 * partition movements and the per-broker extended cap for empty (sub-1 MB) partition movements.
 *
 * <p>Using a named type instead of two adjacent {@code int} parameters eliminates the silent swap hazard at call
 * sites, where the two values have similar types and magnitudes.</p>
 */
public final class InterBrokerConcurrencyLimits {
  /** Maximum number of inter-broker partition movements in flight cluster-wide at any time. */
  public final int maxPartitionMovements;

  /** Per-broker concurrency cap for empty (sub-1 MB) partition moves, applied as an extended limit on top of normal slots. */
  public final int maxEmptyPartitionMovementsPerBroker;

  public InterBrokerConcurrencyLimits(int maxPartitionMovements, int maxEmptyPartitionMovementsPerBroker) {
    this.maxPartitionMovements = maxPartitionMovements;
    this.maxEmptyPartitionMovementsPerBroker = maxEmptyPartitionMovementsPerBroker;
  }
}
