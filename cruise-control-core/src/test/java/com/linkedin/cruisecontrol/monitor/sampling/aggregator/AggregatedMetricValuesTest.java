/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class AggregatedMetricValuesTest {

  @Test
  public void testMetricAdditionRoundsAfterDoubleArithmetic() {
    MetricValues values = new MetricValues(2);
    values.set(0, 16_777_216.0);
    values.set(1, 0.125);
    values.add(new double[]{1.00000001, 0.25});
    assertEquals(16_777_218.0, values.get(0), 0.0);
    assertEquals(0.375, values.get(1), 0.0);
    assertEquals(8_388_609.0, values.avg(), 0.0);
    assertEquals(16_777_218.0, values.max(), 0.0);
  }

  @Test
  public void testMetricAdditionSupportsRepeatedAndSelfAddition() {
    MetricValues values = new MetricValues(2);
    values.set(0, 1.5);
    values.set(1, 2.25);
    MetricValues increment = new MetricValues(2);
    increment.set(0, 0.25);
    increment.set(1, 0.5);
    values.add(increment);
    values.add(increment);
    values.add(values);
    assertEquals(4.0, values.get(0), 0.0);
    assertEquals(6.5, values.get(1), 0.0);
    assertEquals(5.25, values.avg(), 0.0);
    assertEquals(6.5, values.max(), 0.0);
  }

  @Test
  public void testAdd() {
    Map<Short, MetricValues> valuesByMetricId = getValuesByMetricId();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    aggregatedMetricValues.add(aggregatedMetricValues);

    for (Map.Entry<Short, MetricValues> entry : valuesByMetricId.entrySet()) {
      MetricValues values = entry.getValue();
      for (int j = 0; j < 10; j++) {
        assertEquals(2 * j, values.get(j), 0.01);
      }
    }
  }

  @Test
  public void testDeduct() {
    Map<Short, MetricValues> valuesByMetricId = getValuesByMetricId();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(valuesByMetricId);
    aggregatedMetricValues.subtract(aggregatedMetricValues);

    for (Map.Entry<Short, MetricValues> entry : valuesByMetricId.entrySet()) {
      MetricValues values = entry.getValue();
      for (int j = 0; j < 10; j++) {
        assertEquals(0, values.get(j), 0.01);
      }
    }
  }

  @Test
  public void testAddValuesToEmptyAggregatedMetricValues() {
    Map<Short, MetricValues> valuesByMetricId = getValuesByMetricId();

    AggregatedMetricValues aggregatedMetricValues = new AggregatedMetricValues(new HashMap<>());
    AggregatedMetricValues toBeAdded = new AggregatedMetricValues(valuesByMetricId);

    aggregatedMetricValues.add(toBeAdded);
    for (short i = 0; i < 2; i++) {
      MetricValues values = aggregatedMetricValues.valuesFor(i);
      for (int j = 0; j < 10; j++) {
        assertEquals(j, values.get(j), 0.01);
      }
    }
  }

  private Map<Short, MetricValues> getValuesByMetricId() {
    Map<Short, MetricValues> valuesMap = new TreeMap<>();

    for (short i = 0; i < 2; i++) {
      MetricValues value = new MetricValues(10);
      for (int j = 0; j < 10; j++) {
        value.set(j, j);
      }
      valuesMap.put(i, value);
    }
    return valuesMap;
  }

}
