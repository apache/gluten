/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.memory.memtarget;

import org.apache.gluten.config.GlutenCoreConfig;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.memory.SparkMemoryUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WrappedGlobalOffHeapMemoryTargetTest {
  private SparkEnv previousEnv;
  private LimitedMemoryTarget delegate;
  private WrappedGlobalOffHeapMemoryTarget target;
  private DynamicOffHeapSizingMemoryTarget taskTarget;

  @Before
  public void setUp() {
    previousEnv = SparkEnv.get();
    DynamicOffHeapSizingMemoryTarget.resetUsedOffHeapBytesForTesting();
    installSparkEnv(false);
    delegate = new LimitedMemoryTarget();
    target = new WrappedGlobalOffHeapMemoryTarget(delegate);
    taskTarget = new DynamicOffHeapSizingMemoryTarget(new NoopMemoryTarget());
  }

  @After
  public void tearDown() {
    try {
      delegate.maxRepay = Long.MAX_VALUE;
      target.repay(target.usedBytes());
      taskTarget.repay(taskTarget.usedBytes());
      assertAccounting(0L, 0L);
    } finally {
      SparkEnv.set(previousEnv);
      DynamicOffHeapSizingMemoryTarget.resetUsedOffHeapBytesForTesting();
    }
  }

  @Test
  public void borrowAndRepayFollowCurrentSparkEnv() {
    assumeHeapBudgetAvailable();
    for (boolean dynamicSizing : new boolean[] {false, true, false}) {
      installSparkEnv(dynamicSizing);
      final long size = dynamicSizing ? 60L : Runtime.getRuntime().maxMemory();

      Assert.assertEquals(size, target.borrow(size));
      assertAccounting(size, dynamicSizing ? size : 0L);
      Assert.assertEquals(size / 2, target.repay(size / 2));
      final long remaining = size - size / 2;
      assertAccounting(remaining, dynamicSizing ? remaining : 0L);
      Assert.assertEquals(remaining, target.repay(remaining));
      assertAccounting(0L, 0L);
    }

    Assert.assertEquals(Runtime.getRuntime().maxMemory(), target.stats().getPeak());
    Assert.assertTrue(target.name().startsWith("WrappedGlobalOffHeap."));
    Assert.assertTrue(
        SparkMemoryUtil.dumpMemoryTargetStats(target).startsWith("Global off-heap target stats: "));
  }

  @Test
  public void repayPreservesTaskLedgerAcrossModeChanges() {
    assumeHeapBudgetAvailable();
    Assert.assertEquals(40L, target.borrow(40L));
    installSparkEnv(true);
    Assert.assertEquals(20L, taskTarget.borrow(20L));
    Assert.assertEquals(40L, target.repay(40L));
    assertAccounting(0L, 20L);

    Assert.assertEquals(60L, target.borrow(60L));
    assertAccounting(60L, 80L);
    installSparkEnv(false);
    Assert.assertEquals(60L, target.repay(60L));
    assertAccounting(0L, 20L);

    Assert.assertEquals(40L, target.borrow(40L));
    assertAccounting(40L, 20L);
    Assert.assertEquals(40L, target.repay(40L));
    assertAccounting(0L, 20L);
    Assert.assertEquals(20L, taskTarget.usedBytes());
  }

  @Test
  public void borrowRecordsActualGrantsAndIgnoresRejectedReservations() {
    assumeHeapBudgetAvailable();
    for (boolean dynamicSizing : new boolean[] {false, true}) {
      installSparkEnv(dynamicSizing);
      delegate.maxGrant = 40L;

      Assert.assertEquals(40L, target.borrow(100L));
      assertAccounting(40L, dynamicSizing ? 40L : 0L);
      delegate.maxGrant = 0L;
      Assert.assertEquals(0L, target.borrow(60L));
      assertAccounting(40L, dynamicSizing ? 40L : 0L);
      Assert.assertEquals(40L, target.repay(40L));
      assertAccounting(0L, 0L);
    }
  }

  @Test
  public void mixedReservationsUnbookOnlyBytesActuallyFreed() {
    assumeHeapBudgetAvailable();
    installSparkEnv(true);
    Assert.assertEquals(20L, taskTarget.borrow(20L));
    Assert.assertEquals(60L, target.borrow(60L));
    installSparkEnv(false);
    Assert.assertEquals(40L, target.borrow(40L));
    assertAccounting(100L, 80L);

    delegate.maxRepay = 20L;
    Assert.assertEquals(40L, target.repay(80L));
    assertAccounting(60L, 60L);
    installSparkEnv(true);
    delegate.maxRepay = Long.MAX_VALUE;
    Assert.assertEquals(60L, target.repay(60L));
    assertAccounting(0L, 20L);
    Assert.assertEquals(20L, taskTarget.usedBytes());
  }

  private void installSparkEnv(boolean dynamicSizing) {
    final SparkConf conf =
        new SparkConf(false)
            .set(
                GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED().key(),
                Boolean.toString(dynamicSizing));
    final SparkEnv env = mock(SparkEnv.class);
    when(env.conf()).thenReturn(conf);
    SparkEnv.set(env);
  }

  private void assertAccounting(long reserved, long booked) {
    Assert.assertEquals(reserved, target.usedBytes());
    Assert.assertEquals(reserved, target.stats().getCurrent());
    Assert.assertEquals(reserved, delegate.usedBytes());
    Assert.assertEquals(booked, DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
  }

  private void assumeHeapBudgetAvailable() {
    final Runtime runtime = Runtime.getRuntime();
    Assume.assumeTrue(
        "JVM heap is already at its maximum size",
        runtime.totalMemory() + 128L < runtime.maxMemory());
  }

  private static class LimitedMemoryTarget extends NoopMemoryTarget {
    private long maxGrant = Long.MAX_VALUE;
    private long maxRepay = Long.MAX_VALUE;

    @Override
    public long borrow(long size) {
      return super.borrow(Math.min(size, maxGrant));
    }

    @Override
    public long repay(long size) {
      return super.repay(Math.min(Math.min(size, usedBytes()), maxRepay));
    }
  }
}
