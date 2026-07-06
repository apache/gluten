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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Ledger-correctness tests for {@link DynamicOffHeapSizingMemoryTarget}.
 *
 * <p>Regression this class guards against: the wrapped {@code target.borrow(size)} can grant less
 * than {@code size} (Spark's execution memory pool is allowed to return a partial reservation).
 * Booking the requested amount into both {@code USED_OFF_HEAP_BYTES} and the local recorder — as
 * the pre-fix code did — caused the counters to drift above the true reservation. That drift makes
 * the class's own {@code exceedsMaxMemoryUsage()} gate reject subsequent borrows that are in fact
 * within budget. {@code repay} had the mirrored bug.
 *
 * <p>Assertions cover both counters. {@link DynamicOffHeapSizingMemoryTarget#usedBytes()} reads the
 * per-instance recorder, but the actual gate is the static {@code USED_OFF_HEAP_BYTES}, so a
 * regression that fixed one and re-broke the other would slip past an assertions-on-recorder-only
 * suite; we verify both via package-private test hooks.
 *
 * <p><b>JVM-heap assumption.</b> The {@code borrow(long)} implementation enters an on-heap shrink
 * path when {@code Runtime.totalMemory() + size >= Runtime.maxMemory()}. These tests use small
 * allocations (≤128 bytes) and assume the surefire JVM has room to spare, which holds under
 * Gluten's default test JVM. A future config that pins the heap at its max from process start (e.g.
 * {@code -Xms} == {@code -Xmx}) would send every borrow through the shrink branch; if that happens,
 * add a test-only injection seam for {@code Runtime.totalMemory()} rather than relaxing the ledger
 * assertions.
 */
public class DynamicOffHeapSizingMemoryTargetTest {

  @Before
  public void resetStaticCounter() {
    DynamicOffHeapSizingMemoryTarget.resetUsedOffHeapBytesForTesting();
  }

  @After
  public void confirmStaticCounterZero() {
    // Every test must leave the shared JVM-wide counter clean, so downstream tests in the same
    // fork are not perturbed. A test that fails this assertion is either missing a repay or is
    // exposing a real accounting leak.
    Assert.assertEquals(
        "static USED_OFF_HEAP_BYTES leaked past a test method",
        0L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
  }

  @Test
  public void borrowRecordsOnlyTheGrantedAmountForFullGrants() {
    final CountingTarget wrapped = new CountingTarget(Long.MAX_VALUE);
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    final long granted = target.borrow(128L);

    Assert.assertEquals("full grant returned verbatim", 128L, granted);
    Assert.assertEquals("recorder tracks the grant", 128L, target.usedBytes());
    Assert.assertEquals(
        "static counter tracks the grant",
        128L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
    Assert.assertEquals("wrapped target saw the same size", 128L, wrapped.currentReserved());

    // Restore the invariant for the @After check.
    target.repay(128L);
  }

  @Test
  public void borrowRecordsOnlyTheGrantedAmountForPartialGrants() {
    // Wrapped target can only grant 40 out of every 100 requested.
    final CountingTarget wrapped = new CountingTarget(40L);
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    final long granted = target.borrow(100L);

    Assert.assertEquals("return value reflects the actual grant", 40L, granted);
    Assert.assertEquals("recorder must not book the 60-byte overshoot", 40L, target.usedBytes());
    Assert.assertEquals(
        "static counter must not book the 60-byte overshoot — this is the gate for the class's own"
            + " exceedsMaxMemoryUsage() check, so drift here is the whole regression",
        40L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
    Assert.assertEquals("wrapped state matches", 40L, wrapped.currentReserved());

    target.repay(40L);
  }

  @Test
  public void borrowRecordsZeroWhenTargetGrantsNothing() {
    final CountingTarget wrapped = new CountingTarget(0L);
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    final long granted = target.borrow(64L);

    Assert.assertEquals("zero grant surfaces to caller", 0L, granted);
    // The class either consults the wrapped target once (JVM-heap-headroom path) or short-circuits
    // on OOM before ever calling it — both are correct outcomes for the ledger. Anything above 1
    // would mean DynamicOffHeapSizingMemoryTarget.borrow developed an internal retry loop, which
    // is not a behaviour we want to sneak in silently.
    Assert.assertTrue(
        "wrapped target must be consulted at most once — a call count above 1 would indicate an"
            + " unintended retry loop inside DynamicOffHeapSizingMemoryTarget.borrow",
        wrapped.borrowCalls() <= 1);
    Assert.assertEquals("recorder untouched on zero grant", 0L, target.usedBytes());
    Assert.assertEquals(
        "static counter untouched on zero grant",
        0L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
  }

  @Test
  public void sequentialPartialBorrowsAccumulateInBothLedgers() {
    // The other partial-grant test measures a single call. Confirm the ledger *accumulates* the
    // grants — a regression that used AtomicLong.set(...) instead of addAndGet(...) would ship
    // green against a single-call suite and reintroduce the exact drift this class guards.
    final CountingTarget wrapped = new CountingTarget(40L);
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    Assert.assertEquals(40L, target.borrow(100L));
    Assert.assertEquals(40L, target.borrow(100L));

    Assert.assertEquals("recorder accumulates across calls", 80L, target.usedBytes());
    Assert.assertEquals(
        "static counter accumulates across calls",
        80L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());

    target.repay(80L);
  }

  @Test
  public void repayReturnsAndRecordsOnlyTheAmountActuallyFreed() {
    // Grant 100 via borrow so all three ledgers (fake, recorder, static counter) start in sync
    // at 100. Then ask the target to repay 100 but let it only free 30.
    final PartialRepayTarget wrapped = new PartialRepayTarget(30L);
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    Assert.assertEquals(100L, target.borrow(100L));
    Assert.assertEquals(100L, target.usedBytes());
    Assert.assertEquals(100L, DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());

    final long freed = target.repay(100L);

    Assert.assertEquals("repay returns what the wrapped target freed", 30L, freed);
    Assert.assertEquals(
        "recorder decrements by freed bytes, not requested bytes", 70L, target.usedBytes());
    Assert.assertEquals(
        "static counter decrements by freed bytes, not requested bytes",
        70L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());

    // Drain the remaining 70 bytes so the @After check passes. The fake frees 30 per call
    // (bounded by the reserved held), so three more repays clear it (30 + 30 + 10).
    Assert.assertEquals(30L, target.repay(70L));
    Assert.assertEquals(30L, target.repay(40L));
    Assert.assertEquals(10L, target.repay(10L));
    Assert.assertEquals(0L, target.usedBytes());
  }

  @Test
  public void borrowOfZeroIsANoOp() {
    // Guardrail: the short-circuit for size==0 predates this fix; verify it still holds so we
    // don't accidentally introduce a spurious target.borrow(0) call through the fix.
    final CountingTarget wrapped = new CountingTarget(Long.MAX_VALUE);
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    Assert.assertEquals(0L, target.borrow(0L));
    Assert.assertEquals("no interaction with wrapped target on size==0", 0, wrapped.borrowCalls());
  }

  @Test
  public void repayOfZeroIsANoOp() {
    // Symmetric guardrail for repay(0): must not reach the wrapped target and must not perturb
    // either counter.
    final CountingTarget wrapped = new CountingTarget(Long.MAX_VALUE);
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    Assert.assertEquals(0L, target.repay(0L));
    Assert.assertEquals("no interaction with wrapped target on size==0", 0, wrapped.repayCalls());
    Assert.assertEquals(
        "static counter untouched",
        0L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
  }

  @Test
  public void borrowLeavesLedgerUntouchedWhenWrappedTargetThrows() {
    // The whole point of the fix is that USED_OFF_HEAP_BYTES / recorder are only mutated AFTER
    // target.borrow returns normally. A regression back to the pre-fix ordering (increment first,
    // then borrow) would leak the requested size on every throwing borrow. This test locks that
    // in. Under the JVM-heap assumption documented on the class the wrapped target's throw is
    // observed; if the class's own OOM early-return fires first (heap-constrained JVM) the
    // wrapped throw is never reached — both outcomes leave the ledger untouched, which is what
    // the fix actually guards.
    final ThrowingBorrowTarget wrapped = new ThrowingBorrowTarget();
    final DynamicOffHeapSizingMemoryTarget target = new DynamicOffHeapSizingMemoryTarget(wrapped);

    long returned = 0L;
    RuntimeException observed = null;
    try {
      returned = target.borrow(256L);
    } catch (RuntimeException e) {
      observed = e;
    }
    if (observed != null) {
      Assert.assertEquals(
          "wrapped target's throw surfaced verbatim", "simulated", observed.getMessage());
    } else {
      Assert.assertEquals(
          "if the class OOM-early-returned instead of consulting the target, it must return 0",
          0L,
          returned);
    }
    Assert.assertEquals("recorder untouched", 0L, target.usedBytes());
    Assert.assertEquals(
        "static counter untouched regardless of which branch fired",
        0L,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
  }

  @Test
  public void repayLeavesLedgerUntouchedWhenWrappedTargetThrows() {
    // Symmetric guard for repay: pre-fix decremented before calling target.repay, so a throw
    // there would silently under-count. Snapshot the static counter, run a throwing repay on a
    // separate DOHS instance (so its instance recorder starts at 0 and no seeder is needed), and
    // assert the shared static counter didn't move. No JVM-heap assumption needed — repay has no
    // shrink path.
    final long beforeStatic = DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting();

    final ThrowingRepayTarget throwing = new ThrowingRepayTarget();
    final DynamicOffHeapSizingMemoryTarget repayer = new DynamicOffHeapSizingMemoryTarget(throwing);
    try {
      repayer.repay(50L);
      Assert.fail("expected wrapped target to throw");
    } catch (RuntimeException expected) {
      Assert.assertEquals("simulated", expected.getMessage());
    }
    Assert.assertEquals("throwing repayer's recorder untouched", 0L, repayer.usedBytes());
    Assert.assertEquals(
        "static counter untouched by the throwing repay",
        beforeStatic,
        DynamicOffHeapSizingMemoryTarget.usedOffHeapBytesForTesting());
  }

  /**
   * Fake target: grants at most {@link #grantPerCall} bytes per call, matches Spark's contract of
   * granting between 0 and the requested size inclusive. {@link #repay(long)} is a plain
   * capacity-bounded free (mirrors {@link
   * org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumer.Node#repay}). The partial-repay
   * case lives in {@link PartialRepayTarget} instead of folding both behaviours into one fake.
   */
  private static final class CountingTarget implements MemoryTarget {
    private final long grantPerCall;
    private long reserved = 0L;
    private int borrowCalls = 0;
    private int repayCalls = 0;

    CountingTarget(long grantPerCall) {
      this.grantPerCall = grantPerCall;
    }

    @Override
    public long borrow(long size) {
      borrowCalls++;
      final long granted = Math.min(size, grantPerCall);
      reserved += granted;
      return granted;
    }

    @Override
    public long repay(long size) {
      repayCalls++;
      final long freed = Math.min(size, reserved);
      reserved -= freed;
      return freed;
    }

    @Override
    public long usedBytes() {
      return reserved;
    }

    @Override
    public <T> T accept(MemoryTargetVisitor<T> visitor) {
      throw new UnsupportedOperationException("not needed for these tests");
    }

    long currentReserved() {
      return reserved;
    }

    int borrowCalls() {
      return borrowCalls;
    }

    int repayCalls() {
      return repayCalls;
    }
  }

  /**
   * Fake target modelling {@link
   * org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumer.Node#repay}'s partial-free
   * behaviour: {@code repay} returns {@code min(size, freePerCall, reserved)}, so it obeys the
   * production invariant that a target never frees more than requested.
   */
  private static final class PartialRepayTarget implements MemoryTarget {
    private long reserved = 0L;
    private final long freePerCall;

    PartialRepayTarget(long freePerCall) {
      this.freePerCall = freePerCall;
    }

    @Override
    public long borrow(long size) {
      final long granted = Math.min(size, Long.MAX_VALUE - reserved);
      reserved += granted;
      return granted;
    }

    @Override
    public long repay(long size) {
      // freed = min(freePerCall, size, reserved) — respects both the "no more than requested"
      // production invariant and the fake's "at most freePerCall per call" ceiling.
      final long freed = Math.min(Math.min(freePerCall, size), reserved);
      reserved -= freed;
      return freed;
    }

    @Override
    public long usedBytes() {
      return reserved;
    }

    @Override
    public <T> T accept(MemoryTargetVisitor<T> visitor) {
      throw new UnsupportedOperationException("not needed for these tests");
    }
  }

  private static final class ThrowingBorrowTarget implements MemoryTarget {
    @Override
    public long borrow(long size) {
      throw new RuntimeException("simulated");
    }

    @Override
    public long repay(long size) {
      return 0L;
    }

    @Override
    public long usedBytes() {
      return 0L;
    }

    @Override
    public <T> T accept(MemoryTargetVisitor<T> visitor) {
      throw new UnsupportedOperationException("not needed for these tests");
    }
  }

  private static final class ThrowingRepayTarget implements MemoryTarget {
    @Override
    public long borrow(long size) {
      return 0L;
    }

    @Override
    public long repay(long size) {
      throw new RuntimeException("simulated");
    }

    @Override
    public long usedBytes() {
      return 0L;
    }

    @Override
    public <T> T accept(MemoryTargetVisitor<T> visitor) {
      throw new UnsupportedOperationException("not needed for these tests");
    }
  }
}
