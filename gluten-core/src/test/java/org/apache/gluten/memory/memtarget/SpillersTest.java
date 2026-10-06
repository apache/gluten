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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SpillersTest {

  private static Spiller countingSpiller(AtomicInteger counter) {
    return new Spiller() {
      @Override
      public long spill(MemoryTarget self, Phase phase, long size) {
        counter.incrementAndGet();
        // Reclaim nothing, so the caller walks the rest of the list.
        return 0L;
      }
    };
  }

  private static Spiller recordingSpiller(String name, List<String> order, AtomicInteger counter) {
    return new Spiller() {
      @Override
      public long spill(MemoryTarget self, Phase phase, long size) {
        order.add(name);
        counter.incrementAndGet();
        return 0L;
      }
    };
  }

  @Test
  public void testAppendFromAnotherThreadDuringSpill() throws Exception {
    // Callers append after the list is registered with the task's memory tree, and a spill runs on
    // whichever thread hit the limit. The latches pin that interleaving: the appending thread runs
    // while the spilling thread sits between two entries.
    final Spillers.AppendableSpillerList spillers = Spillers.appendable();
    final MemoryTarget target = new NoopMemoryTarget();
    final AtomicInteger spillCount = new AtomicInteger(0);
    final AtomicInteger appendedSpills = new AtomicInteger(0);
    final List<String> order = new CopyOnWriteArrayList<>();
    final CountDownLatch reachedMiddle = new CountDownLatch(1);
    final CountDownLatch appended = new CountDownLatch(1);
    final AtomicReference<Throwable> appendFailure = new AtomicReference<>();

    spillers.append(recordingSpiller("first", order, spillCount));
    spillers.append(
        new Spiller() {
          @Override
          public long spill(MemoryTarget self, Phase phase, long size) {
            order.add("blocking");
            spillCount.incrementAndGet();
            reachedMiddle.countDown();
            try {
              Assert.assertTrue(
                  "appending thread did not finish within 30s",
                  appended.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
            return 0L;
          }
        });
    spillers.append(recordingSpiller("third", order, spillCount));

    final Thread appender =
        new Thread(
            () -> {
              try {
                Assert.assertTrue(
                    "spilling thread did not reach the middle spiller within 30s",
                    reachedMiddle.await(30, TimeUnit.SECONDS));
                spillers.append(countingSpiller(appendedSpills));
              } catch (Throwable t) {
                appendFailure.compareAndSet(null, t);
              } finally {
                appended.countDown();
              }
            },
            "spiller-appender");
    appender.setDaemon(true);
    appender.start();
    final List<String> firstWalk;
    try {
      Assert.assertEquals(0, spillers.spill(target, Spiller.Phase.SPILL, 100));
    } finally {
      // Snapshot before the second walk records into the same list.
      firstWalk = Arrays.asList(order.toArray(new String[0]));
      appender.join(TimeUnit.SECONDS.toMillis(30));
    }

    if (appendFailure.get() != null) {
      Assert.fail("Appending thread failed: " + appendFailure.get());
    }
    Assert.assertFalse("appender thread is still running after join", appender.isAlive());
    // The walk covers the three entries present when it started, in registration order.
    Assert.assertEquals(Arrays.asList("first", "blocking", "third"), firstWalk);
    Assert.assertEquals(3, spillCount.get());
    Assert.assertEquals(0, appendedSpills.get());

    // The spiller appended mid-walk did reach the list: it takes part in the next walk.
    Assert.assertEquals(0, spillers.spill(target, Spiller.Phase.SPILL, 100));
    Assert.assertEquals(1, appendedSpills.get());
    Assert.assertEquals(6, spillCount.get());
  }

  @Test
  public void testAppendDuringOwnSpill() {
    // No production spiller appends today, but a lock around append would not cover one that did,
    // so pin the re-entrant case as well.
    final Spillers.AppendableSpillerList spillers = Spillers.appendable();
    final MemoryTarget target = new NoopMemoryTarget();
    final AtomicInteger appendedSpills = new AtomicInteger(0);
    final AtomicInteger spillCount = new AtomicInteger(0);

    spillers.append(
        new Spiller() {
          @Override
          public long spill(MemoryTarget self, Phase phase, long size) {
            spillCount.incrementAndGet();
            spillers.append(countingSpiller(appendedSpills));
            return 0L;
          }
        });
    spillers.append(countingSpiller(spillCount));

    Assert.assertEquals(0, spillers.spill(target, Spiller.Phase.SPILL, 100));
    Assert.assertEquals(2, spillCount.get());
    Assert.assertEquals(0, appendedSpills.get());
    // The appended spiller takes part in the next walk.
    Assert.assertEquals(0, spillers.spill(target, Spiller.Phase.SPILL, 100));
    Assert.assertEquals(1, appendedSpills.get());
  }

  @Test
  public void testSpillStopsOnceTheRequestIsMet() {
    // Pins the pre-existing short-circuit in AppendableSpillerList#spill, not this fix: once the
    // request is met the walk stops, so later entries are never consulted.
    final Spillers.AppendableSpillerList spillers = Spillers.appendable();
    final MemoryTarget target = new NoopMemoryTarget();
    final AtomicInteger laterSpills = new AtomicInteger(0);

    spillers.append(
        new Spiller() {
          @Override
          public long spill(MemoryTarget self, Phase phase, long size) {
            return size;
          }
        });
    spillers.append(countingSpiller(laterSpills));

    Assert.assertEquals(100, spillers.spill(target, Spiller.Phase.SPILL, 100));
    Assert.assertEquals(0, laterSpills.get());
  }
}
