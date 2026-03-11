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
package org.apache.gluten.memory.memtarget.spark;

import org.apache.gluten.config.GlutenCoreConfig;
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.memory.memtarget.TreeMemoryTarget;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.task.TaskResources$;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import scala.Function0;

public class TreeMemoryConsumerTest {
  @Before
  public void setUp() throws Exception {
    final SQLConf conf = SQLConf.get();
    conf.setConfString("spark.memory.offHeap.enabled", "true");
    conf.setConfString("spark.memory.offHeap.size", "400");
    conf.setConfString(
        GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key(), "100");
  }

  @Test
  public void testIsolated() {
    test(
        () -> {
          final TreeMemoryConsumers.Factory factory =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP);
          final TreeMemoryTarget consumer =
              factory
                  .isolatedRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(10, consumer.borrow(20));
          Assert.assertEquals(0, consumer.borrow(20));
        });
  }

  @Test
  public void testLegacy() {
    test(
        () -> {
          final TreeMemoryConsumers.Factory factory =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP);
          final TreeMemoryTarget consumer =
              factory
                  .legacyRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(20, consumer.borrow(20));
        });
  }

  @Test
  public void testIsolatedAndLegacy() {
    test(
        () -> {
          final TreeMemoryTarget legacy =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP)
                  .legacyRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(110, legacy.borrow(110));
          final TreeMemoryTarget isolated =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP)
                  .isolatedRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(100, isolated.borrow(110));
        });
  }

  @Test
  public void testSpill() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget legacy =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP)
                  .legacyRoot()
                  .newChild(
                      "FOO", TreeMemoryTarget.CAPACITY_UNLIMITED, spillers, Collections.emptyMap());
          final AtomicInteger numSpills = new AtomicInteger(0);
          final AtomicLong numSpilledBytes = new AtomicLong(0L);
          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  long repaid = legacy.repay(size);
                  numSpills.getAndIncrement();
                  numSpilledBytes.getAndAdd(repaid);
                  return repaid;
                }
              });
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(1, numSpills.get());
          Assert.assertEquals(200, numSpilledBytes.get());
          Assert.assertEquals(400, legacy.usedBytes());

          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(3, numSpills.get());
          Assert.assertEquals(800, numSpilledBytes.get());
          Assert.assertEquals(400, legacy.usedBytes());
        });
  }

  @Test
  public void testOverSpill() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget legacy =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP)
                  .legacyRoot()
                  .newChild(
                      "FOO", TreeMemoryTarget.CAPACITY_UNLIMITED, spillers, Collections.emptyMap());
          final AtomicInteger numSpills = new AtomicInteger(0);
          final AtomicLong numSpilledBytes = new AtomicLong(0L);
          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  long repaid = legacy.repay(Long.MAX_VALUE);
                  numSpills.getAndIncrement();
                  numSpilledBytes.getAndAdd(repaid);
                  return repaid;
                }
              });
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(1, numSpills.get());
          Assert.assertEquals(300, numSpilledBytes.get());
          Assert.assertEquals(300, legacy.usedBytes());

          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(3, numSpills.get());
          Assert.assertEquals(900, numSpilledBytes.get());
          Assert.assertEquals(300, legacy.usedBytes());
        });
  }

  /**
   * Test concurrent child addition and spilling operations. This test reproduces the
   * ConcurrentModificationException that occurs when one thread adds children while another thread
   * is iterating during spilling.
   */
  @Test
  public void testConcurrentAddAndSpill() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget root =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP).legacyRoot();

          final AtomicInteger spillCount = new AtomicInteger(0);
          final AtomicReference<Throwable> failure = new AtomicReference<>();
          final AtomicInteger childrenAdded = new AtomicInteger(0);

          // Create initial child with spiller
          final TreeMemoryTarget initialChild =
              root.newChild(
                  "INITIAL", TreeMemoryTarget.CAPACITY_UNLIMITED, spillers, Collections.emptyMap());

          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  spillCount.incrementAndGet();
                  // Simulate spilling by repaying some memory
                  return initialChild.repay(size / 2);
                }
              });

          // Allocate some memory to trigger spilling
          initialChild.borrow(200);

          final int numThreads = 4;
          final int operationsPerThread = 50;
          final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
          final CyclicBarrier barrier = new CyclicBarrier(numThreads);
          final CountDownLatch latch = new CountDownLatch(numThreads);

          // Thread 1 & 2: Add children concurrently
          for (int t = 0; t < 2; t++) {
            executor.submit(
                () -> {
                  try {
                    barrier.await(); // Synchronize start
                    for (int i = 0; i < operationsPerThread; i++) {
                      String childName = "CHILD_" + Thread.currentThread().getId() + "_" + i;
                      root.newChild(
                          childName,
                          TreeMemoryTarget.CAPACITY_UNLIMITED,
                          Spillers.NOOP,
                          Collections.emptyMap());
                      childrenAdded.incrementAndGet();
                      Thread.sleep(1); // Small delay to increase contention
                    }
                  } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                      Thread.currentThread().interrupt();
                    }
                    failure.compareAndSet(null, e);
                  } finally {
                    latch.countDown();
                  }
                });
          }

          // Thread 3 & 4: Trigger spilling and stats collection concurrently
          for (int t = 0; t < 2; t++) {
            executor.submit(
                () -> {
                  try {
                    barrier.await(); // Synchronize start
                    for (int i = 0; i < operationsPerThread; i++) {
                      // Trigger spilling by borrowing memory
                      initialChild.borrow(100);
                      // Also collect stats which iterates over children
                      root.stats();
                      Thread.sleep(1); // Small delay to increase contention
                    }
                  } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                      Thread.currentThread().interrupt();
                    }
                    failure.compareAndSet(null, e);
                  } finally {
                    latch.countDown();
                  }
                });
          }

          // Wait for all threads to complete
          try {
            Assert.assertTrue(
                "Threads did not complete in time", latch.await(30, TimeUnit.SECONDS));
            executor.shutdown();
            Assert.assertTrue(
                "Executor did not terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("Test interrupted: " + e.getMessage());
          }

          // Verify no exceptions occurred
          if (failure.get() != null) {
            Assert.fail("Test failed due to concurrent modification: " + failure.get());
          }

          // Verify children were added
          Assert.assertEquals(
              "Expected children to be added", operationsPerThread * 2, childrenAdded.get());

          // Verify spilling occurred
          Assert.assertTrue("Expected spilling to occur", spillCount.get() > 0);
        });
  }

  /**
   * Test concurrent stats collection while modifying the tree. This ensures that stats() can be
   * called safely while children are being added/removed.
   */
  @Test
  public void testConcurrentStatsCollection() {
    test(
        () -> {
          final TreeMemoryTarget root =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP).legacyRoot();
          final TreeMemoryTarget warmup =
              root.newChild(
                  "WARMUP",
                  TreeMemoryTarget.CAPACITY_UNLIMITED,
                  Spillers.NOOP,
                  Collections.emptyMap());
          warmup.borrow(1);

          final AtomicReference<Throwable> failure = new AtomicReference<>();
          final AtomicLong totalBytesObserved = new AtomicLong(0);

          final int numThreads = 6;
          final int operationsPerThread = 100;
          final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
          final CyclicBarrier barrier = new CyclicBarrier(numThreads);
          final CountDownLatch latch = new CountDownLatch(numThreads);

          // Threads 1-3: Add children and allocate memory
          for (int t = 0; t < 3; t++) {
            final int threadId = t;
            executor.submit(
                () -> {
                  try {
                    barrier.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                      String childName = "CHILD_" + threadId + "_" + i;
                      TreeMemoryTarget child =
                          root.newChild(
                              childName,
                              TreeMemoryTarget.CAPACITY_UNLIMITED,
                              Spillers.NOOP,
                              Collections.emptyMap());
                      child.borrow(10);
                    }
                  } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                      Thread.currentThread().interrupt();
                    }
                    failure.compareAndSet(null, e);
                  } finally {
                    latch.countDown();
                  }
                });
          }

          // Threads 4-6: Continuously collect stats
          for (int t = 0; t < 3; t++) {
            executor.submit(
                () -> {
                  try {
                    barrier.await();
                    for (int i = 0; i < operationsPerThread * 2; i++) {
                      long used = root.stats().getCurrent();
                      totalBytesObserved.addAndGet(used);
                    }
                  } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                      Thread.currentThread().interrupt();
                    }
                    failure.compareAndSet(null, e);
                  } finally {
                    latch.countDown();
                  }
                });
          }

          try {
            Assert.assertTrue(
                "Threads did not complete in time", latch.await(30, TimeUnit.SECONDS));
            executor.shutdown();
            Assert.assertTrue(
                "Executor did not terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("Test interrupted: " + e.getMessage());
          }

          if (failure.get() != null) {
            Assert.fail("Test failed due to concurrent modification: " + failure.get());
          }
          Assert.assertTrue("Expected to observe memory usage", totalBytesObserved.get() > 0);
        });
  }

  /**
   * Stress test with high contention on multiple operations. This test hammers the
   * TreeMemoryConsumer with concurrent operations to expose race conditions.
   */
  @Test
  public void testHighContention() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget root =
              TreeMemoryConsumers.factory(MemoryMode.OFF_HEAP).legacyRoot();

          final AtomicInteger spillCount = new AtomicInteger(0);
          final AtomicReference<Throwable> failure = new AtomicReference<>();

          // Create a child with spiller
          final TreeMemoryTarget spillableChild =
              root.newChild(
                  "SPILLABLE",
                  TreeMemoryTarget.CAPACITY_UNLIMITED,
                  spillers,
                  Collections.emptyMap());

          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  spillCount.incrementAndGet();
                  return spillableChild.repay(size / 2);
                }
              });

          spillableChild.borrow(150);

          final int numThreads = 8;
          final int operationsPerThread = 100;
          final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
          final CyclicBarrier barrier = new CyclicBarrier(numThreads);
          final CountDownLatch latch = new CountDownLatch(numThreads);

          for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(
                () -> {
                  try {
                    barrier.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                      // Mix of operations
                      switch (i % 4) {
                        case 0:
                          // Add child
                          root.newChild(
                              "CHILD_" + threadId + "_" + i,
                              TreeMemoryTarget.CAPACITY_UNLIMITED,
                              Spillers.NOOP,
                              Collections.emptyMap());
                          break;
                        case 1:
                          // Trigger spilling
                          spillableChild.borrow(50);
                          break;
                        case 2:
                          // Collect stats
                          root.stats();
                          break;
                        case 3:
                          // Repay memory
                          spillableChild.repay(10);
                          break;
                      }
                    }
                  } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                      Thread.currentThread().interrupt();
                    }
                    failure.compareAndSet(null, e);
                  } finally {
                    latch.countDown();
                  }
                });
          }

          try {
            Assert.assertTrue(
                "Threads did not complete in time", latch.await(60, TimeUnit.SECONDS));
            executor.shutdown();
            Assert.assertTrue(
                "Executor did not terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("Test interrupted: " + e.getMessage());
          }

          if (failure.get() != null) {
            Assert.fail("Test failed due to concurrent access issues: " + failure.get());
          }
          Assert.assertTrue("Expected spilling to occur", spillCount.get() > 0);
        });
  }

  private void test(Runnable r) {
    TaskResources$.MODULE$.runUnsafe(
        new Function0<Object>() {
          @Override
          public Object apply() {
            r.run();
            return null;
          }
        });
  }
}
