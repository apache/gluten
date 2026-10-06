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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MemoryTargetUtilTest {

  /** The counter is JVM-global and shared with the rest of the suite, so only compare suffixes. */
  private static long suffixOf(String uniqueName) {
    final int dot = uniqueName.lastIndexOf('.');
    Assert.assertTrue("Name carries no suffix: " + uniqueName, dot > 0);
    return Long.parseLong(uniqueName.substring(dot + 1));
  }

  @Test
  public void testNamingHoldsNoPerNameState() {
    // This is the property the fix exists for. Counters kept per name meant one entry per distinct
    // name for the lifetime of the JVM, and callers do not draw names from a fixed set, so the
    // registry grew without bound. Asserting on the suffixes alone would not catch a registry that
    // stores an entry per name and takes the suffix from a shared counter, so check that the
    // sequence is the only state the class keeps, and that it accounts for every call.
    for (Field f : MemoryTargetUtil.class.getDeclaredFields()) {
      Assert.assertFalse(
          "Naming should hold no per-name state, found "
              + f.getType().getName()
              + " "
              + f.getName(),
          Map.class.isAssignableFrom(f.getType())
              || Collection.class.isAssignableFrom(f.getType()));
    }

    final long before = MemoryTargetUtil.sequenceForTesting();
    MemoryTargetUtil.toUniqueName("Foo");
    MemoryTargetUtil.toUniqueName("Bar");
    MemoryTargetUtil.toUniqueName("Foo");
    Assert.assertEquals(
        "Every call should consume one number from the shared sequence",
        3L,
        MemoryTargetUtil.sequenceForTesting() - before);
  }

  @Test
  public void testSuffixesComeFromOneSequenceRatherThanPerNameCounters() {
    final long first = suffixOf(MemoryTargetUtil.toUniqueName("Foo"));
    final long second = suffixOf(MemoryTargetUtil.toUniqueName("Bar"));
    final long third = suffixOf(MemoryTargetUtil.toUniqueName("Foo"));

    Assert.assertTrue(
        "Suffix did not advance across names: " + first + ", " + second, second > first);
    Assert.assertTrue(
        "Suffix did not advance across names: " + second + ", " + third, third > second);
  }

  @Test
  public void testRepeatedNamesStayDistinct() {
    // Sibling memory targets are keyed by name in TreeMemoryConsumer#newChild, which throws on a
    // collision, so two calls with the same input must never produce the same output.
    final String one = MemoryTargetUtil.toUniqueName("Gluten.Tree");
    final String two = MemoryTargetUtil.toUniqueName("Gluten.Tree");
    Assert.assertNotEquals(one, two);
    Assert.assertTrue(one.startsWith("Gluten.Tree."));
    Assert.assertTrue(two.startsWith("Gluten.Tree."));
  }

  @Test
  public void testConcurrentCallsAreAllDistinct() throws Exception {
    final int threads = 8;
    final int perThread = 500;
    final ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<Set<String>>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            executor.submit(
                () -> {
                  final Set<String> local = new HashSet<>();
                  for (int i = 0; i < perThread; i++) {
                    local.add(MemoryTargetUtil.toUniqueName("Concurrent"));
                  }
                  return local;
                }));
      }
      final Set<String> names = new HashSet<>();
      for (Future<Set<String>> f : futures) {
        final Set<String> local = f.get(30, TimeUnit.SECONDS);
        Assert.assertEquals("A thread saw a duplicate name", perThread, local.size());
        names.addAll(local);
      }
      Assert.assertEquals("Two threads got the same name", threads * perThread, names.size());
    } finally {
      executor.shutdownNow();
    }
  }
}
