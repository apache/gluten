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
package org.apache.gluten.utils;

import org.apache.gluten.test.BoltBackendTestBase;

import org.apache.spark.task.TaskResources$;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.IncompatibleMergeException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.nio.ByteBuffer;

public class BoltBloomFilterTest extends BoltBackendTestBase {
  @Test
  public void testEmpty() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = BoltBloomFilter.empty(10000);
          for (int i = 0; i < 1000; i++) {
            Assert.assertFalse(filter.mightContainLong(i));
          }
          return null;
        });
  }

  @Test
  public void testMalformed() {
    final ByteBuffer buf = ByteBuffer.allocate(5);
    buf.put((byte) 1); // kBloomFilterV1
    buf.putInt(0); // size
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = BoltBloomFilter.readFrom(buf.array());
          Assert.assertThrows(
              "Bloom-filter is not initialized",
              RuntimeException.class,
              new ThrowingRunnable() {
                @Override
                public void run() throws Throwable {
                  filter.mightContainLong(0);
                }
              });
          return null;
        });
  }

  @Test
  public void testSanity() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = BoltBloomFilter.empty(10000);
          final int numItems = 2000;
          final int halfNumItems = numItems / 2;
          for (int i = -halfNumItems; i < halfNumItems; i++) {
            Assert.assertFalse(filter.mightContainLong(i));
          }
          for (int i = -halfNumItems; i < halfNumItems; i++) {
            filter.putLong(i);
            Assert.assertTrue(filter.mightContainLong(i));
          }
          for (int i = -halfNumItems; i < halfNumItems; i++) {
            Assert.assertTrue(filter.mightContainLong(i));
          }

          // Check false positives.
          checkFalsePositives(filter, halfNumItems);

          return null;
        });
  }

  @Test
  public void testStaticMightContainLong() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BoltBloomFilter filter = BoltBloomFilter.empty(100);
          final int numItems = 20;
          final int halfNumItems = numItems / 2;
          for (int i = -halfNumItems; i < halfNumItems; i++) {
            final boolean outcome = filter.mightContainLong(i);
            final ByteBuffer serialized = filter.serializeToDirectBuffer();
            Assert.assertEquals(
                outcome, BoltBloomFilter.mightContainLongOnSerializedBloom(serialized, i));
            Assert.assertFalse(outcome);
          }
          for (int i = -halfNumItems; i < halfNumItems; i++) {
            filter.putLong(i);
            boolean outcome = filter.mightContainLong(i);
            final ByteBuffer serialized = filter.serializeToDirectBuffer();
            Assert.assertEquals(
                outcome, BoltBloomFilter.mightContainLongOnSerializedBloom(serialized, i));
            Assert.assertTrue(outcome);
          }
          for (int i = -halfNumItems; i < halfNumItems; i++) {
            boolean outcome = filter.mightContainLong(i);
            final ByteBuffer serialized = filter.serializeToDirectBuffer();
            Assert.assertEquals(
                outcome, BoltBloomFilter.mightContainLongOnSerializedBloom(serialized, i));
            Assert.assertTrue(outcome);
          }
          return null;
        });
  }

  @Test
  public void testMerge() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter1 = BoltBloomFilter.empty(10000);
          final int start1 = 0;
          final int end1 = 2000;
          for (int i = start1; i < end1; i++) {
            Assert.assertFalse(filter1.mightContainLong(i));
          }
          for (int i = start1; i < end1; i++) {
            filter1.putLong(i);
            Assert.assertTrue(filter1.mightContainLong(i));
          }
          for (int i = start1; i < end1; i++) {
            Assert.assertTrue(filter1.mightContainLong(i));
          }

          final BloomFilter filter2 = BoltBloomFilter.empty(10000);
          final int start2 = 1000;
          final int end2 = 3000;
          for (int i = start2; i < end2; i++) {
            Assert.assertFalse(filter2.mightContainLong(i));
          }
          for (int i = start2; i < end2; i++) {
            filter2.putLong(i);
            Assert.assertTrue(filter2.mightContainLong(i));
          }
          for (int i = start2; i < end2; i++) {
            Assert.assertTrue(filter2.mightContainLong(i));
          }

          try {
            filter1.mergeInPlace(filter2);
          } catch (IncompatibleMergeException e) {
            throw new RuntimeException(e);
          }

          for (int i = start1; i < end2; i++) {
            Assert.assertTrue(filter1.mightContainLong(i));
          }

          // Check false positives.
          checkFalsePositives(filter1, end2);

          return null;
        });
  }

  @Test
  public void testSerde() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BoltBloomFilter filter = BoltBloomFilter.empty(10000);
          for (int i = 0; i < 1000; i++) {
            filter.putLong(i);
          }

          byte[] data1 = filter.serialize();

          final BoltBloomFilter filter2 = BoltBloomFilter.readFrom(data1);
          byte[] data2 = filter2.serialize();

          Assert.assertArrayEquals(data2, data1);
          return null;
        });
  }

  private static void checkFalsePositives(BloomFilter filter, int start) {
    final int attemptStart = start;
    final int attemptCount = 5000000;

    int falsePositives = 0;
    int negativeFalsePositives = 0;

    for (int i = attemptStart; i < attemptStart + attemptCount; i++) {
      if (filter.mightContainLong(i)) {
        falsePositives++;
      }
      if (filter.mightContainLong(-i)) {
        negativeFalsePositives++;
      }
    }

    Assert.assertTrue(falsePositives > 0);
    Assert.assertTrue(falsePositives < attemptCount);
    Assert.assertTrue(negativeFalsePositives > 0);
    Assert.assertTrue(negativeFalsePositives < attemptCount);
  }
}
