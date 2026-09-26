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

import org.apache.spark.memory.SparkMemoryUtil;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.task.TaskResources;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ThrowOnOomMemoryTarget implements MemoryTarget {
  private static final Logger LOG = LoggerFactory.getLogger(ThrowOnOomMemoryTarget.class);
  // Max number of sleeps during retrying the reservation.
  // Durations are orderly 1, 2, 4, 8, 16, 32, 64, 128, 256 (total 511 ms ~ 0.5 s).
  private static final int MAX_SLEEPS = 9;
  private static final int MAX_WAIT_MS = 1000;

  private static final List<String> PRINTED_NON_BYTES_CONFIGURATIONS =
      Arrays.asList(
          GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY(),
          GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED().key());

  private static final List<String> PRINTED_BYTES_CONFIGURATIONS =
      Arrays.asList(
          GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES().key(),
          GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES().key(),
          GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key());

  private final MemoryTarget target;

  public ThrowOnOomMemoryTarget(MemoryTarget target) {
    this.target = target;
  }

  @Override
  public long borrow(long size) {
    long granted;
    {
      granted = target.borrow(size);
      if (granted >= size) {
        return granted;
      }
      if (granted != 0L) {
        target.repay(granted);
      }
    }

    // About to OOM.
    LOG.warn("Off-heap reservation of {} bytes failed.", size);

    // Invoke GC, then retry up to 9 times (1s extra delay in total) for this
    // reservation. This is for ensuring we waited for GC to collect all the
    // non-reachable objects, during which the off-heap allocations might also
    // be returned to the memory manager. For example, UnsafeByteArray implements
    // `finalize` to release its off-heap memory allocation so its lifecycle
    // relies on JVM GC.
    LOG.warn("Invoking GC to try reclaiming some off-heap memory space if applicable...");
    System.gc();
    final long start = System.currentTimeMillis();
    int sleeps = 0;
    long sleepTime = 1;
    while (true) {
      final long elapsedMs = System.currentTimeMillis() - start;
      if (elapsedMs >= MAX_WAIT_MS) {
        LOG.warn("Max wait time (in ms) {} has reached. ", MAX_WAIT_MS);
        break;
      }
      LOG.warn(
          "Retrying reserving {} bytes (finished {}/{} number of sleeps, elapsed {}/{} ms)... ",
          size,
          sleeps,
          MAX_SLEEPS,
          elapsedMs,
          MAX_WAIT_MS);
      granted = target.borrow(size);
      if (granted >= size) {
        return granted;
      }
      if (granted != 0L) {
        target.repay(granted);
      }
      if (sleeps >= MAX_SLEEPS) {
        LOG.warn("Max number of sleeps {} has reached. ", MAX_SLEEPS);
        break;
      }
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // A killed/cancelled task must not be reported as an off-heap OOM.
        throw new RuntimeException(
            "Interrupted while waiting for enough off-heap memory to be granted", e);
      }
      sleepTime *= 2;
      sleeps++;
    }

    // OOM happens.
    // Note if the target is a Spark memory consumer, spilling should already be requested but
    // failed to reclaim enough memory.
    //
    // Log memory usage.
    if (TaskResources.inSparkTask()) {
      TaskResources.getLocalTaskContext().taskMemoryManager().showMemoryUsage();
    }
    // Build error message, then throw.
    final StringBuilder errorBuilder = new StringBuilder();
    errorBuilder
        .append(
            String.format(
                "Not enough spark off-heap execution memory. Acquired: %s, granted: %s. "
                    + "Try tweaking config option spark.memory.offHeap.size to get larger "
                    + "space to run this application "
                    + "(if spark.gluten.memory.dynamic.offHeap.sizing.enabled "
                    + "is not enabled). %n",
                Utils.bytesToString(size), Utils.bytesToString(granted)))
        .append("Current config settings: ")
        .append(System.lineSeparator());
    for (String confKey : PRINTED_NON_BYTES_CONFIGURATIONS) {
      errorBuilder
          .append(String.format("\t%s=%s", confKey, getSqlConfStringOrNa(confKey, v -> v)))
          .append(System.lineSeparator());
    }
    for (String confKey : PRINTED_BYTES_CONFIGURATIONS) {
      errorBuilder
          .append(
              String.format(
                  "\t%s=%s",
                  confKey, getSqlConfStringOrNa(confKey, ThrowOnOomMemoryTarget::reformatBytes)))
          .append(System.lineSeparator());
    }
    // Dump all consumer usages to exception body
    errorBuilder.append(SparkMemoryUtil.dumpMemoryTargetStats(target));
    errorBuilder.append(System.lineSeparator());
    throw new OutOfMemoryException(errorBuilder.toString());
  }

  private static String getSqlConfStringOrNa(String confKey, Function<String, String> ifPresent) {
    final SQLConf sqlConf = SQLConf.get();
    if (!sqlConf.contains(confKey)) {
      return "N/A";
    }
    return ifPresent.apply(sqlConf.getConfString(confKey));
  }

  private static String reformatBytes(String in) {
    return Utils.bytesToString(Utils.byteStringAsBytes(in));
  }

  @Override
  public long repay(long size) {
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public static class OutOfMemoryException extends RuntimeException {
    public OutOfMemoryException(String message) {
      super(message);
    }
  }

  public MemoryTarget target() {
    return target;
  }
}
