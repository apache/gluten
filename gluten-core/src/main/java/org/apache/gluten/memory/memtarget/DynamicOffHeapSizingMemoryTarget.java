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

import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.proto.MemoryUsageStats;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.util.SparkThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The memory target used by dynamic off-heap sizing. Since
 * https://github.com/apache/gluten/issues/5439.
 */
@Experimental
public class DynamicOffHeapSizingMemoryTarget implements MemoryTarget, KnownNameAndStats {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicOffHeapSizingMemoryTarget.class);
  // When dynamic off-heap sizing is enabled, the off-heap should be sized for the total usable
  // memory, so we can use it as the max memory we will use.
  private static final long TOTAL_MEMORY_SHARED;

  private static final double ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO = 0.85;
  private static final double ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO = 0.65;
  private static final double GC_MAX_HEAP_FREE_RATIO = 0.05;
  private static final int MAX_GC_RETRY_TIMES = 3;

  private static final AtomicBoolean ASYNC_GC_SUSPEND = new AtomicBoolean(false);
  private static final Object JVM_SHRINK_SYNC_OBJECT = new Object();

  private static final int ORIGINAL_MAX_HEAP_FREE_RATIO;
  private static final int ORIGINAL_MIN_HEAP_FREE_RATIO;

  static {
    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    List<String> jvmArgs = runtimeMxBean.getInputArguments();
    int originalMaxHeapFreeRatio = 70;
    int originalMinHeapFreeRatio = 40;
    for (String arg : jvmArgs) {
      if (arg.startsWith("-XX:MaxHeapFreeRatio=")) {
        String valuePart = arg.substring(arg.indexOf('=') + 1);
        try {
          originalMaxHeapFreeRatio = Integer.parseInt(valuePart);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Failed to parse MaxHeapFreeRatio from JVM argument: {}. Using default value: {}.",
              arg,
              originalMaxHeapFreeRatio);
        }
      } else if (arg.startsWith("-XX:MinHeapFreeRatio=")) {
        String valuePart = arg.substring(arg.indexOf('=') + 1);
        try {
          originalMinHeapFreeRatio = Integer.parseInt(valuePart);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Failed to parse MinHeapFreeRatio from JVM argument: {}. Using default value: {}.",
              arg,
              originalMinHeapFreeRatio);
        }
      } else if (Objects.equals(arg, "-XX:+ExplicitGCInvokesConcurrent")) {
        // If this is set -XX:+ExplicitGCInvokesConcurrent, System.gc() does not trigger Full GC,
        // so explicit JVM shrinking is not effective.
        LOG.error(
            "Explicit JVM shrinking is not effective because -XX:+ExplicitGCInvokesConcurrent"
                + " is set. Please check the JVM arguments: {}. ",
            arg);

      } else if (Objects.equals(arg, "-XX:+DisableExplicitGC")) {
        // If -XX:+DisableExplicitGC is set, calls to System.gc() are ignored,
        // so explicit JVM shrinking will not work as intended.
        LOG.error(
            "Explicit JVM shrinking is disabled because -XX:+DisableExplicitGC is set. "
                + "System.gc() calls will be ignored and JVM shrinking will not work. "
                + "Please check the JVM arguments: {}. ",
            arg);
      }
    }
    ORIGINAL_MIN_HEAP_FREE_RATIO = originalMinHeapFreeRatio;
    ORIGINAL_MAX_HEAP_FREE_RATIO = originalMaxHeapFreeRatio;

    if (!isJava9OrLater()) {
      // For JDK 8, we cannot change MaxHeapFreeRatio programmatically at runtime.
      LOG.error("Dynamic off-heap sizing is not supported before JDK 9.");
    }

    TOTAL_MEMORY_SHARED = Runtime.getRuntime().maxMemory();

    LOG.info(
        "Initialized DynamicOffHeapSizingMemoryTarget with MAX_MEMORY_IN_BYTES = {}, "
            + "ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO = {}, "
            + "ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO = {}, "
            + "GC_MAX_HEAP_FREE_RATIO = {}.",
        TOTAL_MEMORY_SHARED,
        ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO,
        ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO,
        GC_MAX_HEAP_FREE_RATIO);
  }

  private static final AtomicLong USED_OFF_HEAP_BYTES = new AtomicLong();

  // Test only.
  private static final AtomicLong TOTAL_EXPLICIT_GC_COUNT = new AtomicLong(0L);

  private final String name = MemoryTargetUtil.toUniqueName("DynamicOffHeapSizing");
  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();

  private final MemoryTarget target;

  public DynamicOffHeapSizingMemoryTarget(MemoryTarget target) {
    this.target = target;
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }

    // Only JVM shrinking can reclaim space from the total JVM memory.
    // See https://github.com/apache/gluten/issues/9276.
    long totalHeapMemory = Runtime.getRuntime().totalMemory();
    long freeHeapMemory = Runtime.getRuntime().freeMemory();
    long usedOffHeapMemory = USED_OFF_HEAP_BYTES.get();

    // Adds the total JVM memory which is the actual memory the JVM occupied from the operating
    // system into the counter.
    if (exceedsMaxMemoryUsage(totalHeapMemory, usedOffHeapMemory, size, 1.0)) {
      // Perform GC synchronously to shrink memory; native tasks need to wait for this to obtain
      // more memory.
      synchronized (JVM_SHRINK_SYNC_OBJECT) {
        totalHeapMemory = Runtime.getRuntime().totalMemory();
        freeHeapMemory = Runtime.getRuntime().freeMemory();
        if (exceedsMaxMemoryUsage(totalHeapMemory, usedOffHeapMemory, size, 1.0)) {
          shrinkOnHeapMemory(totalHeapMemory, freeHeapMemory, false);
          totalHeapMemory = Runtime.getRuntime().totalMemory();
          freeHeapMemory = Runtime.getRuntime().freeMemory();
        }
      }
      // Check if we can allocate the requested size again after JVM shrinking(GC).
      if (exceedsMaxMemoryUsage(totalHeapMemory, usedOffHeapMemory, size, 1.0)) {
        LOG.warn(
            String.format(
                "Failing allocation as unified memory is OOM. "
                    + "Used Off-heap: %d, Used On-Heap: %d, "
                    + "Free On-heap: %d, Total On-heap: %d, "
                    + "Max On-heap: %d, Allocation: %d.",
                usedOffHeapMemory,
                totalHeapMemory - freeHeapMemory,
                freeHeapMemory,
                totalHeapMemory,
                TOTAL_MEMORY_SHARED,
                size));

        return 0;
      }
    } else if (shouldTriggerAsyncOnHeapMemoryShrink(
        totalHeapMemory, freeHeapMemory, usedOffHeapMemory, size)) {
      // Proactively trigger memory shrinking in the thread pool to prevent GC from blocking
      // native task execution.
      SparkThreadPoolUtil.triggerGCInThreadPool(
          new Runnable() {
            @Override
            public void run() {
              synchronized (JVM_SHRINK_SYNC_OBJECT) {
                long totalJvmMem = Runtime.getRuntime().totalMemory();
                long freeJvmMem = Runtime.getRuntime().freeMemory();
                if (shouldTriggerAsyncOnHeapMemoryShrink(
                    totalJvmMem, freeJvmMem, usedOffHeapMemory, size)) {
                  shrinkOnHeapMemory(totalJvmMem, freeJvmMem, true);
                }
              }
            }
          });
    }

    USED_OFF_HEAP_BYTES.addAndGet(size);
    recorder.inc(size);
    target.borrow(size);
    return size;
  }

  @Override
  public long repay(long size) {
    USED_OFF_HEAP_BYTES.addAndGet(-size);
    recorder.inc(-size);
    target.repay(size);
    return size;
  }

  @Override
  public long usedBytes() {
    return recorder.current();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MemoryUsageStats stats() {
    return recorder.toStats();
  }

  public MemoryTarget target() {
    return target;
  }

  public static boolean isJava9OrLater() {
    String spec = System.getProperty("java.specification.version", "1.8");
    // "1.8" → 8, "9" → 9, "11" → 11, etc.
    if (spec.startsWith("1.")) {
      spec = spec.substring(2);
    }
    try {
      return Integer.parseInt(spec) >= 9;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean canShrinkJVMMemory(long totalMemory, long freeMemory) {
    // Check if the JVM memory can be shrunk by a full GC.
    return freeMemory > totalMemory * GC_MAX_HEAP_FREE_RATIO;
  }

  public static long getTotalExplicitGCCount() {
    return TOTAL_EXPLICIT_GC_COUNT.get();
  }

  private static boolean exceedsMaxMemoryUsage(
      long totalOnHeapMemory, long totalOffHeapMemory, long requestedSize, double ratio) {
    return requestedSize + totalOffHeapMemory + totalOnHeapMemory >= TOTAL_MEMORY_SHARED * ratio;
  }

  private static boolean shouldTriggerAsyncOnHeapMemoryShrink(
      long totalOnHeapMemory, long freeOnHeapMemory, long totalOffHeapMemory, long requestedSize) {
    // If most of the memory has already been used, there's a high chance that memory will be fully
    // consumed. We proactively detect this situation to trigger JVM memory shrinking using the
    // following conditions.

    boolean exceedsMaxMemoryUsageRatio =
        exceedsMaxMemoryUsage(
            totalOnHeapMemory,
            totalOffHeapMemory,
            requestedSize,
            ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO);
    return exceedsMaxMemoryUsageRatio
        && canShrinkJVMMemory(totalOnHeapMemory, freeOnHeapMemory)
        // Limit GC frequency to prevent performance impact from excessive garbage collection.
        && totalOnHeapMemory > TOTAL_MEMORY_SHARED * ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO
        && (!ASYNC_GC_SUSPEND.get()
            || freeOnHeapMemory > totalOnHeapMemory * (ORIGINAL_MIN_HEAP_FREE_RATIO / 100.0));
  }

  private static long shrinkOnHeapMemoryInternal(
      long totalMemory, long freeMemory, boolean isAsyncGc) {
    long totalOffHeapMemory = USED_OFF_HEAP_BYTES.get();
    LOG.warn(
        String.format(
            "Starting %sfull gc to shrink JVM memory: "
                + "Total On-heap: %d, Free On-heap: %d, "
                + "Total Off-heap: %d, Used On-Heap: %d, Executor memory: %d.",
            isAsyncGc ? "async " : "",
            totalMemory,
            freeMemory,
            totalOffHeapMemory,
            (totalMemory - freeMemory),
            TOTAL_MEMORY_SHARED));
    // Explicitly calling System.gc() to trigger a full garbage collection.
    // This is necessary in this context to attempt to shrink JVM memory usage
    // when off-heap memory allocation is constrained. Use of System.gc() is
    // generally discouraged due to its unpredictable performance impact, but
    // here it is used as a last resort to prevent memory allocation failures.
    System.gc();
    long newTotalMemory = Runtime.getRuntime().totalMemory();
    long newFreeMemory = Runtime.getRuntime().freeMemory();
    int gcRetryTimes = 0;
    while (!isAsyncGc
        && gcRetryTimes < MAX_GC_RETRY_TIMES
        && newTotalMemory >= totalMemory
        && canShrinkJVMMemory(newTotalMemory, newFreeMemory)) {
      // System.gc() is just a suggestion; the JVM may ignore it or perform only a partial GC.
      // Here, the total memory is not reduced but the free memory ratio is bigger than the
      // GC_MAX_HEAP_FREE_RATIO. So we need to call System.gc() again to try to reduce the total
      // memory.
      // This is a workaround for the JVM's behavior of not reducing the total memory after GC.
      System.gc();
      newTotalMemory = Runtime.getRuntime().totalMemory();
      newFreeMemory = Runtime.getRuntime().freeMemory();
      gcRetryTimes++;
    }
    // If the memory usage is still high after GC, we need to suspend the async GC for a while.
    if (isAsyncGc) {
      ASYNC_GC_SUSPEND.set(
          totalMemory - newTotalMemory < totalMemory * (ORIGINAL_MIN_HEAP_FREE_RATIO / 100.0));
    }

    TOTAL_EXPLICIT_GC_COUNT.getAndAdd(1);
    LOG.warn(
        String.format(
            "Finished %sfull gc to shrink JVM memory: "
                + "Total On-heap: %d, Free On-heap: %d, "
                + "Total Off-heap: %d, Used On-Heap: %d, Executor memory: %d, "
                + "[GC Retry times: %d].",
            isAsyncGc ? "async " : "",
            newTotalMemory,
            newFreeMemory,
            totalOffHeapMemory,
            (newTotalMemory - newFreeMemory),
            TOTAL_MEMORY_SHARED,
            gcRetryTimes));
    return newTotalMemory;
  }

  public static long shrinkOnHeapMemory(long totalMemory, long freeMemory, boolean isAsyncGc) {
    boolean updateMaxHeapFreeRatio = false;
    Object hotSpotBean = null;
    String maxHeapFreeRatioName = "MaxHeapFreeRatio";
    String minHeapFreeRatioName = "MinHeapFreeRatio";
    int newValue = (int) (GC_MAX_HEAP_FREE_RATIO * 100);

    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      Class<?> beanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
      hotSpotBean =
          ManagementFactory.newPlatformMXBeanProxy(
              mbs, "com.sun.management:type=HotSpotDiagnostic", beanClass);

      Method setOption = beanClass.getMethod("setVMOption", String.class, String.class);
      if (newValue < ORIGINAL_MIN_HEAP_FREE_RATIO) {
        // Adjust the MinHeapFreeRatio to avoid the violation of the MaxHeapFreeRatio.
        setOption.invoke(hotSpotBean, minHeapFreeRatioName, Integer.toString(newValue));
      }
      if (newValue < ORIGINAL_MAX_HEAP_FREE_RATIO) {
        setOption.invoke(hotSpotBean, maxHeapFreeRatioName, Integer.toString(newValue));
        updateMaxHeapFreeRatio = true;
        LOG.info(
            String.format(
                "Updated VM flags: MaxHeapFreeRatio from %d to %d.",
                ORIGINAL_MAX_HEAP_FREE_RATIO, newValue));
      }
      return shrinkOnHeapMemoryInternal(totalMemory, freeMemory, isAsyncGc);
    } catch (Exception e) {
      LOG.warn(
          "Failed to update JVM heap free ratio via HotSpotDiagnosticMXBean: {}", e.toString());
      return totalMemory;
    } finally {
      // Reset the MaxHeapFreeRatio to the original values.
      if (hotSpotBean != null && updateMaxHeapFreeRatio) {
        try {
          Class<?> beanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
          Method setOption = beanClass.getMethod("setVMOption", String.class, String.class);
          setOption.invoke(
              hotSpotBean, maxHeapFreeRatioName, Integer.toString(ORIGINAL_MAX_HEAP_FREE_RATIO));
          LOG.info("Reverted VM flags back.");
        } catch (Exception ignore) {
          // best‐effort revert
        }
      }
    }
  }
}
