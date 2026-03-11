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

import org.apache.gluten.memory.MemoryUsageStatsBuilder;
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.*;
import org.apache.gluten.proto.MemoryUsageStats;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This is a Spark memory consumer and at the same time a factory to create sub-targets that share
 * one fixed memory capacity.
 *
 * <p>Once full (size > capacity), spillers will be called by the consumer with order. If failed to
 * free out enough memory, throw OOM to caller.
 *
 * <p>Spark's memory manager could either trigger spilling on the children spillers since this was
 * registered as a Spark memory consumer.
 *
 * <p>Typically used by utility class {@link
 * org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumers}.
 */
public class TreeMemoryConsumer extends MemoryConsumer implements TreeMemoryTarget {

  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();
  private final Map<String, TreeMemoryTarget> children = new ConcurrentHashMap<>();
  private final String name = MemoryTargetUtil.toUniqueName("Gluten.Tree");

  TreeMemoryConsumer(TaskMemoryManager taskMemoryManager, MemoryMode mode) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), mode);
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      // or Spark complains about the zero size by throwing an error
      return 0;
    }
    long acquired = acquireMemory(size);
    recorder.inc(acquired);
    return acquired;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    long toFree = Math.min(getUsed(), size);
    freeMemory(toFree);
    Preconditions.checkArgument(getUsed() >= 0);
    recorder.inc(-toFree);
    return toFree;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long usedBytes() {
    return getUsed();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public MemoryUsageStats stats() {
    Map<String, MemoryUsageStats> childrenStats =
        children.values().stream()
            .collect(Collectors.toMap(TreeMemoryTarget::name, TreeMemoryTarget::stats));
    return recorder.toStats(childrenStats);
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    // subject to the regular Spark spill calls
    return TreeMemoryTargets.spillTree(this, size);
  }

  @Override
  public TreeMemoryTarget newChild(
      String name,
      long capacity,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    final TreeMemoryTarget child = new Node(this, name, capacity, spiller, virtualChildren);
    TreeMemoryTarget existing = children.putIfAbsent(child.name(), child);
    if (existing != null) {
      throw new IllegalArgumentException("Child already registered: " + child.name());
    }
    return child;
  }

  @Override
  public Map<String, TreeMemoryTarget> children() {
    return Collections.unmodifiableMap(children);
  }

  @Override
  public TreeMemoryTarget parent() {
    // we are root
    throw new IllegalStateException(
        "Unreachable code org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumer.parent");
  }

  @Override
  public Spiller getNodeSpiller() {
    // root doesn't spill
    return Spillers.NOOP;
  }

  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }

  public static class Node implements TreeMemoryTarget, KnownNameAndStats {
    private final Map<String, Node> children = new ConcurrentHashMap<>();
    private final TreeMemoryTarget parent;
    private final String name;
    private final long capacity;
    private final Spiller spiller;
    private final Map<String, MemoryUsageStatsBuilder> virtualChildren;
    private final SimpleMemoryUsageRecorder selfRecorder = new SimpleMemoryUsageRecorder();

    private Node(
        TreeMemoryTarget parent,
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      this.parent = parent;
      this.capacity = capacity;
      final String uniqueName = MemoryTargetUtil.toUniqueName(name);
      if (capacity == TreeMemoryTarget.CAPACITY_UNLIMITED) {
        this.name = uniqueName;
      } else {
        this.name = String.format("%s, %s", uniqueName, Utils.bytesToString(capacity));
      }
      this.spiller = spiller;
      this.virtualChildren = virtualChildren;
    }

    @Override
    public long borrow(long size) {
      if (size == 0) {
        return 0;
      }
      ensureFreeCapacity(size);
      return borrow0(Math.min(freeBytes(), size));
    }

    private long freeBytes() {
      return capacity - usedBytes();
    }

    private long borrow0(long size) {
      long granted = parent.borrow(size);
      selfRecorder.inc(granted);
      return granted;
    }

    @Override
    public Spiller getNodeSpiller() {
      return spiller;
    }

    private boolean ensureFreeCapacity(long bytesNeeded) {
      while (true) { // FIXME should we add retry limit?
        long freeBytes = freeBytes();
        Preconditions.checkState(freeBytes >= 0);
        if (freeBytes >= bytesNeeded) {
          // free bytes fit requirement
          return true;
        }
        // spill
        long bytesToSpill = bytesNeeded - freeBytes;
        long spilledBytes = TreeMemoryTargets.spillTree(this, bytesToSpill);
        Preconditions.checkState(spilledBytes >= 0);
        if (spilledBytes == 0) {
          // OOM
          return false;
        }
      }
    }

    @Override
    public long repay(long size) {
      if (size == 0) {
        return 0;
      }
      long toFree = Math.min(usedBytes(), size);
      long freed = parent.repay(toFree);
      selfRecorder.inc(-freed);
      return freed;
    }

    @Override
    public long usedBytes() {
      return selfRecorder.current();
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
      final Map<String, MemoryUsageStats> childrenStats =
          children.values().stream()
              .collect(Collectors.toMap(TreeMemoryTarget::name, TreeMemoryTarget::stats));

      // add virtual children
      for (Map.Entry<String, MemoryUsageStatsBuilder> entry : virtualChildren.entrySet()) {
        if (childrenStats.containsKey(entry.getKey())) {
          throw new IllegalArgumentException("Child stats already exists: " + entry.getKey());
        }
        childrenStats.put(entry.getKey(), entry.getValue().toStats());
      }
      return selfRecorder.toStats(childrenStats);
    }

    @Override
    public TreeMemoryTarget newChild(
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      final Node child =
          new Node(this, name, Math.min(this.capacity, capacity), spiller, virtualChildren);
      Node existing = children.putIfAbsent(child.name(), child);
      if (existing != null) {
        throw new IllegalArgumentException("Child already registered: " + child.name());
      }
      return child;
    }

    @Override
    public Map<String, TreeMemoryTarget> children() {
      return Collections.unmodifiableMap(children);
    }

    @Override
    public TreeMemoryTarget parent() {
      return parent;
    }
  }
}
