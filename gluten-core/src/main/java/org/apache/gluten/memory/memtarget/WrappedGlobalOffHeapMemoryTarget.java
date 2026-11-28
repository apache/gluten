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
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.proto.MemoryUsageStats;

public final class WrappedGlobalOffHeapMemoryTarget implements MemoryTarget, KnownNameAndStats {
  private final String name = MemoryTargetUtil.toUniqueName("WrappedGlobalOffHeap");
  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();
  private final MemoryTarget target;
  private DynamicOffHeapSizingMemoryTarget dynamicTarget;

  WrappedGlobalOffHeapMemoryTarget(MemoryTarget target) {
    this.target = target;
  }

  @Override
  public synchronized long borrow(long size) {
    final long granted;
    if (GlutenCoreConfig.isDynamicOffHeapSizingEnabled()) {
      if (dynamicTarget == null) {
        dynamicTarget = new DynamicOffHeapSizingMemoryTarget(target);
      }
      granted = dynamicTarget.borrow(size);
    } else {
      granted = target.borrow(size);
    }
    recorder.inc(granted);
    return granted;
  }

  @Override
  public synchronized long repay(long size) {
    final long booked = dynamicTarget == null ? 0 : Math.min(size, dynamicTarget.usedBytes());
    final long unbooked = booked == 0 ? 0 : dynamicTarget.repay(booked);
    final long freed = unbooked + target.repay(size - booked);
    recorder.inc(-freed);
    return freed;
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
}
