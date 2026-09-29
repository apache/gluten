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

import com.google.common.base.Preconditions;

/**
 * Wraps {@code target} and, on each successful {@code borrow}, performs a preemptive spill probe
 * against {@code overTarget}.
 *
 * <p>The probe asks {@code overTarget} to borrow {@code ratio * target.usedBytes()} bytes, then
 * immediately releases them. Because {@code overTarget} shares Spark's memory pool with {@code
 * target}, the borrow call forces Spark to spill other memory consumers if the pool is tight, and
 * the subsequent repay frees the reservation without holding any actual memory. The net effect is
 * that subsequent allocations under {@code target} start with the pool in a shrunk state and are
 * less likely to hit an OOM that {@code target} itself cannot spill out of (for example, a two-step
 * reservation where step A is spillable but step B is not).
 *
 * <p>Historically the over-acquired memory was held for the lifetime of the reservation as a backup
 * for OOM; that shape was changed in <a
 * href="https://github.com/apache/gluten/pull/8247">#8247</a>, which added the {@code granted >=
 * size} gate and moved the {@code overTarget.repay} to run right after {@code overTarget.borrow}.
 * The class no longer holds any over-acquired bytes.
 */
public class OverAcquire implements MemoryTarget {

  // The underlying target.
  private final MemoryTarget target;

  // Probes Spark's memory pool by borrowing / immediately repaying; see the class Javadoc.
  private final MemoryTarget overTarget;

  // Fraction of target.usedBytes() used as the probe size on each successful borrow. When set to
  // 0, no probe runs and MemoryTargets#overAcquire returns target unwrapped.
  private final double ratio;

  OverAcquire(MemoryTarget target, MemoryTarget overTarget, double ratio) {
    Preconditions.checkArgument(ratio >= 0.0D);
    this.overTarget = overTarget;
    this.target = target;
    this.ratio = ratio;
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }
    Preconditions.checkState(overTarget.usedBytes() == 0);
    long granted = target.borrow(size);
    if (granted >= size) {
      long majorSize = target.usedBytes();
      long overSize = (long) (ratio * majorSize);
      long overAcquired = overTarget.borrow(overSize);
      Preconditions.checkState(overAcquired == overTarget.usedBytes());
      long releasedOverSize = overTarget.repay(overAcquired);
      Preconditions.checkState(releasedOverSize == overAcquired);
      Preconditions.checkState(overTarget.usedBytes() == 0);
    }
    return granted;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes() + overTarget.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public MemoryTarget getTarget() {
    return target;
  }
}
