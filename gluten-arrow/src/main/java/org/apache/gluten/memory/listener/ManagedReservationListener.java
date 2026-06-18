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
package org.apache.gluten.memory.listener;

import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.MemoryTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reserve Spark managed memory. */
public class ManagedReservationListener implements ReservationListener {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedReservationListener.class);

  private final MemoryTarget target;
  // Metrics shared by task.
  private final SimpleMemoryUsageRecorder sharedUsage;
  // Lock shared by task. Using a common lock avoids ABBA deadlock
  // when multiple listeners created under the same TMM.
  // See: https://github.com/apache/gluten/issues/6622
  private final Object sharedLock;

  public ManagedReservationListener(
      MemoryTarget target, SimpleMemoryUsageRecorder sharedUsage, Object sharedLock) {
    this.target = target;
    this.sharedUsage = sharedUsage;
    this.sharedLock = sharedLock;
  }

  @Override
  public long reserve(long size) {
    synchronized (sharedLock) {
      try {
        long granted = target.borrow(size);
        sharedUsage.inc(granted);
        return granted;
      } catch (Exception e) {
        LOG.warn("Error reserving memory from target", e);
        throw e;
      }
    }
  }

  @Override
  public long unreserve(long size) {
    synchronized (sharedLock) {
      try {
        long freed = target.repay(size);
        sharedUsage.inc(-freed);
        return freed;
      } catch (Exception e) {
        LOG.warn("Error unreserving memory from target", e);
        throw e;
      }
    }
  }

  @Override
  public long getUsedBytes() {
    return target.usedBytes();
  }
}
