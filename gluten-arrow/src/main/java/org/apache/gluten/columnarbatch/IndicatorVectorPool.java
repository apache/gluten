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
package org.apache.gluten.columnarbatch;

import org.apache.spark.task.TaskResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndicatorVectorPool implements TaskResource {
  private static final Logger LOG = LoggerFactory.getLogger(IndicatorVectorPool.class);
  // A pool for all alive indicator vectors. The reason we adopt the pool
  // is, we don't want one native columnar batch (which is located via the
  // long int handle through JNI bridge) to be owned by more than one IndicatorVector
  // instance so release method of the native columnar batch could be guaranteed
  // to be called and only called once.
  private final Map<Long, IndicatorVector> uniqueInstances = new ConcurrentHashMap<>();

  IndicatorVectorPool() {}

  @Override
  public void release() throws Exception {
    if (!uniqueInstances.isEmpty()) {
      LOG.warn(
          "There are still unreleased native columnar batches during ending the task."
              + " Will close them automatically however the batches should be better released"
              + " manually to minimize memory pressure.");
    }
  }

  IndicatorVector obtain(long handle) {
    return uniqueInstances.computeIfAbsent(handle, h -> new IndicatorVector(this, handle));
  }

  void remove(long handle) {
    if (uniqueInstances.remove(handle) == null) {
      throw new IllegalStateException("Indicator vector not found in pool, this should not happen");
    }
  }

  @Override
  public int priority() {
    return 20;
  }

  @Override
  public String resourceName() {
    return IndicatorVectorPool.class.getName();
  }
}
