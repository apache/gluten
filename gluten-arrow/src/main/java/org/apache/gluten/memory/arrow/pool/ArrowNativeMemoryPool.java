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
package org.apache.gluten.memory.arrow.pool;

import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.spark.task.TaskResource;
import org.apache.spark.task.TaskResources;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowNativeMemoryPool implements TaskResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowNativeMemoryPool.class);

  private final NativeMemoryPool arrowPool;
  private final ArrowReservationListener listener;

  public ArrowNativeMemoryPool() {
    listener = new ArrowReservationListener(TaskResources.getSharedUsage());
    arrowPool = NativeMemoryPool.createListenable(listener);
  }

  public static NativeMemoryPool arrowPool(String name) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.");
    }
    String id = "ArrowNativeMemoryPool:" + name;
    return TaskResources.addResourceIfNotRegistered(id, () -> createArrowNativeMemoryPool(name))
        .getArrowPool();
  }

  private static ArrowNativeMemoryPool createArrowNativeMemoryPool(String name) {
    return new ArrowNativeMemoryPool();
  }

  @Override
  public void release() throws Exception {
    if (arrowPool.getBytesAllocated() != 0) {
      LOGGER.warn(
          String.format(
              "Arrow pool still reserved non-zero bytes, "
                  + "which may cause memory leak, size: %s. ",
              Utils.bytesToString(arrowPool.getBytesAllocated())));
    }
    arrowPool.close();
  }

  @Override
  public int priority() {
    return 10;
  }

  @Override
  public String resourceName() {
    return "arrow_mem";
  }

  public NativeMemoryPool getArrowPool() {
    return arrowPool;
  }
}
