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

import org.apache.gluten.config.GlutenCoreConfig;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.memory.memtarget.TreeMemoryTarget;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.task.TaskResource;
import org.apache.spark.task.TaskResources;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import scala.Function0;

public final class TreeMemoryConsumers {
  private TreeMemoryConsumers() {}

  public static Factory factory(MemoryMode mode) {
    final Factory factory =
        TaskResources.addResourceIfNotRegistered(
            Factory.class.getName(),
            new Function0<Factory>() {
              @Override
              public Factory apply() {
                return new Factory(TaskResources.getLocalTaskContext().taskMemoryManager(), mode);
              }
            });
    final MemoryMode foundMode = factory.sparkConsumer.getMode();
    Preconditions.checkState(
        foundMode == mode,
        "An existing Spark memory consumer already exists but is of the different memory "
            + "mode: %s",
        foundMode);
    return factory;
  }

  public static class Factory implements TaskResource {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);

    private final TreeMemoryConsumer sparkConsumer;
    private final Map<Long, TreeMemoryTarget> roots = new ConcurrentHashMap<>();

    private Factory(TaskMemoryManager tmm, MemoryMode mode) {
      this.sparkConsumer = new TreeMemoryConsumer(tmm, mode);
    }

    private TreeMemoryTarget ofCapacity(long capacity) {
      return roots.computeIfAbsent(
          capacity,
          cap ->
              sparkConsumer.newChild(
                  String.format("Capacity[%s]", Utils.bytesToString(cap)),
                  cap,
                  Spillers.NOOP,
                  Collections.emptyMap()));
    }

    /**
     * This works as a legacy Spark memory consumer which grants as much as possible of memory
     * capacity to each task.
     */
    public TreeMemoryTarget legacyRoot() {
      return ofCapacity(TreeMemoryTarget.CAPACITY_UNLIMITED);
    }

    /**
     * A hub to provide memory target instances whose shared size (in the same task) is limited to
     * X, X = executor memory / task slots.
     *
     * <p>Using this to prevent OOMs if the delegated memory target could possibly hold large memory
     * blocks that are not spill-able.
     *
     * <p>See <a href="https://github.com/oap-project/gluten/issues/3030">GLUTEN-3030</a>
     */
    public TreeMemoryTarget isolatedRoot() {
      return ofCapacity(GlutenCoreConfig.get().conservativeTaskOffHeapMemorySize());
    }

    @Override
    public void release() throws Exception {
      if (sparkConsumer.usedBytes() != 0) {
        LOG.warn(
            "{} still used {} bytes when task is ending," + " this may cause memory leak",
            resourceName(),
            sparkConsumer.usedBytes());
      }
    }

    @Override
    public int priority() {
      return 5;
    }

    @Override
    public String resourceName() {
      return Factory.class.getSimpleName();
    }
  }
}
