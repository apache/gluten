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

import org.apache.gluten.memory.memtarget.spark.RegularMemoryConsumer;
import org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumer;

import org.apache.spark.memory.GlobalOffHeapMemoryTarget;

public interface MemoryTargetVisitor<T> {
  T visit(OverAcquire overAcquire);

  T visit(RegularMemoryConsumer regularMemoryConsumer);

  T visit(ThrowOnOomMemoryTarget throwOnOomMemoryTarget);

  T visit(TreeMemoryConsumer treeMemoryConsumer);

  T visit(TreeMemoryConsumer.Node node);

  T visit(LoggingMemoryTarget loggingMemoryTarget);

  T visit(NoopMemoryTarget noopMemoryTarget);

  T visit(DynamicOffHeapSizingMemoryTarget dynamicOffHeapSizingMemoryTarget);

  T visit(RetryOnOomMemoryTarget retryOnOomMemoryTarget);

  T visit(GlobalOffHeapMemoryTarget globalOffHeapMemoryTarget);

  T visit(WrappedGlobalOffHeapMemoryTarget wrappedGlobalOffHeapMemoryTarget);
}
