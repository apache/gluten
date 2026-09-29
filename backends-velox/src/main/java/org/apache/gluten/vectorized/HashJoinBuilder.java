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
package org.apache.gluten.vectorized;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

public class HashJoinBuilder implements RuntimeAware {
  private final Runtime runtime;

  private HashJoinBuilder(Runtime runtime) {
    this.runtime = runtime;
  }

  public static HashJoinBuilder create(Runtime runtime) {
    return new HashJoinBuilder(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  public static native void clearHashTable(String cacheKey, long hashTableData);

  public static native long cloneHashTable(String cacheKey, long hashTableData);

  public static native long deserializeHashTableDirect(
      String cacheKey, long address, int size, boolean ignoreNullKeys, boolean joinHasNullKeys);

  public static native boolean getHashTableIgnoreNullKeys(long hashTableHandle);

  public static native boolean getHashTableJoinHasNullKeys(long hashTableHandle);

  public static native long getHashTableBloomFilterBlocksByteSize(long hashTableHandle);

  public static native long getHashTableMemoryUsage(long hashTableHandle);

  public static native long serializedHashTableSizeDirect(long hashTableHandle);

  public static native void serializeHashTableDirect(long hashTableHandle, long address, long size);

  public native long nativeBuild(
      String buildHashTableId,
      long[] batchHandlers,
      String[] joinKeys,
      String[] filterBuildColumns,
      boolean filterPropagatesNulls,
      int joinType,
      boolean hasMixedFiltCondition,
      boolean isExistenceJoin,
      byte[] namedStruct,
      boolean isNullAwareAntiJoin,
      long bloomFilterPushdownSize,
      int broadcastHashTableBuildThreads);
}
