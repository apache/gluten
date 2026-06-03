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
package org.apache.spark.sql.execution

import org.apache.spark.sql.types.{DataType, StructType}

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}

import java.nio.charset.StandardCharsets

/**
 * Process-local memoizer for `StructType <-> JSON` codec on the cached-batch hot path. Best-effort
 * Caffeine LRU; eviction recomputes via the same pure codec, so misses are indistinguishable from
 * the no-cache baseline. Thread-safety via Caffeine `get(key, mappingFunction)`.
 */
final private[execution] class SchemaJsonInternCache {
  import SchemaJsonInternCache._

  private val encodeCache: Cache[StructType, Array[Byte]] =
    Caffeine.newBuilder.maximumSize(CAP).build[StructType, Array[Byte]]()

  private val decodeCache: Cache[String, StructType] =
    Caffeine.newBuilder.maximumSize(CAP).build[String, StructType]()

  /** Returns the canonical UTF-8 JSON byte form of `schema`. */
  def encodeBytes(schema: StructType): Array[Byte] =
    encodeCache.get(schema, k => k.json.getBytes(StandardCharsets.UTF_8))

  /** Returns the canonical [[StructType]] parsed from `bytes` (UTF-8 JSON). */
  def decodeStructType(bytes: Array[Byte]): StructType = {
    val key = new String(bytes, StandardCharsets.UTF_8)
    decodeCache.get(key, k => DataType.fromJson(k).asInstanceOf[StructType])
  }
}

private[execution] object SchemaJsonInternCache {
  // 256 entries: <= ~8.5 MB retained even at 1000-field schemas (~33 KB JSON each). Verified by
  // Section C working-set sweep of the FU-D7 bench harness; revisit if C1/C2 gates fail.
  private val CAP = 256L
}
