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
package org.apache.gluten.memory;

/** JNI down-calls for governing the Velox memCache capacity. Handle-free (VeloxBackend::get()). */
public class VeloxCacheJniWrapper {
  private VeloxCacheJniWrapper() {}

  /**
   * Moves the cache capacity from {@code from} to {@code to} and returns the capacity in effect
   * afterwards.
   *
   * <p>Both are absolute values rather than a delta so that the caller needs no baseline from the
   * native side: what Spark has lent us is the authoritative record of what the cache occupies.
   * {@code from} is that reservation, and passing it is what lets the native side tell a rise from
   * a fall and check that the reservation still covers the capacity.
   *
   * <p>Raising takes effect as asked, so the caller must have borrowed the memory from Spark
   * beforehand. Lowering evicts first and narrows the capacity second, but the result stays above
   * {@code to} when entries are pinned and could not be evicted; the caller repays the difference
   * between what it had reserved and the returned value, which is exactly the memory given up.
   */
  public static native long setCacheCapacity(long from, long to);
}
