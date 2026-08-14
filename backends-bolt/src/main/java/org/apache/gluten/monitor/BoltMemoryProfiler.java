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
package org.apache.gluten.monitor;

/**
 * BoltMemoryProfiler is a JNI for controlling native memory profiler. Currently, it uses jemalloc
 * for memory profiling, so if you want to enable it, need to build gluten with
 * `--enable_jemalloc_stats=ON`.
 *
 * <p>Please set the following configurations by using the same lib jemalloc linked to Gluten native
 * lib.
 *
 * <ul>
 *   <li>spark.executorEnv.LD_PRELOAD=/path/to/libjemalloc.so
 *   <li>spark.executorEnv.MALLOC_CONF=prof:true,prof_prefix:/tmp/gluten_heap_perf
 * </ul>
 */
public class BoltMemoryProfiler {

  /** Starts the Bolt memory profiler. (jemalloc: prof.active=ture) */
  public static native void start();

  /** Dumps the current memory profile. (jemalloc: prof.dump) */
  public static native void dump();

  /** Stops the Bolt memory profiler. (jemalloc: prof.active=false) */
  public static native void stop();
}
