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

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicLong;

public final class MemoryTargetUtil {
  private MemoryTargetUtil() {}

  // One sequence shared by every name. The suffix only has to make the name unique, since
  // TreeMemoryConsumer#newChild keys siblings by name and rejects a collision, and a shared
  // sequence does that while holding no per-name state. Callers pass names that are not drawn from
  // a fixed set, so a counter per name accumulated an entry per distinct name for the lifetime of
  // the JVM.
  private static final AtomicLong SEQUENCE = new AtomicLong(0L);

  public static String toUniqueName(String name) {
    return name + "." + SEQUENCE.getAndIncrement();
  }

  // Lets a test assert that naming holds no state beyond this counter. Not for production use;
  // @VisibleForTesting flags any accidental same-package caller.
  @VisibleForTesting
  static long sequenceForTesting() {
    return SEQUENCE.get();
  }
}
