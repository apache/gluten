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

import java.util.*;

public final class Spillers {
  private Spillers() {
    // enclose factory ctor
  }

  public static final Spiller NOOP =
      new Spiller() {
        @Override
        public long spill(MemoryTarget self, Phase phase, long size) {
          return 0;
        }
      };

  public static final Set<Spiller.Phase> PHASE_SET_ALL =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(Spiller.Phase.SHRINK, Spiller.Phase.SPILL)));

  public static Spiller withMinSpillSize(Spiller spiller, long minSize) {
    return new WithMinSpillSize(spiller, minSize);
  }

  public static AppendableSpillerList appendable() {
    return new AppendableSpillerList();
  }

  // Minimum spill target size should be larger than spark.gluten.memory.reservationBlockSize,
  // since any release action within size smaller than the block size may not have chance to
  // report back to the Java-side reservation listener.
  private static class WithMinSpillSize implements Spiller {
    private final Spiller delegated;
    private final long minSize;

    private WithMinSpillSize(Spiller delegated, long minSize) {
      this.delegated = delegated;
      this.minSize = minSize;
    }

    @Override
    public long spill(MemoryTarget self, Spiller.Phase phase, long size) {
      return delegated.spill(self, phase, Math.max(size, minSize));
    }
  }

  public static class AppendableSpillerList implements Spiller {
    private final List<Spiller> spillers = new ArrayList<>();

    private AppendableSpillerList() {}

    public void append(Spiller spiller) {
      spillers.add(spiller);
    }

    @Override
    public long spill(MemoryTarget self, Phase phase, final long size) {
      long remainingBytes = size;
      for (Spiller spiller : spillers) {
        if (remainingBytes <= 0) {
          break;
        }
        remainingBytes -= spiller.spill(self, phase, remainingBytes);
      }
      return size - remainingBytes;
    }
  }
}
