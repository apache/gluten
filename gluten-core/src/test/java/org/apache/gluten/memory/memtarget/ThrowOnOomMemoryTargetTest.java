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

import org.junit.Assert;
import org.junit.Test;

public class ThrowOnOomMemoryTargetTest {

  @Test
  public void testInterruptedBorrowSurfacesInterruptionNotOom() {
    final MemoryTarget neverGrants =
        new NoopMemoryTarget() {
          @Override
          public long borrow(long size) {
            return 0L;
          }
        };
    final ThrowOnOomMemoryTarget target = new ThrowOnOomMemoryTarget(neverGrants);

    // A pre-set interrupt flag makes the first Thread.sleep in the retry loop
    // throw InterruptedException immediately - the situation of a task killed
    // while waiting for memory.
    Thread.currentThread().interrupt();
    try {
      target.borrow(1024L);
      Assert.fail("Expected a RuntimeException carrying the interruption");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof InterruptedException);
    } finally {
      // The handler must have restored the interrupt status; consuming it here
      // also keeps the flag from leaking into other tests.
      Assert.assertTrue(Thread.interrupted());
    }
  }
}
