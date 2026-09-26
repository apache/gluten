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
package org.apache.gluten.iterator;

import org.apache.gluten.exception.GlutenException;

import org.junit.Assert;
import org.junit.Test;

public class ClosableIteratorTranslateTest {

  private static class ThrowingIterator extends ClosableIterator<Object> {
    private final Exception toThrow;

    ThrowingIterator(Exception toThrow) {
      this.toThrow = toThrow;
    }

    @Override
    protected boolean hasNext0() throws Exception {
      throw toThrow;
    }

    @Override
    protected Object next0() throws Exception {
      throw toThrow;
    }

    @Override
    protected void close0() {}
  }

  @Test
  public void testGlutenExceptionPassesThroughUnwrapped() {
    final GlutenException inner = new GlutenException("inner");
    final ThrowingIterator iterator = new ThrowingIterator(inner);
    try {
      iterator.hasNext();
      Assert.fail("Expected the GlutenException to propagate");
    } catch (GlutenException e) {
      // The exact instance must surface, not a wrapper around it.
      Assert.assertSame(inner, e);
      Assert.assertNull(e.getCause());
    }
  }

  @Test
  public void testRawExceptionIsWrapped() {
    final ThrowingIterator iterator = new ThrowingIterator(new IllegalStateException("raw"));
    try {
      iterator.hasNext();
      Assert.fail("Expected a GlutenException wrapper");
    } catch (GlutenException e) {
      Assert.assertEquals("raw", e.getCause().getMessage());
    }
  }
}
