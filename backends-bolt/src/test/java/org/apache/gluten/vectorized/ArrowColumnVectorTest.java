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

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.task.TaskResources$;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import scala.collection.JavaConverters;

public class ArrowColumnVectorTest {

  @Test
  public void testWriteByArrowWriter() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final Decimal decimal = new Decimal();
          decimal.set(234, 20, 1);
          try (ColumnarBatch batch =
              ArrowColumnVectors.fromRows(
                  StructType.fromDDL("a decimal(20, 1)"),
                  JavaConverters.asScalaIterator(
                      Collections.<InternalRow>singletonList(
                              new GenericInternalRow(new Object[] {decimal}))
                          .iterator()),
                  ArrowBufferAllocators.contextInstance())) {
            Assert.assertEquals(decimal, batch.column(0).getDecimal(0, 20, 1));
          }
          return null;
        });
  }
}
