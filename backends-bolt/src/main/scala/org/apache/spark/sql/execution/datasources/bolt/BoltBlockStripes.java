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
package org.apache.spark.sql.execution.datasources.bolt;

import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.BlockStripe;
import org.apache.spark.sql.execution.datasources.BlockStripes;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class BoltBlockStripes extends BlockStripes {
  public BoltBlockStripes(BlockStripes bs) {
    super(bs.originBlockAddress,
        bs.blockAddresses, bs.headingRowIndice, bs.originBlockNumColumns,
        bs.headingRowBytes);
  }

  @Override
  public @NotNull Iterator<BlockStripe> iterator() {
    return new Iterator<BlockStripe>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < blockAddresses.length;
      }

      @Override
      public BlockStripe next() {
        final BlockStripe nextStripe = new BlockStripe() {
          private final long blockAddress = blockAddresses[index];
          private final byte[] headingRowByteArray = headingRowBytes[index];

          @Override
          public ColumnarBatch getColumnarBatch() {
            return ColumnarBatches.create(blockAddress);
          }

          @Override
          public InternalRow getHeadingRow() {
            UnsafeRow row = new UnsafeRow(originBlockNumColumns);
            row.pointTo(headingRowByteArray, headingRowByteArray.length);
            return row;
          }
        };
        index += 1;
        return nextStripe;
      }
    };
  }


  @Override
  public void release() {

  }
}

