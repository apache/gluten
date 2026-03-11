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
package org.apache.spark.sql.execution.datasources.velox;

import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.BlockStripe;
import org.apache.spark.sql.execution.datasources.BlockStripes;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class VeloxBlockStripes extends BlockStripes {
  private final BlockStripe[] blockStripes;

  public VeloxBlockStripes(BlockStripes bs) {
    super(bs.originBlockAddress,
        bs.blockAddresses, bs.headingRowIndice, bs.originBlockNumColumns,
        bs.headingRowBytes);
    blockStripes = new BlockStripe[blockAddresses.length];
    for (int i = 0; i < blockStripes.length; i++) {
      final long blockAddress = blockAddresses[i];
      final byte[] headingRowByteArray = headingRowBytes[i];
      blockStripes[i] = new BlockStripe() {
        private final ColumnarBatch batch = ColumnarBatches.create(blockAddress);
        private final UnsafeRow headingRow = new UnsafeRow(originBlockNumColumns);
        {
          headingRow.pointTo(headingRowByteArray, headingRowByteArray.length);
        }

        @Override
        public ColumnarBatch getColumnarBatch() {
          return batch;
        }

        @Override
        public InternalRow getHeadingRow() {
          return headingRow;
        }
      };
    }
  }

  @Override
  public Iterator<BlockStripe> iterator() {
    return new Iterator<BlockStripe>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < blockAddresses.length;
      }

      @Override
      public BlockStripe next() {
        final BlockStripe nextStripe = blockStripes[index];
        index++;
        return nextStripe;
      }
    };
  }


  @Override
  public void release() {
    // Do nothing. We rely on the caller to call #close API on columnar batches returned to them.
  }
}
