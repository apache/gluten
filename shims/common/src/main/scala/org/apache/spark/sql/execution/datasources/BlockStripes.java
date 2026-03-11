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
package org.apache.spark.sql.execution.datasources;

import java.util.Iterator;

// FIXME: The abstraction is broken: VL / CH don't rely on the same binary layout of
//  this class.
public class BlockStripes implements Iterable<BlockStripe> {
    public long originBlockAddress;
    public long[] blockAddresses;
    public int[] headingRowIndice; // Only used by CH backend.
    public int originBlockNumColumns;
    public byte[][] headingRowBytes; // Only used by Velox backend.

    public BlockStripes(
            long originBlockAddress,
            long[] blockAddresses,
            int[] headingRowIndice,
            int originBlockNumColumns) {
        this.originBlockAddress = originBlockAddress;
        this.blockAddresses = blockAddresses;
        this.headingRowIndice = headingRowIndice;
        this.originBlockNumColumns = originBlockNumColumns;
    }

    public BlockStripes(
        long originBlockAddress,
        long[] blockAddresses,
        int[] headingRowIndice,
        int originBlockNumColumns,
        byte[][] headingRowBytes) {
        this.originBlockAddress = originBlockAddress;
        this.blockAddresses = blockAddresses;
        this.headingRowIndice = headingRowIndice;
        this.originBlockNumColumns = originBlockNumColumns;
        this.headingRowBytes = headingRowBytes;
    }

    public void release() {
        throw new UnsupportedOperationException("subclass of BlockStripe should implement this");
    }

    @Override
    public Iterator<BlockStripe> iterator() {
        throw new UnsupportedOperationException("subclass of BlockStripe should implement this");
    }
}

