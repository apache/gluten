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
package org.apache.gluten.velox;

import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlutenRowtimeInserterHelperTest {

  private static final RowType UPSTREAM_ROW_TYPE = RowType.of(new BigIntType());

  private static Transformation<RowData> newUpstream() {
    return new StubTransformation("upstream", InternalTypeInfo.of(UPSTREAM_ROW_TYPE));
  }

  private static OneInputTransformation<RowData, RowData> newNativeInserterTx(
      Transformation<RowData> upstream, int rowtimeIndex) {
    StreamRecordTimestampInserter op = new StreamRecordTimestampInserter(rowtimeIndex);
    return new OneInputTransformation<>(
        upstream, "native-inserter", op, upstream.getOutputType(), 1);
  }

  private static OneInputTransformation<RowData, RowData> newOtherOperatorTx(
      Transformation<RowData> upstream) {
    StreamMap<RowData, RowData> other = new StreamMap<>(new IdentityMapFunction());
    return new OneInputTransformation<>(upstream, "other-op", other, upstream.getOutputType(), 1);
  }

  @Test
  void testNoOpForNonOneInputTransformation() {
    Transformation<RowData> stub =
        new StubTransformation("stub", InternalTypeInfo.of(UPSTREAM_ROW_TYPE));
    assertSame(stub, GlutenRowtimeInserterHelper.processTransformation(stub, false));
    assertSame(stub, GlutenRowtimeInserterHelper.processTransformation(stub, true));
  }

  @Test
  void testNoOpForNonInserterOperator() {
    Transformation<RowData> upstream = newUpstream();
    OneInputTransformation<RowData, RowData> tx = newOtherOperatorTx(upstream);
    assertSame(tx, GlutenRowtimeInserterHelper.processTransformation(tx, false));
    assertSame(tx, GlutenRowtimeInserterHelper.processTransformation(tx, true));
  }

  @Test
  void testReplacesNativeInserterWhenTimestampRequired() {
    Transformation<RowData> upstream = newUpstream();
    OneInputTransformation<RowData, RowData> nativeTx = newNativeInserterTx(upstream, 0);

    Transformation<RowData> result =
        GlutenRowtimeInserterHelper.processTransformation(nativeTx, true);

    assertNotSame(nativeTx, result);
    assertTrue(result instanceof OneInputTransformation);
    @SuppressWarnings("unchecked")
    OneInputTransformation<RowData, RowData> out =
        (OneInputTransformation<RowData, RowData>) result;
    assertTrue(out.getOperatorFactory() instanceof SimpleOperatorFactory);
    Object op = ((SimpleOperatorFactory<?>) out.getOperatorFactory()).getOperator();
    assertTrue(op instanceof GlutenOneInputOperator);
    assertSame(upstream, out.getInputs().get(0));
    assertSame(1, out.getParallelism());
  }

  @Test
  void testRemovesNativeInserterWhenTimestampNotRequired() {
    Transformation<RowData> upstream = newUpstream();
    OneInputTransformation<RowData, RowData> nativeTx = newNativeInserterTx(upstream, 0);

    Transformation<RowData> result =
        GlutenRowtimeInserterHelper.processTransformation(nativeTx, false);

    assertSame(upstream, result);
  }

  private static final class StubTransformation extends Transformation<RowData> {
    StubTransformation(String name, InternalTypeInfo<RowData> typeInfo) {
      super(name, typeInfo, 1);
    }

    @Override
    public List<Transformation<?>> getInputs() {
      return Collections.emptyList();
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
      return Collections.emptyList();
    }
  }

  private static final class IdentityMapFunction implements MapFunction<RowData, RowData> {
    @Override
    public RowData map(RowData value) {
      return value;
    }
  }
}
