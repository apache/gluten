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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrintSinkFactoryTest {

  private static final String ROWDATA_PRINT_FUNCTION_CN =
      "org.apache.flink.connector.print.table.PrintTableSinkFactory$RowDataPrintFunction";

  @SuppressWarnings("unchecked")
  private static SinkFunction<RowData> newRowDataPrintFunction(String identifier, boolean isStdErr)
      throws Exception {
    Class<?> cls = Class.forName(ROWDATA_PRINT_FUNCTION_CN);
    Constructor<?> ctor =
        cls.getDeclaredConstructor(
            org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter.class,
            String.class,
            boolean.class);
    ctor.setAccessible(true);
    return (SinkFunction<RowData>) ctor.newInstance(null, identifier, isStdErr);
  }

  private static LegacySinkTransformation<RowData> buildSinkTransformation(
      SinkFunction<RowData> userFunction) {
    SinkOperator sinkOp = new SinkOperator(userFunction, -1);
    SimpleOperatorFactory<Object> factory = SimpleOperatorFactory.of(sinkOp);
    Transformation<RowData> input = new StubTransformation();
    return new LegacySinkTransformation<>(input, "print-sink", factory, 1);
  }

  private static final class StubTransformation extends Transformation<RowData> {
    StubTransformation() {
      super("stub", InternalTypeInfo.of(RowType.of(new IntType())), 1);
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

  private static final class OtherSinkFunction extends RichSinkFunction<RowData> {}

  @Test
  void testMatchAcceptsRowDataPrintFunction() throws Exception {
    PrintSinkFactory factory = new PrintSinkFactory();
    assertTrue(factory.match(buildSinkTransformation(newRowDataPrintFunction("foo", false))));
  }

  @Test
  void testMatchRejectsNonPrintSinkFunction() {
    PrintSinkFactory factory = new PrintSinkFactory();
    assertFalse(factory.match(buildSinkTransformation(new OtherSinkFunction())));
  }

  @Test
  void testMatchRejectsNonLegacySinkTransformation() {
    PrintSinkFactory factory = new PrintSinkFactory();
    assertFalse(factory.match(new StubTransformation()));
  }

  @Test
  void testExtractPrintOptionsReadsIdentifierAndStderr() throws Exception {
    LegacySinkTransformation<RowData> tx =
        buildSinkTransformation(newRowDataPrintFunction("foo", true));
    PrintSinkFactory.PrintOptions opts = PrintSinkFactory.extractPrintOptions(tx);
    assertEquals("foo", opts.getPrintIdentifier());
    assertTrue(opts.isStdErr());
  }

  @Test
  void testExtractPrintOptionsDefaultsWhenUnset() throws Exception {
    LegacySinkTransformation<RowData> tx =
        buildSinkTransformation(newRowDataPrintFunction(null, false));
    PrintSinkFactory.PrintOptions opts = PrintSinkFactory.extractPrintOptions(tx);
    assertEquals("", opts.getPrintIdentifier());
    assertFalse(opts.isStdErr());
  }
}
