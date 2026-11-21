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
package org.apache.gluten.rexnode;

import org.apache.gluten.rexnode.functions.FromUnixtimeRexCallConverter;
import org.apache.gluten.rexnode.functions.RexCallConverter;
import org.apache.gluten.rexnode.functions.RexCallConverterFactory;
import org.apache.gluten.rexnode.functions.UnixTimestampRexCallConverter;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.VarCharType;

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FromUnixtimeUnixTimestampConverterTest {

  private static FlinkTypeFactory typeFactory;
  private static FlinkRexBuilder rexBuilder;
  private static RexConversionContext context;

  @BeforeAll
  public static void setUp() {
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);
    context = new RexConversionContext(List.of("c0", "c1"));
  }

  private RexNode intInputRef() {
    return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
  }

  private RexNode bigintInputRef() {
    return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
  }

  private RexNode varcharInputRef() {
    return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
  }

  private RexNode varcharLiteral(String value) {
    return rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
  }

  @Test
  public void fromUnixtimeIntOperandIsCastToBigint() {
    RexCall call =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.FROM_UNIXTIME, intInputRef());
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter).isInstanceOf(FromUnixtimeRexCallConverter.class);
    assertThat(converter.isSuitable(call, context).isOk()).isTrue();

    TypedExpr expr = converter.toTypedExpr(call, context);
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    CallTypedExpr callExpr = (CallTypedExpr) expr;
    assertThat(callExpr.getFunctionName()).isEqualTo("from_unixtime");
    assertThat(callExpr.getReturnType()).isInstanceOf(VarCharType.class);
    assertThat(callExpr.getInputs()).hasSize(1);
    assertThat(callExpr.getInputs().get(0)).isInstanceOf(CastTypedExpr.class);
    assertThat(callExpr.getInputs().get(0).getReturnType()).isInstanceOf(BigIntType.class);
  }

  @Test
  public void fromUnixtimeBigintOperandPassesThrough() {
    RexCall call =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.FROM_UNIXTIME, bigintInputRef());
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter.isSuitable(call, context).isOk()).isTrue();

    CallTypedExpr callExpr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(callExpr.getInputs().get(0)).isNotInstanceOf(CastTypedExpr.class);
    assertThat(callExpr.getInputs().get(0).getReturnType()).isInstanceOf(BigIntType.class);
  }

  @Test
  public void fromUnixtimeWithFormatKeepsFormatOperand() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                FlinkSqlOperatorTable.FROM_UNIXTIME,
                bigintInputRef(),
                varcharLiteral("yyyy-MM-dd"));
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter.isSuitable(call, context).isOk()).isTrue();

    CallTypedExpr callExpr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(callExpr.getInputs()).hasSize(2);
    assertThat(callExpr.getInputs().get(1).getReturnType()).isInstanceOf(VarCharType.class);
  }

  @Test
  public void fromUnixtimeRejectsNonIntegralFirstOperand() {
    RexCall call =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.FROM_UNIXTIME, varcharInputRef());
    // The factory throws when no converter is suitable (GFV has no fallback);
    // assert on the converter directly for the negative case.
    assertThat(new FromUnixtimeRexCallConverter().isSuitable(call, context).isOk()).isFalse();
  }

  @Test
  public void unixTimestampNoOperands() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                FlinkSqlOperatorTable.UNIX_TIMESTAMP, Collections.<RexNode>emptyList());
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter).isInstanceOf(UnixTimestampRexCallConverter.class);
    assertThat(converter.isSuitable(call, context).isOk()).isTrue();

    CallTypedExpr callExpr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(callExpr.getFunctionName()).isEqualTo("unix_timestamp");
    assertThat(callExpr.getReturnType()).isInstanceOf(BigIntType.class);
    assertThat(callExpr.getInputs()).isEmpty();
  }

  @Test
  public void unixTimestampStringAndFormatOperands() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                FlinkSqlOperatorTable.UNIX_TIMESTAMP,
                varcharInputRef(),
                varcharLiteral("yyyy-MM-dd"));
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter.isSuitable(call, context).isOk()).isTrue();

    CallTypedExpr callExpr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(callExpr.getInputs()).hasSize(2);
    assertThat(callExpr.getReturnType()).isInstanceOf(BigIntType.class);
  }

  @Test
  public void unixTimestampRejectsNonStringOperand() {
    RexCall call =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.UNIX_TIMESTAMP, bigintInputRef());
    assertThat(new UnixTimestampRexCallConverter().isSuitable(call, context).isOk()).isFalse();
  }
}
