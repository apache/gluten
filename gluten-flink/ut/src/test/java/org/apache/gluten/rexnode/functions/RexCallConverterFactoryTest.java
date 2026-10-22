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
package org.apache.gluten.rexnode.functions;

import org.apache.gluten.rexnode.RexConversionContext;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class RexCallConverterFactoryTest {

  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;
  private static RexConversionContext context;

  @BeforeAll
  public static void setUp() {
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);
    context = new RexConversionContext(Collections.emptyList());
  }

  /**
   * A one-operand "-" is the Flink unary minus; it must resolve to the negate converter and emit a
   * same-typed negate call.
   */
  @Test
  public void testUnaryMinusResolvesWithSingleOperand() {
    SqlFunction minus =
        new SqlFunction(
            "-",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.NUMERIC,
            SqlFunctionCategory.NUMERIC);
    RexNode operand =
        rexBuilder.makeLiteral(7, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexCall call = (RexCall) rexBuilder.makeCall(minus, operand);
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter).isInstanceOf(UnaryMinusRexCallConverter.class);
    TypedExpr expr = converter.toTypedExpr(call, context);
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("negate");
    assertThat(expr.getInputs()).hasSize(1);
  }

  /** A two-operand "-" over decimals still resolves to the decimal arithmetic converter. */
  @Test
  public void testBinaryDecimalMinusResolvesToDecimalConverter() {
    SqlFunction minus =
        new SqlFunction(
            "-",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            null,
            OperandTypes.NUMERIC,
            SqlFunctionCategory.NUMERIC);
    RexNode left =
        rexBuilder.makeExactLiteral(
            new BigDecimal("12.34"), typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2));
    RexNode right =
        rexBuilder.makeExactLiteral(
            new BigDecimal("1.00"), typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2));
    RexCall call = (RexCall) rexBuilder.makeCall(minus, left, right);
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter).isInstanceOf(DecimalArithmeticOperatorRexCallConverters.class);
  }

  /**
   * ROUND over a decimal narrows the scale by the (value-dependent) second argument, which a velox
   * signature cannot express; the converter must emit a same-typed round call wrapped in a cast to
   * Flink's inferred result type.
   */
  @Test
  public void testRoundDecimalEmitsPostCast() {
    RexNode operand =
        rexBuilder.makeExactLiteral(
            new BigDecimal("55.67"), typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2));
    RexCall call = (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.ROUND, operand);
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter).isInstanceOf(DecimalPostCastRexCallConverter.class);
    TypedExpr expr = converter.toTypedExpr(call, context);
    assertThat(expr).isInstanceOf(CastTypedExpr.class);
    TypedExpr input = expr.getInputs().get(0);
    assertThat(input).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) input).getFunctionName()).isEqualTo("round");
    // The inner call stays same-typed (decimal in, decimal out)...
    assertThat(input.getReturnType())
        .isInstanceOf(io.github.zhztheplayer.velox4j.type.DecimalType.class);
    // ...and the cast lands on Flink's round type (decimal with narrowed scale).
    assertThat(expr.getReturnType())
        .isInstanceOf(io.github.zhztheplayer.velox4j.type.DecimalType.class);
    assertThat(expr.getReturnType()).isNotEqualTo(input.getReturnType());
  }

  /** Simple name-mapped entries resolve to the default converter. */
  @Test
  public void testFloorResolvesToDefaultConverter() {
    RexNode operand =
        rexBuilder.makeLiteral(1.5, typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
    RexLiteral lit = (RexLiteral) operand;
    RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, lit);
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    assertThat(converter).isInstanceOf(DefaultRexCallConverter.class);
    TypedExpr expr = converter.toTypedExpr(call, context);
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("floor");
  }
}
