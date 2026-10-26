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

import org.apache.gluten.rexnode.functions.RexCallConverter;
import org.apache.gluten.rexnode.functions.RexCallConverterFactory;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.IntervalDayTimeType;
import io.github.zhztheplayer.velox4j.type.IntervalYearMonthType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IntervalArithmeticConverterTest {

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

  private RexNode tsInputRef() {
    return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), 0);
  }

  private RexNode dateInputRef() {
    return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0);
  }

  private RexNode bigintInputRef() {
    return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
  }

  private RexNode hourInterval(long millis) {
    SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.HOUR, null, SqlParserPos.ZERO);
    return rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(millis), qualifier);
  }

  private RexNode monthInterval(int months) {
    SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO);
    return rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(months), qualifier);
  }

  @Test
  public void logicalTypeConverterMapsIntervals() {
    org.apache.flink.table.types.logical.DayTimeIntervalType dayTime =
        new org.apache.flink.table.types.logical.DayTimeIntervalType(
            org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution
                .DAY_TO_SECOND,
            2,
            6);
    org.apache.flink.table.types.logical.YearMonthIntervalType yearMonth =
        new org.apache.flink.table.types.logical.YearMonthIntervalType(
            org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution.YEAR, 2);
    assertThat(org.apache.gluten.util.LogicalTypeConverter.toVLType(dayTime))
        .isInstanceOf(IntervalDayTimeType.class);
    assertThat(org.apache.gluten.util.LogicalTypeConverter.toVLType(yearMonth))
        .isInstanceOf(IntervalYearMonthType.class);
  }

  @Test
  public void toVariantReadsIntervalLiterals() {
    RexLiteral hour = (RexLiteral) hourInterval(3600000);
    Variant variant = RexNodeConverter.toVariant(hour);
    assertThat(variant).isInstanceOf(BigIntValue.class);
    assertThat(((BigIntValue) variant).getValue()).isEqualTo(3600000L);
  }

  @Test
  public void timestampPlusIntervalDayTime() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
                tsInputRef(),
                hourInterval(3600000));
    assertThat(call.getOperator().getName()).isEqualTo("+");
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    CallTypedExpr expr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(expr.getFunctionName()).isEqualTo("plus");
    assertThat(expr.getInputs()).hasSize(2);
    assertThat(expr.getInputs().get(1).getReturnType()).isInstanceOf(IntervalDayTimeType.class);
  }

  @Test
  public void timestampPlusIntervalYearMonth() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
                tsInputRef(),
                monthInterval(1));
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    CallTypedExpr expr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(expr.getFunctionName()).isEqualTo("plus");
    assertThat(expr.getInputs().get(1).getReturnType()).isInstanceOf(IntervalYearMonthType.class);
  }

  @Test
  public void intervalOnLeftPlusIsFlipped() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
                hourInterval(3600000),
                tsInputRef());
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    CallTypedExpr expr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(expr.getFunctionName()).isEqualTo("plus");
    // Interval flipped to the right: velox only registers (datetime, interval).
    assertThat(expr.getInputs().get(0).getReturnType()).isInstanceOf(TimestampType.class);
    assertThat(expr.getInputs().get(1).getReturnType()).isInstanceOf(IntervalDayTimeType.class);
  }

  @Test
  public void datePlusInterval() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
                dateInputRef(),
                monthInterval(1));
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    CallTypedExpr expr = (CallTypedExpr) converter.toTypedExpr(call, context);
    assertThat(expr.getFunctionName()).isEqualTo("plus");
    assertThat(expr.getReturnType()).isInstanceOf(DateType.class);
  }

  @Test
  public void timestampMinusInterval() {
    RexCall minus =
        (RexCall)
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS,
                tsInputRef(),
                hourInterval(3600000));
    RexCallConverter converter = RexCallConverterFactory.getConverter(minus, context);
    CallTypedExpr expr = (CallTypedExpr) converter.toTypedExpr(minus, context);
    assertThat(expr.getFunctionName()).isEqualTo("minus");
  }

  @Test
  public void intervalMultiplyBigint() {
    RexCall multiply =
        (RexCall)
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY,
                hourInterval(3600000),
                bigintInputRef());
    RexCallConverter converter = RexCallConverterFactory.getConverter(multiply, context);
    CallTypedExpr expr = (CallTypedExpr) converter.toTypedExpr(multiply, context);
    assertThat(expr.getFunctionName()).isEqualTo("multiply");
    assertThat(expr.getInputs().get(0).getReturnType()).isInstanceOf(IntervalDayTimeType.class);
  }

  @Test
  public void numericPlusStillUsesArithmeticConverter() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
                bigintInputRef(),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0));
    RexCallConverter converter = RexCallConverterFactory.getConverter(call, context);
    CallTypedExpr expr = (CallTypedExpr) converter.toTypedExpr(call, context);
    // Plain numeric plus must keep mapping to "add", not the interval branch.
    assertThat(expr.getFunctionName()).isEqualTo("add");
  }
}
