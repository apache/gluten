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

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimeString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RexNodeConverterTest {

  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;

  @BeforeAll
  public static void setUp() {
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  @Test
  public void testVarcharLiteralExtractsRawValue() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral("apple", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isEqualTo("apple");
  }

  @Test
  public void testCharLiteralExtractsRawValue() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral("banana", typeFactory.createSqlType(SqlTypeName.CHAR), false);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isEqualTo("banana");
  }

  @Test
  public void testNullLiteralConvertsToNullCarrier() {
    RexLiteral intervalLiteral =
        (RexLiteral)
            rexBuilder.makeNullLiteral(
                typeFactory.createSqlIntervalType(
                    new SqlIntervalQualifier(
                        TimeUnit.SECOND,
                        RelDataType.PRECISION_NOT_SPECIFIED,
                        TimeUnit.SECOND,
                        RelDataType.PRECISION_NOT_SPECIFIED,
                        SqlParserPos.ZERO)));
    assertThat(((VarCharValue) RexNodeConverter.toVariant(intervalLiteral)).getValue()).isNull();
    SqlTypeName[] names = {
      SqlTypeName.VARCHAR,
      SqlTypeName.INTEGER,
      SqlTypeName.BIGINT,
      SqlTypeName.DOUBLE,
      SqlTypeName.TIMESTAMP,
      SqlTypeName.DATE,
      SqlTypeName.SYMBOL,
    };
    for (SqlTypeName name : names) {
      RexLiteral literal = (RexLiteral) rexBuilder.makeNullLiteral(typeFactory.createSqlType(name));
      Variant variant = RexNodeConverter.toVariant(literal);
      assertThat(variant).isInstanceOf(VarCharValue.class);
      assertThat(((VarCharValue) variant).getValue()).isNull();
    }
  }

  @Test
  public void testDecimalNullLiteralConvertsToNullCarrier() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2));
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isNull();
  }

  // R1: CURRENT_TIMESTAMP maps by name to the flinksql current_timestamp
  // with a Timestamp (TIMESTAMP_LTZ(3)) result.
  @Test
  public void testCurrentTimestampMapping() {
    RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP);
    TypedExpr expr = RexNodeConverter.toTypedExpr(call, new RexConversionContext(List.of()));
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("current_timestamp");
    assertThat(expr.getReturnType()).isInstanceOf(TimestampType.class);
  }

  // R2: NOW has no named static field — it is registered at runtime via
  // FlinkSqlOperatorTable.dynamicFunctions (streaming mode here), matching
  // the operator shape the Flink planner hands to the converter.
  @Test
  public void testNowMapping() {
    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                FlinkSqlOperatorTable.dynamicFunctions(false /* isBatchMode */).stream()
                    .filter(f -> f.getName().equals("NOW"))
                    .findFirst()
                    .get());
    TypedExpr expr = RexNodeConverter.toTypedExpr(call, new RexConversionContext(List.of()));
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("now");
    assertThat(expr.getReturnType()).isInstanceOf(TimestampType.class);
  }

  // R3: LOCALTIMESTAMP maps by name to the flinksql localtimestamp with a
  // Timestamp (TIMESTAMP(3)) result.
  @Test
  public void testLocalTimestampMapping() {
    RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIMESTAMP);
    TypedExpr expr = RexNodeConverter.toTypedExpr(call, new RexConversionContext(List.of()));
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("localtimestamp");
    assertThat(expr.getReturnType()).isInstanceOf(TimestampType.class);
  }

  // R4: LOCALTIME maps by name to the flinksql localtime with an INTEGER
  // result — the TIME(0) milliseconds-of-day carrier.
  @Test
  public void testLocalTimeMapping() {
    RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    TypedExpr expr = RexNodeConverter.toTypedExpr(call, new RexConversionContext(List.of()));
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("localtime");
    assertThat(expr.getReturnType()).isInstanceOf(IntegerType.class);
  }

  // G1: CAST(LOCALTIME AS STRING) is rejected — casting the TIME carrier
  // would expose the raw integer.
  @Test
  public void testCastLocalTimeToStringRejected() {
    RexCall localtime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    RexCall cast =
        (RexCall) rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.VARCHAR), localtime);
    assertThatThrownBy(
            () -> RexNodeConverter.toTypedExpr(cast, new RexConversionContext(List.of())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("CAST")
        .hasMessageContaining("LOCALTIME direct projection");
  }

  // G2: EXTRACT(HOUR FROM LOCALTIME) is rejected — the velox extract
  // signature has no TIME form and would read the raw integer carrier.
  @Test
  public void testExtractHourFromLocalTimeRejected() {
    RexCall localtime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    RexCall extract =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.EXTRACT, rexBuilder.makeFlag(TimeUnit.HOUR), localtime);
    assertThatThrownBy(
            () -> RexNodeConverter.toTypedExpr(extract, new RexConversionContext(List.of())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("EXTRACT")
        .hasMessageContaining("LOCALTIME direct projection");
  }

  // G3: LOCALTIME < TIME '12:00:00' is rejected — TIME comparisons have no
  // supported carrier semantics.
  @Test
  public void testLocalTimeComparisonRejected() {
    RexCall localtime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    RexLiteral noon = rexBuilder.makeTimeLiteral(new TimeString(12, 0, 0), 0);
    RexCall lessThan =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, localtime, noon);
    assertThatThrownBy(
            () -> RexNodeConverter.toTypedExpr(lessThan, new RexConversionContext(List.of())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("<")
        .hasMessageContaining("LOCALTIME direct projection");
  }

  // G4: CASE WHEN b THEN LOCALTIME ELSE LOCALTIME END is rejected — the
  // TIME-typed result would silently integer-ize.
  @Test
  public void testCaseOverLocalTimeRejected() {
    RexCall localtime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    RexInputRef condition =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 0);
    RexCall caseCall =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition, localtime, localtime);
    assertThatThrownBy(
            () -> RexNodeConverter.toTypedExpr(caseCall, new RexConversionContext(List.of())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("CASE")
        .hasMessageContaining("LOCALTIME direct projection");
  }

  // G5: the whitelist — the LOCALTIME direct projection itself converts
  // (INTEGER carrier), and a TIME-typed InputRef flows through as the
  // isomorphic carrier.
  @Test
  public void testLocalTimeDirectProjectionAndInputRefAllowed() {
    RexCall localtime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    TypedExpr expr =
        RexNodeConverter.toTypedExpr(localtime, new RexConversionContext(java.util.List.of("f0")));
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("localtime");
    assertThat(expr.getReturnType()).isInstanceOf(IntegerType.class);

    RexInputRef timeRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIME), 0);
    TypedExpr refExpr =
        RexNodeConverter.toTypedExpr(timeRef, new RexConversionContext(java.util.List.of("f0")));
    assertThat(refExpr).isInstanceOf(FieldAccessTypedExpr.class);
    assertThat(((FieldAccessTypedExpr) refExpr).getFieldName()).isEqualTo("f0");
    assertThat(refExpr.getReturnType()).isInstanceOf(IntegerType.class);
  }
}
