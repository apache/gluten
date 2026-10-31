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

import org.apache.gluten.rexnode.functions.RexCallConverterFactory;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.TimestampValue;
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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
  public void testDateLiteralExtractsRawValue() {
    // Calcite keeps a DATE literal as int epoch days — the velox DATE representation.
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral(20707, typeFactory.createSqlType(SqlTypeName.DATE), false);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(IntegerValue.class);
    assertThat(((IntegerValue) variant).getValue()).isEqualTo(20707);
  }

  @Test
  public void testNullDateLiteralConvertsToNullVariant() {
    // TO_DATE(NULL) folds to a NULL DATE literal; it must produce a null variant.
    RexLiteral literal = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.DATE));
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isNull();
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

  @Test
  public void testIdentityCastAroundDateLiteralIsRemoved() {
    // The planner wraps folded constants in identity casts; velox rejects same-type casts.
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral(20707, typeFactory.createSqlType(SqlTypeName.DATE), false);
    RexCall cast =
        (RexCall)
            rexBuilder.makeAbstractCast(
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.DATE), true),
                literal);
    TypedExpr expr =
        RexNodeConverter.toTypedExpr(
            cast, new RexConversionContext(java.util.Collections.emptyList()));
    assertThat(expr).isInstanceOf(ConstantTypedExpr.class);
    ConstantTypedExpr constant = (ConstantTypedExpr) expr;
    assertThat(constant.getReturnType()).isInstanceOf(DateType.class);
    assertThat(constant.getValue()).isInstanceOf(IntegerValue.class);
    assertThat(((IntegerValue) constant.getValue()).getValue()).isEqualTo(20707);
  }

  @Test
  public void testToDateCallMapsToVeloxName() {
    RexInputRef stringFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexInputRef formatFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "s", "f"));

    RexCall oneArg = (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.TO_DATE, stringFieldRef);
    CallTypedExpr oneArgExpr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(oneArg, context).toTypedExpr(oneArg, context);
    assertThat(oneArgExpr.getFunctionName()).isEqualTo("to_date");
    assertThat(oneArgExpr.getReturnType()).isInstanceOf(DateType.class);
    assertThat(oneArgExpr.getInputs()).hasSize(1);

    RexCall twoArgs =
        (RexCall)
            rexBuilder.makeCall(FlinkSqlOperatorTable.TO_DATE, stringFieldRef, formatFieldRef);
    CallTypedExpr twoArgExpr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(twoArgs, context).toTypedExpr(twoArgs, context);
    assertThat(twoArgExpr.getFunctionName()).isEqualTo("to_date");
    assertThat(twoArgExpr.getReturnType()).isInstanceOf(DateType.class);
    assertThat(twoArgExpr.getInputs()).hasSize(2);
  }

  @Test
  public void testIdentityCastOnCompositeTypeIsRemoved() {
    // Nullability-only casts on composites are identity too (all component types match).
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexConversionContext context =
        new RexConversionContext(java.util.Collections.singletonList("f0"));

    assertIdentityCastRemoved(typeFactory.createArrayType(intType, -1), context);

    assertIdentityCastRemoved(
        typeFactory.createStructType(
            java.util.Collections.singletonList(intType), java.util.Collections.singletonList("f")),
        context);

    assertIdentityCastRemoved(typeFactory.createMapType(intType, intType), context);
  }

  @Test
  public void testCompositeCastWithDifferentComponentTypeIsKept() {
    // Component type differences are not identity; the cast must stay a cast.
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType bigType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    RexConversionContext context =
        new RexConversionContext(java.util.Collections.singletonList("f0"));

    assertCastKept(
        typeFactory.createArrayType(intType, -1),
        typeFactory.createArrayType(bigType, -1),
        context);

    assertCastKept(
        typeFactory.createStructType(
            java.util.Collections.singletonList(intType), java.util.Collections.singletonList("f")),
        typeFactory.createStructType(
            java.util.Collections.singletonList(bigType), java.util.Collections.singletonList("f")),
        context);

    assertCastKept(
        typeFactory.createMapType(intType, intType),
        typeFactory.createMapType(bigType, intType),
        context);

    assertCastKept(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 5),
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 10),
        context);

    assertCastKept(intType, bigType, context);
  }

  @Test
  public void testTimestampLiteralExtractsRawValue() {
    // The folded TO_TIMESTAMP literal keeps the wall time as UTC epoch millis.
    RexLiteral literal =
        (RexLiteral) rexBuilder.makeTimestampLiteral(new TimestampString("2026-09-11 10:00:00"), 3);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(TimestampValue.class);
    assertThat(((TimestampValue) variant).getSeconds()).isEqualTo(1789120800L);
    assertThat(((TimestampValue) variant).getNanos()).isEqualTo(0L);
  }

  @Test
  public void testTimestampWithLocalTimeZoneLiteralExtractsRawValue() {
    // The folded TO_TIMESTAMP_LTZ literal shares the TIMESTAMP conversion:
    // the epoch-millis base (Calcite resolves both names through
    // TimestampString#getMillisSinceEpoch; the Flink round-trip is atOffset(UTC)).
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
                new TimestampString("2026-09-11 10:00:00"), 3);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(TimestampValue.class);
    assertThat(((TimestampValue) variant).getSeconds()).isEqualTo(1789120800L);
    assertThat(((TimestampValue) variant).getNanos()).isEqualTo(0L);
  }

  @Test
  public void testNullLtzLiteralConvertsToNullVariant() {
    // TO_TIMESTAMP_LTZ never folds to NULL (the TableException aborts
    // folding), but a NULL LTZ literal from any path must not NPE the planner.
    RexLiteral literal =
        rexBuilder.makeNullLiteral(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3));
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isNull();
  }

  @Test
  public void testToTimestampCallMapsToVeloxName() {
    RexInputRef stringFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexInputRef formatFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "s", "f"));

    RexCall oneArg =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.TO_TIMESTAMP, stringFieldRef);
    CallTypedExpr oneArgExpr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(oneArg, context).toTypedExpr(oneArg, context);
    assertThat(oneArgExpr.getFunctionName()).isEqualTo("to_timestamp");
    assertThat(oneArgExpr.getReturnType()).isInstanceOf(TimestampType.class);
    assertThat(oneArgExpr.getInputs()).hasSize(1);

    RexCall twoArgs =
        (RexCall)
            rexBuilder.makeCall(FlinkSqlOperatorTable.TO_TIMESTAMP, stringFieldRef, formatFieldRef);
    CallTypedExpr twoArgExpr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(twoArgs, context).toTypedExpr(twoArgs, context);
    assertThat(twoArgExpr.getFunctionName()).isEqualTo("to_timestamp");
    assertThat(twoArgExpr.getReturnType()).isInstanceOf(TimestampType.class);
    assertThat(twoArgExpr.getInputs()).hasSize(2);
  }

  @Test
  public void testToTimestampLtzCallMapsToVeloxName() {
    RexInputRef bigIntFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    RexLiteral precisionLiteral =
        (RexLiteral)
            rexBuilder.makeLiteral(3, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "x", "p"));

    RexCall call =
        (RexCall)
            rexBuilder.makeCall(
                FlinkSqlOperatorTable.TO_TIMESTAMP_LTZ, bigIntFieldRef, precisionLiteral);
    CallTypedExpr expr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(call, context).toTypedExpr(call, context);
    assertThat(expr.getFunctionName()).isEqualTo("to_timestamp_ltz");
    assertThat(expr.getReturnType()).isInstanceOf(TimestampType.class);
    assertThat(expr.getInputs()).hasSize(2);
  }

  @Test
  public void testIdentityCastAroundLtzLiteralIsRemoved() {
    // The planner wraps folded constants in identity casts (e.g. when the sink
    // declares TIMESTAMP_LTZ(3)); nullability-only casts are identity and pass
    // through, keeping the folding path reachable.
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
                new TimestampString("2026-09-11 10:00:00"), 3);
    RexCall cast =
        (RexCall)
            rexBuilder.makeAbstractCast(
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3), true),
                literal);
    TypedExpr expr =
        RexNodeConverter.toTypedExpr(
            cast, new RexConversionContext(java.util.Collections.emptyList()));
    assertThat(expr).isInstanceOf(ConstantTypedExpr.class);
    ConstantTypedExpr constant = (ConstantTypedExpr) expr;
    assertThat(constant.getReturnType()).isInstanceOf(TimestampType.class);
    assertThat(constant.getValue()).isInstanceOf(TimestampValue.class);
    assertThat(((TimestampValue) constant.getValue()).getSeconds()).isEqualTo(1789120800L);
    assertThat(((TimestampValue) constant.getValue()).getNanos()).isEqualTo(0L);
  }

  private void assertIdentityCastRemoved(RelDataType fromType, RexConversionContext context) {
    RexInputRef ref = rexBuilder.makeInputRef(fromType, 0);
    RexCall cast =
        (RexCall)
            rexBuilder.makeAbstractCast(typeFactory.createTypeWithNullability(fromType, true), ref);
    TypedExpr expr = RexNodeConverter.toTypedExpr(cast, context);
    assertThat(expr).isInstanceOf(FieldAccessTypedExpr.class);
    assertThat(((FieldAccessTypedExpr) expr).getFieldName()).isEqualTo("f0");
  }

  private void assertCastKept(
      RelDataType fromType, RelDataType toType, RexConversionContext context) {
    RexInputRef ref = rexBuilder.makeInputRef(fromType, 0);
    RexCall cast = (RexCall) rexBuilder.makeAbstractCast(toType, ref);
    TypedExpr expr = RexNodeConverter.toTypedExpr(cast, context);
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) expr).getFunctionName()).isEqualTo("cast");
    assertThat(((CallTypedExpr) expr).getInputs()).hasSize(1);
  }
}
