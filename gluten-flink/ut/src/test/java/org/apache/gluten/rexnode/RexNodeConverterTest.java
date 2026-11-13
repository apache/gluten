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
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.DateType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.TimestampValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableRangeSet;
import org.apache.flink.calcite.shaded.com.google.common.collect.Range;
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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
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
  public void testTimeLiteralConvertsToMillisOfDay() {
    RexLiteral literal = rexBuilder.makeTimeLiteral(new TimeString(12, 34, 56), 0);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(IntegerValue.class);
    assertThat(((IntegerValue) variant).getValue())
        .isEqualTo(12 * 3_600_000 + 34 * 60_000 + 56_000);
  }

  @Test
  public void testTimeNullLiteralConvertsToNullCarrier() {
    RexLiteral literal =
        (RexLiteral) rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.TIME));
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isNull();
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
  public void testSha2FamilyCallsMapToVeloxNames() {
    RexInputRef stringFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexInputRef intFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "s", "n"));

    RexCall sha2 =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, intFieldRef);
    CallTypedExpr sha2Expr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(sha2, context).toTypedExpr(sha2, context);
    assertThat(sha2Expr.getFunctionName()).isEqualTo("sha2");
    assertThat(sha2Expr.getReturnType()).isInstanceOf(VarCharType.class);
    assertThat(sha2Expr.getInputs()).hasSize(2);

    SqlFunction[] fixedLengthOps = {
      FlinkSqlOperatorTable.SHA224,
      FlinkSqlOperatorTable.SHA256,
      FlinkSqlOperatorTable.SHA384,
      FlinkSqlOperatorTable.SHA512
    };
    String[] veloxNames = {"sha224", "sha256", "sha384", "sha512"};
    for (int i = 0; i < fixedLengthOps.length; i++) {
      RexCall call = (RexCall) rexBuilder.makeCall(fixedLengthOps[i], stringFieldRef);
      CallTypedExpr expr =
          (CallTypedExpr)
              RexCallConverterFactory.getConverter(call, context).toTypedExpr(call, context);
      assertThat(expr.getFunctionName()).isEqualTo(veloxNames[i]);
      assertThat(expr.getReturnType()).isInstanceOf(VarCharType.class);
      assertThat(expr.getInputs()).hasSize(1);
    }
  }

  @Test
  public void testSha2LiteralInvalidLengthRejected() {
    // Flink validates a constant hashLength in the generated operator's
    // constructor (CodeGeneratorContext#addReusableSha2MessageDigest throws
    // "Unsupported algorithm." for anything outside {224, 256, 384, 512}) and
    // fails the job even on empty input; the conversion rejects the constant
    // form the same way, while the column form keeps the per-row rejection.
    RexInputRef stringFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "s", "n"));

    int[] invalidLengths = {0, 385, -1, 1};
    for (int length : invalidLengths) {
      RexLiteral literal =
          (RexLiteral)
              rexBuilder.makeLiteral(length, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
      RexCall call =
          (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, literal);
      assertThatThrownBy(() -> RexCallConverterFactory.getConverter(call, context))
          .hasMessageContaining(
              String.format("SHA2 with literal hashLength %d is not supported", length));
    }

    // The BIGINT form (a folded CAST(... AS BIGINT)) rejects too.
    RexLiteral bigZero =
        (RexLiteral)
            rexBuilder.makeLiteral(0L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
    RexCall bigCall =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, bigZero);
    assertThatThrownBy(() -> RexCallConverterFactory.getConverter(bigCall, context))
        .hasMessageContaining("SHA2 with literal hashLength 0 is not supported");
  }

  @Test
  public void testSha2LiteralValidAndNullLengthStillConvert() {
    RexInputRef stringFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexConversionContext context =
        new RexConversionContext(java.util.Arrays.asList("ignored", "s", "n"));

    // A valid INT constant reaches velox intact.
    RexLiteral intLength =
        (RexLiteral)
            rexBuilder.makeLiteral(256, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexCall intCall =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, intLength);
    CallTypedExpr intExpr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(intCall, context).toTypedExpr(intCall, context);
    assertThat(intExpr.getFunctionName()).isEqualTo("sha2");
    assertThat(intExpr.getInputs()).hasSize(2);
    ConstantTypedExpr length = (ConstantTypedExpr) intExpr.getInputs().get(1);
    assertThat(length.getReturnType()).isInstanceOf(IntegerType.class);
    assertThat(((IntegerValue) length.getValue()).getValue()).isEqualTo(256);

    // A valid BIGINT constant routes through the int64 signature.
    RexLiteral bigLength =
        (RexLiteral)
            rexBuilder.makeLiteral(512L, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
    RexCall bigCall =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, bigLength);
    CallTypedExpr bigExpr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(bigCall, context).toTypedExpr(bigCall, context);
    assertThat(bigExpr.getFunctionName()).isEqualTo("sha2");
    ConstantTypedExpr bigLengthExpr = (ConstantTypedExpr) bigExpr.getInputs().get(1);
    assertThat(bigLengthExpr.getReturnType()).isInstanceOf(BigIntType.class);
    assertThat(((BigIntValue) bigLengthExpr.getValue()).getValue()).isEqualTo(512L);

    // A NULL constant cannot be checked at plan time; it converts like any
    // other NULL carrier and keeps the per-row rejection in velox.
    RexLiteral nullLength =
        (RexLiteral) rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexCall nullCall =
        (RexCall) rexBuilder.makeCall(FlinkSqlOperatorTable.SHA2, stringFieldRef, nullLength);
    CallTypedExpr nullExpr =
        (CallTypedExpr)
            RexCallConverterFactory.getConverter(nullCall, context).toTypedExpr(nullCall, context);
    assertThat(nullExpr.getFunctionName()).isEqualTo("sha2");
    assertThat(nullExpr.getInputs()).hasSize(2);
    Variant nullVariant = ((ConstantTypedExpr) nullExpr.getInputs().get(1)).getValue();
    assertThat(nullVariant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) nullVariant).getValue()).isNull();
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
  // away from TIME would expose the raw integer.
  @Test
  public void testCastLocalTimeToStringRejected() {
    RexCall localtime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    RexCall cast =
        (RexCall) rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.VARCHAR), localtime);
    assertThatThrownBy(
            () -> RexNodeConverter.toTypedExpr(cast, new RexConversionContext(List.of())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("CAST")
        .hasMessageContaining("milliseconds-of-day carrier");
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
        .hasMessageContaining("milliseconds-of-day carrier");
  }

  // G3: LOCALTIME < TIME '12:00:00' is supported — comparisons on the
  // INTEGER carrier keep TIME ordering semantics (CURRENT_TIME comparisons
  // are part of the same supported surface).
  @Test
  public void testLocalTimeComparisonAllowed() {
    RexCall localtime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    RexLiteral noon = rexBuilder.makeTimeLiteral(new TimeString(12, 0, 0), 0);
    RexCall lessThan =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, localtime, noon);
    TypedExpr expr = RexNodeConverter.toTypedExpr(lessThan, new RexConversionContext(List.of()));
    assertThat(expr).isInstanceOf(CallTypedExpr.class);
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
        .hasMessageContaining("milliseconds-of-day carrier");
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

  // G6: CAST to TIME keeps carrier semantics only when the operand is already
  // TIME — CAST(CURRENT_TIME AS TIME) (the upstream surface) passes, while
  // CAST(<timestamp or varchar column> AS TIME) would lower to a velox
  // cast(<type> AS INTEGER) that neither computes the milliseconds-of-day
  // nor parses a time-of-day string, so it is rejected at plan time.
  @Test
  public void testCastToTimeFromNonTimeOperandRejected() {
    RexCall castTime = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LOCALTIME);
    RexCall cast =
        (RexCall) rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.TIME), castTime);
    TypedExpr timeToTime = RexNodeConverter.toTypedExpr(cast, new RexConversionContext(List.of()));
    assertThat(timeToTime).isInstanceOf(CallTypedExpr.class);

    RexInputRef tsRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3), 0);
    RexCall tsCast =
        (RexCall) rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.TIME), tsRef);
    assertThatThrownBy(
            () -> RexNodeConverter.toTypedExpr(tsCast, new RexConversionContext(List.of())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("CAST")
        .hasMessageContaining("casts a non-TIME operand to TIME")
        .hasMessageContaining("milliseconds-of-day carrier");

    RexInputRef strRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
    RexCall strCast =
        (RexCall) rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.TIME), strRef);
    assertThatThrownBy(
            () -> RexNodeConverter.toTypedExpr(strCast, new RexConversionContext(List.of())))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("CAST")
        .hasMessageContaining("casts a non-TIME operand to TIME")
        .hasMessageContaining("milliseconds-of-day carrier");
  }

  // G7: IS [NOT] NULL and SEARCH over a TIME-typed operand are
  // carrier-isomorphic — a nullability check and a point / range search on
  // the INTEGER carrier — and plan through the existing converters: IS [NOT]
  // NULL lowers to isnull / isnotnull over the carrier, and SEARCH (the
  // Calcite form of IN) lowers to "in" over the carrier millisecond-of-day
  // points.
  @Test
  public void testCarrierPredicatesOverTimeAllowed() {
    RexInputRef timeRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.TIME), 0);
    RexConversionContext context = new RexConversionContext(List.of("t"));

    RexCall isNotNull = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, timeRef);
    TypedExpr isNotNullExpr = RexNodeConverter.toTypedExpr(isNotNull, context);
    assertThat(isNotNullExpr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) isNotNullExpr).getFunctionName()).isEqualTo("isnotnull");

    RexCall isNull = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, timeRef);
    TypedExpr isNullExpr = RexNodeConverter.toTypedExpr(isNull, context);
    assertThat(isNullExpr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) isNullExpr).getFunctionName()).isEqualTo("isnull");

    TimeString noon = new TimeString(12, 0, 0);
    TimeString halfPastNoon = new TimeString(12, 30, 0);
    RexLiteral searchLiteral =
        rexBuilder.makeSearchArgumentLiteral(
            Sarg.of(
                false,
                ImmutableRangeSet.<TimeString>builder()
                    .add(Range.closed(noon, noon))
                    .add(Range.closed(halfPastNoon, halfPastNoon))
                    .build()),
            typeFactory.createSqlType(SqlTypeName.TIME));
    RexCall search =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, timeRef, searchLiteral);
    TypedExpr searchExpr = RexNodeConverter.toTypedExpr(search, context);
    assertThat(searchExpr).isInstanceOf(CallTypedExpr.class);
    assertThat(((CallTypedExpr) searchExpr).getFunctionName()).isEqualTo("in");
  }
}
