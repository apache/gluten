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
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    int[] invalidLengths = {0, 385, -1};
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
}
