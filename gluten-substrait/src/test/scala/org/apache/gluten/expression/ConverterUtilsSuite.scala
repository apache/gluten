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
package org.apache.gluten.expression

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{DataType, IntegerType, TimestampType}

import io.substrait.proto.Type
import org.scalatest.funsuite.AnyFunSuiteLike

/**
 * Guards the Substrait 0.98 temporal type migration: Spark's TimestampType maps to
 * `PrecisionTimestampTZ` and TimestampNTZType maps to `PrecisionTimestamp`, both at microsecond
 * precision (6), and both round-trip back to the original Spark type.
 */
class ConverterUtilsSuite extends AnyFunSuiteLike {

  test("TimestampType emits PrecisionTimestampTZ(precision=6) and round-trips") {
    Seq(true, false).foreach {
      nullable =>
        val proto = ConverterUtils.getTypeNode(TimestampType, nullable).toProtobuf
        assert(proto.getKindCase === Type.KindCase.PRECISION_TIMESTAMP_TZ)
        assert(proto.getPrecisionTimestampTz.getPrecision === 6)
        assert(ConverterUtils.isNullable(proto.getPrecisionTimestampTz.getNullability) === nullable)

        val (dataType, parsedNullable) = ConverterUtils.parseFromSubstraitType(proto)
        assert(dataType === TimestampType)
        assert(parsedNullable === nullable)
    }
  }

  test("TimestampNTZType emits PrecisionTimestamp(precision=6) and round-trips") {
    // TimestampNTZType is package-private before Spark 3.4, so resolve the singleton
    // reflectively (mirroring ConverterUtils.parseFromSubstraitType) to keep this suite
    // compilable across all supported Spark versions.
    val timestampNTZType = Class
      .forName("org.apache.spark.sql.types.TimestampNTZType$")
      .getField("MODULE$")
      .get(null)
      .asInstanceOf[DataType]

    Seq(true, false).foreach {
      nullable =>
        val proto = ConverterUtils.getTypeNode(timestampNTZType, nullable).toProtobuf
        assert(proto.getKindCase === Type.KindCase.PRECISION_TIMESTAMP)
        assert(proto.getPrecisionTimestamp.getPrecision === 6)
        assert(ConverterUtils.isNullable(proto.getPrecisionTimestamp.getNullability) === nullable)

        val (dataType, parsedNullable) = ConverterUtils.parseFromSubstraitType(proto)
        assert(dataType === timestampNTZType)
        assert(parsedNullable === nullable)
    }
  }

  test("column names for an attribute with a null name") {
    // An attribute can have a null name, e.g. from a StructField built with a null name.
    val nullNamed = AttributeReference(null, IntegerType)()
    assert(ConverterUtils.genColumnNameWithoutExprId(nullNamed) === "")
    assert(ConverterUtils.genColumnNameWithExprId(nullNamed) === s"#${nullNamed.exprId.id}")

    val named = AttributeReference("B", IntegerType)()
    assert(ConverterUtils.genColumnNameWithExprId(named) === s"b#${named.exprId.id}")
  }
}
