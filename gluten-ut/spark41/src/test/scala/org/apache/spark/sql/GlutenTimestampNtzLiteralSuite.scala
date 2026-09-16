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
package org.apache.spark.sql

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer
import org.apache.gluten.substrait.expression.ExpressionBuilder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StructField, StructType, TimestampNTZType, TimestampType}

import java.time.LocalDateTime

class GlutenTimestampNtzLiteralSuite extends GlutenSQLTestsTrait {

  testGluten("timestamp literal serialization preserves logical types") {
    Seq(TimestampNTZType, TimestampType).foreach {
      dataType =>
        Seq(-1L, 1704067200123456L).foreach {
          micros =>
            val scalar =
              ExpressionBuilder.makeLiteral(Long.box(micros), dataType, true).toProtobuf.getLiteral
            val array = ExpressionBuilder
              .makeLiteral(
                new GenericArrayData(Array[Any](micros, null)),
                ArrayType(dataType),
                true)
              .toProtobuf.getLiteral
            val struct = ExpressionBuilder
              .makeLiteral(
                InternalRow(micros, null),
                StructType(Seq(StructField("value", dataType), StructField("missing", dataType))),
                true)
              .toProtobuf.getLiteral
            Seq(scalar, array.getList.getValues(0), struct.getStruct.getFields(0)).foreach {
              literal =>
                if (dataType == TimestampNTZType) {
                  assert(literal.hasTimestamp)
                  assert(literal.getTimestamp == micros)
                } else {
                  assert(literal.hasTimestampTz)
                  assert(literal.getTimestampTz == micros)
                }
            }
            Seq(
              ExpressionBuilder.makeLiteral(null, dataType, true).toProtobuf.getLiteral,
              array.getList.getValues(1),
              struct.getStruct.getFields(1)).foreach {
              literal =>
                assert(literal.hasNull)
                if (dataType == TimestampNTZType) {
                  assert(literal.getNull.hasPrecisionTimestamp)
                } else {
                  assert(literal.getNull.hasPrecisionTimestampTz)
                }
            }
        }
    }
  }

  testGluten("timestamp_ntz literals in native expressions") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      GlutenConfig.GLUTEN_ANSI_FALLBACK_ENABLED.key -> "false",
      "spark.gluten.sql.columnar.backend.velox.enableTimestampNtzValidation" -> "false"
    ) {
      val result = spark.range(2).selectExpr(
        "timestampadd(MICROSECOND, id, cast('1969-12-31 23:59:59.999999' as timestamp_ntz))",
        "timestampadd(MICROSECOND, id, cast('2024-01-01 00:00:00.123456' as timestamp_ntz))"
      )
      checkAnswer(
        result,
        Seq(
          Row(
            LocalDateTime.parse("1969-12-31T23:59:59.999999"),
            LocalDateTime.parse("2024-01-01T00:00:00.123456")),
          Row(
            LocalDateTime.parse("1970-01-01T00:00:00"),
            LocalDateTime.parse("2024-01-01T00:00:00.123457"))
        )
      )
      val resultOutput = result.queryExecution.executedPlan.outputSet
      assert(
        getExecutedPlan(result).exists {
          case project: ProjectExecTransformer => project.outputSet == resultOutput
          case _ => false
        },
        result.queryExecution.executedPlan.treeString
      )
    }
  }
}
