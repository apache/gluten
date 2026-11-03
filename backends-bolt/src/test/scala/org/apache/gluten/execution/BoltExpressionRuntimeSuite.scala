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
package org.apache.gluten.execution

import org.apache.gluten.config.{BoltConfig, GlutenConfig}
import org.apache.gluten.expression.aggregate.BoltBloomFilterAggregate
import org.apache.gluten.utils.BoltBloomFilter

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.classic.ClassicColumn
import org.apache.spark.sql.types._
import org.apache.spark.task.TaskResources

class BoltExpressionRuntimeSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "placeholder"
  override protected val fileFormat: String = "parquet"

  private def withInput(schema: StructType, rows: Seq[Row])(f: => Unit): Unit = {
    withTempPath {
      path =>
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
            .coalesce(1).write.parquet(path.getCanonicalPath)
        }
        withTempView("bolt_expression_input") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("bolt_expression_input")
          f
        }
    }
  }

  for (chunking <- Seq(true, false)) {
    testWithMinSparkVersion(
      s"Base64 native output preserves long input with chunking=$chunking",
      "3.5") {
      withInput(
        new StructType().add("bytes", BinaryType),
        Seq(Row(Array.fill[Byte](200)(97)), Row(null))) {
        withSQLConf("spark.sql.chunkBase64String.enabled" -> chunking.toString) {
          runQueryAndCompare("SELECT base64(bytes) FROM bolt_expression_input") {
            df => assert(getExecutedPlan(df).exists(_.isInstanceOf[ProjectExecTransformer]))
          }
        }
      }
    }
  }

  test("from_json native output preserves nested mixed-case keys") {
    val json =
      """{"Outer":{"ChildKey":7},"ArrayKey":[{"ChildKey":8}],"MapKey":{"x":{"ChildKey":9}}}"""
    withInput(new StructType().add("json", StringType), Seq(Row(json), Row(null))) {
      runQueryAndCompare(
        """SELECT from_json(json,
          |'STRUCT<Outer:STRUCT<ChildKey:INT>,ArrayKey:ARRAY<STRUCT<ChildKey:INT>>,MapKey:MAP<STRING,STRUCT<ChildKey:INT>>>')
          |FROM bolt_expression_input""".stripMargin) {
        df => assert(getExecutedPlan(df).exists(_.isInstanceOf[ProjectExecTransformer]))
      }
    }
  }

  for (bits <- Seq(1024L, 32768L)) {
    test(s"native and JVM Bloom aggregates merge with bit budget $bits") {
      withInput(new StructType().add("value", LongType), Seq(Row(1L), Row(2L), Row(null))) {
        withSQLConf(BoltConfig.COLUMNAR_BOLT_BLOOM_FILTER_MAX_NUM_BITS.key -> "4096") {
          val expression = BloomFilterAggregate(
            UnresolvedAttribute("value"),
            Literal(100000L),
            Literal(bits),
            0,
            0).toAggregateExpression()
          val df = spark.table("bolt_expression_input").select(ClassicColumn(expression).as("bf"))
          val bytes = df.collect().head.getAs[Array[Byte]](0)
          assert(getExecutedPlan(df).exists(_.isInstanceOf[HashAggregateExecTransformer]))
          TaskResources.runUnsafe {
            val aggregate = BoltBloomFilterAggregate(
              BoundReference(0, LongType, nullable = true),
              Literal(100000L),
              Literal(bits),
              0,
              0)
            val fallback = aggregate.createAggregationBuffer()
            aggregate.update(fallback, InternalRow(99L))
            val native = BoltBloomFilter.readFrom(bytes)
            // Mixing partial results must retain every inserted value. This checks the
            // independent native aggregate against the JVM fallback's allocation policy.
            val merged = aggregate.merge(native, fallback)
            Seq(1L, 2L, 99L).foreach(value => assert(merged.mightContainLong(value)))
          }
        }
      }
    }
  }
}
