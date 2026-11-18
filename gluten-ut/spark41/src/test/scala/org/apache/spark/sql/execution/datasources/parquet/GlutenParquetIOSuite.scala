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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType, StringType, StructType}

import scala.jdk.CollectionConverters._

/** A test suite that tests basic Parquet I/O. */
class GlutenParquetIOSuite extends ParquetIOSuite with GlutenSQLTestsBaseTrait {
  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + fileName
  }

  override protected def readResourceParquetFile(name: String): DataFrame = {
    spark.read.parquet(testFile(name))
  }

  // The Spark test also runs the scan node alone with VerifyNoAdditionalScanOutputExec, which
  // does not work for Gluten's scan node. Check only the result here.
  testGluten(
    "SPARK-53535: vectorized reader: missing all struct fields, struct with complex fields") {
    val data = Seq(
      Row(Row(Seq(11, 12, null, 14), Row("21", 22), Row(true)), 100),
      Row(Row(Seq(11, 12, null, 14), Row("21", 22), Row(false)), 100),
      Row(null, 100)
    )

    val tableSchema = new StructType()
      .add(
        "_1",
        new StructType()
          .add("_1", ArrayType(IntegerType, containsNull = true))
          .add("_2", new StructType().add("_1", StringType).add("_2", IntegerType))
          .add("_3", new StructType().add("_1", BooleanType))
      )
      .add("_2", IntegerType)

    val readSchema = new StructType()
      .add("_1", new StructType().add("_101", IntegerType).add("_102", LongType))

    withTempPath {
      path =>
        val file = path.getCanonicalPath
        spark.createDataFrame(data.asJava, tableSchema).write.partitionBy("_2").parquet(file)

        for {
          offheapEnabled <- Seq(true, false)
          returnNullStructIfAllFieldsMissing <- Seq(true, false)
        } {
          withSQLConf(
            SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.key ->
              returnNullStructIfAllFieldsMissing.toString,
            SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
            SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString
          ) {
            val expectedAnswer = if (!returnNullStructIfAllFieldsMissing) {
              Row(Row(null, null), 100) :: Row(Row(null, null), 100) :: Row(null, 100) :: Nil
            } else {
              Row(null, 100) :: Row(null, 100) :: Row(null, 100) :: Nil
            }

            withAllParquetReaders {
              checkAnswer(spark.read.schema(readSchema).parquet(file), expectedAnswer)
            }
          }
        }
    }
  }

  testGluten(
    "SPARK-54220: vectorized reader: missing all struct fields, struct with NullType only") {
    val data = Seq(
      Tuple1((null, null)),
      Tuple1((null, null)),
      Tuple1(null)
    )
    val readSchema = new StructType().add(
      "_1",
      new StructType()
        .add("_3", IntegerType, nullable = true)
        .add("_4", StringType, nullable = true),
      nullable = true)
    val expectedAnswer = Row(Row(null, null)) :: Row(Row(null, null)) :: Row(null) :: Nil

    withParquetFile(data) {
      file =>
        for (offheapEnabled <- Seq(true, false)) {
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
            SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.key -> "false",
            SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString
          ) {
            withAllParquetReaders {
              val df = spark.read.schema(readSchema).parquet(file)
              checkAnswer(df, expectedAnswer)
            }
          }
        }
    }
  }
}
