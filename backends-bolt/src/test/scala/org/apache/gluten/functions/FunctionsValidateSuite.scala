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
package org.apache.gluten.functions

import org.apache.gluten.execution.WholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.types._

import java.nio.file.Files

import scala.collection.JavaConverters._

class FunctionsValidateSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"
  private var parquetPath: String = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set(
        "spark.sql.optimizer.excludedRules",
        ConstantFolding.ruleName + "," +
          NullPropagation.ruleName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    parquetPath = lfile.getAbsolutePath

    val schema = StructType(
      Array(
        StructField("double_field1", DoubleType, true),
        StructField("int_field1", IntegerType, true),
        StructField("string_field1", StringType, true)
      ))
    val rowData = Seq(
      Row(1.025, 1, "{\"a\":\"b\"}"),
      Row(1.035, 2, null),
      Row(1.045, 3, null)
    )

    val dfParquet = spark.createDataFrame(rowData.asJava, schema)
    dfParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)

    spark.catalog.createTable("datatab", parquetPath, fileFormat)
  }
}
