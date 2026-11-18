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

import org.apache.spark.SparkException
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLType
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

class GlutenParquetSchemaInferenceSuite
  extends ParquetSchemaInferenceSuite
  with GlutenSQLTestsBaseTrait {}

class GlutenParquetSchemaSuite extends ParquetSchemaSuite with GlutenSQLTestsBaseTrait {

  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + fileName
  }

  testGluten("CANNOT_MERGE_SCHEMAS: Failed merging schemas") {
    import testImplicits._

    withTempPath {
      dir =>
        val path = dir.getCanonicalPath

        // Note: Velox backend always generates Parquet files with nullable = true,
        // regardless of whether nullable is set to false or true in the schema.
        // Before https://github.com/apache/spark/pull/44644, `StructField.sql` would not
        // return the `NOT NULL` qualifier. This is why this test succeeds in Spark 3.5.
        val schema1 = StructType(Seq(StructField("id", LongType, nullable = true)))
        val df1 = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(0L), Row(1L), Row(2L))),
          schema1)
        df1.write.parquet(s"$path/p=1")
        val df2 = df1.select($"id".cast(IntegerType).as(Symbol("id")))
        df2.write.parquet(s"$path/p=2")

        checkError(
          exception = intercept[SparkException] {
            spark.read.option("mergeSchema", "true").parquet(path)
          },
          condition = "CANNOT_MERGE_SCHEMAS",
          sqlState = "42KD9",
          parameters = Map("left" -> toSQLType(df1.schema), "right" -> toSQLType(df2.schema))
        )
    }
  }
}
