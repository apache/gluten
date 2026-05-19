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
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{AnalysisException, GlutenTestSetWithSystemPropertyTrait, Row}
import org.apache.spark.tags.SlowHiveTest

@SlowHiveTest
class GlutenHiveDDLSuite
  extends HiveDDLSuite
  with GlutenTestSetWithSystemPropertyTrait
  with GlutenHiveResourcePathSupport {

  override def testNameBlackList: Seq[String] = Seq(
    // Rewritten with workspace-backed schema URLs because TestHive.getHiveFile resolves
    // these resources from the spark-hive tests jar in gluten-ut.
    "SPARK-34370: support Avro schema evolution (add column with avro.schema.url)",
    "SPARK-34370: support Avro schema evolution (remove column with avro.schema.url)",
    "SPARK-34261: Avoid side effect if create exists temporary function"
  )

  testGluten("SPARK-34370: support Avro schema evolution (add column with avro.schema.url)") {
    checkAvroSchemaEvolutionAddColumn(
      avroSchemaUrlProperty("schemaWithOneField.avsc"),
      avroSchemaUrlProperty("schemaWithTwoFields.avsc"))
  }

  testGluten("SPARK-34370: support Avro schema evolution (remove column with avro.schema.url)") {
    checkAvroSchemaEvolutionRemoveColumn(
      avroSchemaUrlProperty("schemaWithTwoFields.avsc"),
      avroSchemaUrlProperty("schemaWithOneField.avsc"))
  }

  testGluten("SPARK-34261: Avoid side effect if create exists temporary function") {
    withUserDefinedFunction("f1" -> true) {
      sql("CREATE TEMPORARY FUNCTION f1 AS 'org.apache.hadoop.hive.ql.udf.UDFUUID'")

      val jarName = "TestUDTF.jar"
      val jar = hiveResourcePath(jarName).toUri.toString
      spark.sparkContext.addedJars.keys
        .find(_.contains(jarName))
        .foreach(spark.sparkContext.addedJars.remove)
      assert(!spark.sparkContext.listJars().exists(_.contains(jarName)))
      val msg = intercept[AnalysisException] {
        sql(
          "CREATE TEMPORARY FUNCTION f1 AS " +
            s"'org.apache.hadoop.hive.ql.udf.UDFUUID' USING JAR '$jar'")
      }.getMessage
      assert(msg.contains("Function f1 already exists"))
      assert(!spark.sparkContext.listJars().exists(_.contains(jarName)))

      sql(
        "CREATE OR REPLACE TEMPORARY FUNCTION f1 AS " +
          s"'org.apache.hadoop.hive.ql.udf.UDFUUID' USING JAR '$jar'")
      assert(spark.sparkContext.listJars().exists(_.contains(jarName)))
    }
  }

  private def avroSchemaUrlProperty(fileName: String): String = {
    val schemaPath = hiveResourcePath(fileName)
    s"'avro.schema.url'='${schemaPath.toUri.toString}'"
  }

  private def checkAvroSchemaEvolutionAddColumn(
      originalSerdeProperties: String,
      evolvedSerdeProperties: String): Unit = {
    withTable("t") {
      sql(s"""
             |CREATE TABLE t PARTITIONED BY (ds string)
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |WITH SERDEPROPERTIES ($originalSerdeProperties)
             |STORED AS
             |INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |""".stripMargin)
      sql("INSERT INTO t partition (ds='1981-01-07') VALUES ('col2_value')")
      sql(s"ALTER TABLE t SET SERDEPROPERTIES ($evolvedSerdeProperties)")
      sql("INSERT INTO t partition (ds='1983-04-27') VALUES ('col1_value', 'col2_value')")
      checkAnswer(
        spark.table("t"),
        Row("col1_default", "col2_value", "1981-01-07") ::
          Row("col1_value", "col2_value", "1983-04-27") :: Nil)
    }
  }

  private def checkAvroSchemaEvolutionRemoveColumn(
      originalSerdeProperties: String,
      evolvedSerdeProperties: String): Unit = {
    withTable("t") {
      sql(s"""
             |CREATE TABLE t PARTITIONED BY (ds string)
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |WITH SERDEPROPERTIES ($originalSerdeProperties)
             |STORED AS
             |INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |""".stripMargin)
      sql("INSERT INTO t partition (ds='1983-04-27') VALUES ('col1_value', 'col2_value')")
      sql(s"ALTER TABLE t SET SERDEPROPERTIES ($evolvedSerdeProperties)")
      sql("INSERT INTO t partition (ds='1981-01-07') VALUES ('col2_value')")
      checkAnswer(
        spark.table("t"),
        Row("col2_value", "1981-01-07") ::
          Row("col2_value", "1983-04-27") :: Nil)
    }
  }
}
