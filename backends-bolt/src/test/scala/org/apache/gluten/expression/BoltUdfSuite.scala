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

import org.apache.gluten.backendsapi.bolt.BoltBackendSettings
import org.apache.gluten.execution.ProjectExecTransformer
import org.apache.gluten.tags.{SkipTest, UDFTest}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.expression.UDFResolver

import java.nio.file.Paths

abstract class BoltUdfSuite extends GlutenQueryTest with SQLHelper {

  protected val master: String

  private var _spark: SparkSession = _

  // This property is used for unit tests.
  val UDFLibPathProperty: String = "bolt.udf.lib.path"

  protected lazy val udfLibPath: String =
    sys.props.get(UDFLibPathProperty) match {
      case Some(path) =>
        path
      case None =>
        throw new IllegalArgumentException(
          UDFLibPathProperty + s" cannot be null. You may set it by adding " +
            s"-D$UDFLibPathProperty=" +
            "/path/to/gluten/cpp/build/bolt/udf/examples/libmyudf.so")
    }

  protected lazy val udfLibRelativePath: String =
    udfLibPath.split(",").map(p => Paths.get(p).getFileName.toString).mkString(",")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .master(master)
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }

    _spark.sparkContext.setLogLevel("warn")
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      doThreadPostAudit()
    }
  }

  override protected def spark = _spark

  protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
  }

  // Aggregate result can be flaky.
  ignore("test native hive udaf") {
    val tbl = "test_hive_udaf_replacement"
    withTempPath {
      dir =>
        try {
          // Check native hive udaf has been registered.
          val udafClass = "test.org.apache.spark.sql.MyDoubleAvg"
          assert(UDFResolver.UDAFNames.contains(udafClass))

          spark.sql(s"""
                       |CREATE TEMPORARY FUNCTION my_double_avg
                       |AS '$udafClass'
                       |""".stripMargin)
          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $tbl
                       |LOCATION 'file://$dir'
                       |AS select * from values (1, '1'), (2, '2'), (3, '3')
                       |""".stripMargin)
          val df = spark.sql(s"""select
                                |  my_double_avg(cast(col1 as double)),
                                |  my_double_avg(cast(col2 as double))
                                |  from $tbl
                                |""".stripMargin)
          val nativeImplicitConversionDF = spark.sql(s"""select
                                                        |  my_double_avg(col1),
                                                        |  my_double_avg(col2)
                                                        |  from $tbl
                                                        |""".stripMargin)
          val nativeResult = df.collect()
          val nativeImplicitConversionResult = nativeImplicitConversionDF.collect()

          UDFResolver.UDAFNames.remove(udafClass)
          val fallbackDF = spark.sql(s"""select
                                        |  my_double_avg(cast(col1 as double)),
                                        |  my_double_avg(cast(col2 as double))
                                        |  from $tbl
                                        |""".stripMargin)
          val fallbackResult = fallbackDF.collect()
          assert(nativeResult.sameElements(fallbackResult))
          assert(nativeImplicitConversionResult.sameElements(fallbackResult))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $tbl")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS my_double_avg")
        }
    }
  }

  test("test native hive udf") {
    val tbl = "test_hive_udf_replacement"
    withTempPath {
      dir =>
        try {
          spark.sql(s"""
                       |CREATE EXTERNAL TABLE $tbl
                       |LOCATION 'file://$dir'
                       |AS select * from values (1, '1'), (2, '2'), (3, '3')
                       |""".stripMargin)

          // Check native hive udf has been registered.
          assert(
            UDFResolver.UDFNames.contains("org.apache.spark.sql.hive.execution.UDFStringString"))

          spark.sql("""
                      |CREATE TEMPORARY FUNCTION hive_string_string
                      |AS 'org.apache.spark.sql.hive.execution.UDFStringString'
                      |""".stripMargin)

          val offloadWithImplicitConversionDF =
            spark.sql(s"""SELECT hive_string_string(col1, 'a') FROM $tbl""")
          checkGlutenPlan[ProjectExecTransformer](offloadWithImplicitConversionDF)
          val offloadWithImplicitConversionResult = offloadWithImplicitConversionDF.collect()

          val offloadDF =
            spark.sql(s"""SELECT hive_string_string(col2, 'a') FROM $tbl""")
          checkGlutenPlan[ProjectExecTransformer](offloadDF)
          val offloadResult = offloadWithImplicitConversionDF.collect()

          // Unregister native hive udf to fallback.
          UDFResolver.UDFNames.remove("org.apache.spark.sql.hive.execution.UDFStringString")
          val fallbackDF =
            spark.sql(s"""SELECT hive_string_string(col2, 'a') FROM $tbl""")
          checkSparkPlan[ProjectExec](fallbackDF)
          val fallbackResult = fallbackDF.collect()
          assert(offloadWithImplicitConversionResult.sameElements(fallbackResult))
          assert(offloadResult.sameElements(fallbackResult))

          // Add an unimplemented udf to the map to test fallback of registered native hive udf.
          UDFResolver.UDFNames.add("org.apache.spark.sql.hive.execution.UDFIntegerToString")
          spark.sql("""
                      |CREATE TEMPORARY FUNCTION hive_int_to_string
                      |AS 'org.apache.spark.sql.hive.execution.UDFIntegerToString'
                      |""".stripMargin)
          val df = spark.sql(s"""select hive_int_to_string(col1) from $tbl""")
          checkSparkPlan[ProjectExec](df)
          checkAnswer(df, Seq(Row("1"), Row("2"), Row("3")))
        } finally {
          spark.sql(s"DROP TABLE IF EXISTS $tbl")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_string_string")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_int_to_string")
        }
    }
  }

  test("test udf fallback in partition filter") {
    withTempPath {
      dir =>
        try {
          spark.sql("""
                      |CREATE TEMPORARY FUNCTION hive_int_to_string
                      |AS 'org.apache.spark.sql.hive.execution.UDFIntegerToString'
                      |""".stripMargin)

          spark.sql(s"""
                       |CREATE EXTERNAL TABLE t(i INT, p INT)
                       |LOCATION 'file://$dir'
                       |PARTITIONED BY (p)""".stripMargin)

          spark
            .range(0, 10, 1)
            .selectExpr("id as col")
            .createOrReplaceTempView("temp")

          for (part <- Seq(1, 2, 3, 4)) {
            spark.sql(s"""
                         |INSERT OVERWRITE TABLE t PARTITION (p=$part)
                         |SELECT col FROM temp""".stripMargin)
          }

          val df = spark.sql("SELECT i FROM t WHERE hive_int_to_string(p) = '4'")
          checkAnswer(df, (0 until 10).map(Row(_)))
        } finally {
          spark.sql("DROP TABLE IF EXISTS t")
          spark.sql("DROP VIEW IF EXISTS temp")
          spark.sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_string_string")
        }
    }
  }
}

@UDFTest
class BoltUdfSuiteLocal extends BoltUdfSuite {

  override val master: String = "local[2]"
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set(BoltBackendSettings.GLUTEN_BOLT_UDF_LIB_PATHS, udfLibRelativePath)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }
}

// Set below environment variables and VM options to run this test:
// export SCALA_HOME=/usr/share/scala
// export SPARK_SCALA_VERSION=2.12
//
// VM options:
// -Dspark.test.home=${SPARK_HOME}
// -Dgluten.package.jar=\
// /path/to/gluten/package/target/gluten-package-${project.version}.jar
// -Dbolt.udf.lib.path=\
// /path/to/gluten/cpp/build/bolt/udf/examples/libmyudf.so
@SkipTest
class BoltUdfSuiteCluster extends BoltUdfSuite {

  override val master: String = "local-cluster[2,2,1024]"

  val GLUTEN_JAR: String = "gluten.package.jar"

  private lazy val glutenJar = sys.props.get(GLUTEN_JAR) match {
    case Some(jar) => jar
    case None =>
      throw new IllegalArgumentException(
        GLUTEN_JAR + s" cannot be null. You may set it by adding " +
          s"-D$GLUTEN_JAR=" +
          "/path/to/gluten/package/target/gluten-package-${project.version}.jar")
  }

  private lazy val driverUdfLibPath =
    udfLibPath.split(",").map("file://" + _).mkString(",")

  override protected def beforeAll(): Unit = {
    assume(
      sys.props.contains(GLUTEN_JAR),
      s"Skip cluster UDF tests because required system property '$GLUTEN_JAR' is not set.")
    super.beforeAll()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.files", udfLibPath)
      .set(BoltBackendSettings.GLUTEN_BOLT_DRIVER_UDF_LIB_PATHS, driverUdfLibPath)
      .set(BoltBackendSettings.GLUTEN_BOLT_UDF_LIB_PATHS, udfLibRelativePath)
      .set("spark.driver.extraClassPath", glutenJar)
      .set("spark.executor.extraClassPath", glutenJar)
  }
}
