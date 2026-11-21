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

import org.apache.gluten.config.BoltConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._

class BoltExplodeExpressionSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = "/"
  override protected val fileFormat: String = "parquet"
  private var parquetPath: String = _
  private var explodeDataPath: File = _
  override def beforeAll(): Unit = {
    super.beforeAll()
    createDataExplodeTable()
    // custom Dataset
    var dfile = Files.createTempFile("", ".parquet").toFile
    dfile.deleteOnExit()
    parquetPath = dfile.getAbsolutePath
    val explodeTestSchema = StructType(
      Array(
        StructField("c1", IntegerType, true),
        StructField("c2", StringType, true),
        StructField("c3", DataTypes.createArrayType(StringType), true),
        StructField("c4", DataTypes.createArrayType(StringType), true),
        StructField("c5", DataTypes.createArrayType(StringType), true)
      )
    )
    val rowExplodeTestData = Seq(
      Row(
        1,
        "a",
        Array("test1", "Johnny", "cool"),
        Array("test1_a", "Johnny_a", "cool_a"),
        Array("test1_01", "Johnny_01", "cool_01")),
      Row(
        2,
        "b",
        Array("test2", "KK", "chill"),
        Array("test2_a", "KK_a", "chill_a"),
        Array("test2_01", "KK_01", "chill_01")),
      Row(3, "c", Array(), Array(), Array()),
      Row(4, "d", Array(), Array(), Array()),
      Row(
        5,
        "e",
        Array("test3", "Jenny", "cute"),
        Array("test3_a", "Jenny_a", "cute_a"),
        Array("test3_01", "Jenny_01", "cute_01"))
    )
    var dExplodeDataParquet = spark.createDataFrame(rowExplodeTestData.asJava, explodeTestSchema)
    dExplodeDataParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)
    spark.catalog.createTable("table2", parquetPath, fileFormat)
  }
  protected def createDataExplodeTable(): Unit = {
    explodeDataPath = Files.createTempDirectory("bolt-explode-fixture").toFile
    val schema = StructType(
      Seq(StructField("volc_account_id", StringType, nullable = true)))
    val rows = Seq(
      Row("2000004577,1"),
      Row("2,2000004577"),
      Row("2000004577,2000004577"),
      Row(""),
      Row(null))
    spark.createDataFrame(rows.asJava, schema)
      .coalesce(1)
      .write.mode("overwrite").parquet(explodeDataPath.getAbsolutePath)
    spark.read.parquet(explodeDataPath.getAbsolutePath).createOrReplaceTempView("table1")
  }

  override protected def afterAll(): Unit = {
    try {
      if (explodeDataPath != null) {
        FileUtils.deleteDirectory(explodeDataPath)
      }
    } finally {
      super.afterAll()
    }
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "10M")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.planChangeLog.level", "warn")
      .set("spark.gluten.sql.columnar.project.pushdown", "true")
      .set("spark.gluten.sql.columnar.project.remove", "true")
      .set(
        "spark.sql.optimizer.excludedRules",
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation," +
          "org.apache.spark.sql.catalyst.optimizer.InferFiltersFromGenerate,"
      )
  }
  test("Pushdown project before generator") {
    runQueryAndCompare(
      s"select max(col3),sum(l_2) from " +
        s"(select col3,COALESCE(cast(col2 as double)) as l_2 from values('1,2,3,4',2) " +
        s"lateral view explode(split(col1, ',')) b as col3)") {
      df => assert(getExecutedPlan(df).count(p => p.isInstanceOf[ProjectExecTransformer]) == 2)
    }
    runQueryAndCompare(
      s"select col1,col3,col2,col2 from (select col1,COALESCE(cast(col2 as double),0) " +
        s"as col2 ,col3 from values('1,2,3,4',2) " +
        s"lateral view explode(split(col1, ',')) b as col3)") {
      df => assert(getExecutedPlan(df).count(p => p.isInstanceOf[ProjectExecTransformer]) == 2)
    }
  }
  test("Remove project before generator") {
    val query =
      s"select * from (select col2 as id,col3 as l_col3 from values('1,2,3,4',2) ,('1,2,3,4',1)" +
        s"lateral view explode(split(col1, ',')) b as col3 )" +
        s"cross join (select col1,col2 as id_2 from values('1','2'))"
    for (enabled <- Seq(false, true)) {
      withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_REMOVE.key -> enabled.toString) {
        runQueryAndCompare(query) {
          df =>
            val plan = getExecutedPlan(df)
            val generators = plan.collect { case g: GenerateExecTransformer => g }
            assert(generators.size == 1)
            val generate = generators.head
            val projectsAboveGenerate = plan.collect {
              case p @ ProjectExecTransformer(_, _: GenerateExecTransformer) => p
            }
            // Removing the output aliases can require an input projection to bind renamed
            // child columns. Check the target projection, not the whole query's project count.
            if (enabled) {
              assert(projectsAboveGenerate.isEmpty)
              assert(generate.output.map(_.name) == Seq("id", "l_col3"))
            } else {
              assert(projectsAboveGenerate.size == 1)
              assert(projectsAboveGenerate.head.output.map(_.name) == Seq("id", "l_col3"))
            }
            assert(generate.requiredChildOutput.forall(generate.child.outputSet.contains))
            assert(generate.generator.references.subsetOf(generate.child.outputSet))
            assert(df.columns.toSeq == Seq("id", "l_col3", "col1", "id_2"))
        }
      }
    }
  }
  test("Pushdown project before filter and generator") {
    runQueryAndCompare(
      s"select * from (select cast(col2 as bigint) as id,col3 as l_col3 from values('1,2,3,4',2)," +
        s"('1,2,3,4',1) lateral view explode(split(col1, ',')) b as col3  where col3 = 2 )" +
        s"cross join (select col1,col2 as id_2 from values('1','2'))") {
      df => assert(getExecutedPlan(df).count(p => p.isInstanceOf[ProjectExecTransformer]) == 2)
    }
  }
  test("Explode expression test1") {
    runQueryAndCompare(
      s"select * from (select * from table1 " +
        s"lateral view explode(split(volc_account_id, ',')) b as volc_account_id_new) " +
        s"where volc_account_id_new = '2000004577'") { _ => }
  }
  test("Explode expression test2 with custom dataset") {
    runQueryAndCompare(
      s"select c3, vid from table2 lateral " +
        s"view explode(c3) tmp as vid where vid is null") { _ => }
  }

  private def withJsonTupleTable(f: => Unit): Unit = {
    withTempPath {
      path =>
        withTable("json_tuple_input") {
          val schema = new StructType().add("id", IntegerType).add("json_column", StringType)
          val rows = Seq(
            Row(1, """{"a":"value","b":"second"}"""),
            Row(2, """{"a":null}"""),
            Row(3, "{}"),
            Row(4, null),
            Row(5, "invalid json"),
            Row(6, "null"))
          withSQLConf("spark.gluten.enabled" -> "false") {
            spark.createDataFrame(rows.asJava, schema).coalesce(1).write.parquet(path.toString)
          }
          spark.catalog.createTable("json_tuple_input", path.toString, "parquet")
          f
        }
    }
  }

  for (outer <- Seq(false, true)) {
    val outerSql = if (outer) "OUTER" else ""
    test(s"file json_tuple outer=$outer preserves rows with unused generator output") {
      withJsonTupleTable {
        runQueryAndCompare(
          s"""SELECT count(*) FROM json_tuple_input
             |LATERAL VIEW $outerSql json_tuple(json_column, 'a') jt AS a""".stripMargin) {
          df =>
            checkGlutenPlan[GenerateExecTransformer](df)
            checkAnswer(df, Seq(Row(6L)))
        }
      }
    }

    test(s"file json_tuple outer=$outer unpacks fields and preserves child columns") {
      withJsonTupleTable {
        runQueryAndCompare(
          s"""SELECT id, a, b FROM json_tuple_input
             |LATERAL VIEW $outerSql json_tuple(json_column, 'a', 'b') jt AS a, b""".stripMargin) {
          df =>
            checkGlutenPlan[GenerateExecTransformer](df)
            checkAnswer(
              df,
              Seq(
                Row(1, "value", "second"),
                Row(2, null, null),
                Row(3, null, null),
                Row(4, null, null),
                Row(5, null, null),
                Row(6, null, null)))
        }
      }
    }
  }

  test("Test with Continuously explode") {
    runQueryAndCompare(
      s"select t_c3, t_c4, t_c5 from (" +
        s"select c3,c4,c5 from table2) t " +
        s"lateral view explode(c3) tmp as t_c3 " +
        s"lateral view explode(c4) tmp as t_c4 " +
        s"lateral view explode(c5) tmp as t_c5")(checkGlutenPlan[GenerateExecTransformer])
    runQueryAndCompare(
      s"select t_c3, t_c5 from(" +
        s"select c3,c4,c5 from table2) t " +
        s"lateral view explode(c3) tmp as t_c3 " +
        s"lateral view explode(c4) tmp as t_c4 " +
        s"lateral view explode(c5) tmp as t_c5")(checkGlutenPlan[GenerateExecTransformer])
    runQueryAndCompare(
      s"select t_c3, t_c4 from(" +
        s"select c3,c4,c5 from table2) t " +
        s"lateral view explode(c3) tmp as t_c3 " +
        s"lateral view explode(c4) tmp as t_c4 " +
        s"lateral view explode(c5) tmp as t_c5")(checkGlutenPlan[GenerateExecTransformer])
    runQueryAndCompare(
      s"select t_c3, t_c4, t_c6 from(" +
        s"select c3,c4,c5 from table2) t " +
        s"lateral view explode(c3) tmp as t_c3 " +
        " lateral view explode(c3) tmp as t_c6 " +
        s"lateral view explode(c4) tmp as t_c4 " +
        s"lateral view explode(c5) tmp as t_c5")(checkGlutenPlan[GenerateExecTransformer])
  }
}
