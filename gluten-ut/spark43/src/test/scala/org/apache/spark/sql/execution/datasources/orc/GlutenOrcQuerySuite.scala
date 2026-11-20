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
package org.apache.spark.sql.execution.datasources.orc

import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

class GlutenOrcQuerySuite extends OrcQuerySuite with GlutenSQLTestsBaseTrait {
  testGluten("Simple selection form ORC table") {
    val data = (1 to 10).map {
      i => Person(s"name_$i", i, (0 to 1).map(m => Contact(s"contact_$m", s"phone_$m")))
    }

    withOrcTable(data, "t") {
      withSQLConf("spark.sql.orc.enableVectorizedReader" -> "false") {
        // ppd:
        // leaf-0 = (LESS_THAN_EQUALS age 5)
        // expr = leaf-0
        assert(sql("SELECT name FROM t WHERE age <= 5").count() === 5)

        // ppd:
        // leaf-0 = (LESS_THAN_EQUALS age 5)
        // expr = (not leaf-0)
        assertResult(10) {
          sql("SELECT name, contacts FROM t where age > 5").rdd
            .flatMap(_.getAs[scala.collection.Seq[_]]("contacts"))
            .count()
        }

        // ppd:
        // leaf-0 = (LESS_THAN_EQUALS age 5)
        // leaf-1 = (LESS_THAN age 8)
        // expr = (and (not leaf-0) leaf-1)
        {
          val df = sql("SELECT name, contacts FROM t WHERE age > 5 AND age < 8")
          assert(df.count() === 2)
          assertResult(4) {
            df.rdd.flatMap(_.getAs[scala.collection.Seq[_]]("contacts")).count()
          }
        }

        // ppd:
        // leaf-0 = (LESS_THAN age 2)
        // leaf-1 = (LESS_THAN_EQUALS age 8)
        // expr = (or leaf-0 (not leaf-1))
        {
          val df = sql("SELECT name, contacts FROM t WHERE age < 2 OR age > 8")
          assert(df.count() === 3)
          assertResult(6) {
            df.rdd.flatMap(_.getAs[scala.collection.Seq[_]]("contacts")).count()
          }
        }
      }
    }
  }

  testGluten("simple select queries") {
    withOrcTable((0 until 10).map(i => (i, i.toString)), "t") {
      withSQLConf("spark.sql.orc.enableVectorizedReader" -> "false") {
        checkAnswer(sql("SELECT `_1` FROM t where t.`_1` > 5"), (6 until 10).map(Row.apply(_)))

        checkAnswer(
          sql("SELECT `_1` FROM t as tmp where tmp.`_1` < 5"),
          (0 until 5).map(Row.apply(_)))
      }
    }
  }

  testGluten("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    withOrcTable(data, "t") {
      withSQLConf("spark.sql.orc.enableVectorizedReader" -> "false") {
        sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
        checkAnswer(spark.table("t"), data.map(Row.fromTuple))
      }
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("self-join") {
    // 4 rows, cells of column 1 of row 2 and row 4 are null
    val data = (1 to 4).map {
      i =>
        val maybeInt = if (i % 2 == 0) None else Some(i)
        (maybeInt, i.toString)
    }

    withOrcTable(data, "t") {
      withSQLConf("spark.sql.orc.enableVectorizedReader" -> "false") {
        val selfJoin = sql("SELECT * FROM t x JOIN t y WHERE x.`_1` = y.`_1`")
        val queryOutput = selfJoin.queryExecution.analyzed.output

        assertResult(4, "Field count mismatches")(queryOutput.size)
        assertResult(2, s"Duplicated expression ID in query plan:\n $selfJoin") {
          queryOutput.filter(_.name == "_1").map(_.exprId).size
        }

        checkAnswer(selfJoin, List(Row(1, "1", 1, "1"), Row(3, "3", 3, "3")))
      }
    }
  }

  testGluten("columns only referenced by pushed down filters should remain") {
    withOrcTable((1 to 10).map(Tuple1.apply), "t") {
      withSQLConf("spark.sql.orc.enableVectorizedReader" -> "false") {
        checkAnswer(sql("SELECT `_1` FROM t WHERE `_1` < 10"), (1 to 9).map(Row.apply(_)))
      }
    }
  }

  testGluten("SPARK-5309 strings stored using dictionary compression in orc") {
    withOrcTable((0 until 1000).map(i => ("same", "run_" + i / 100, 1)), "t") {
      withSQLConf("spark.sql.orc.enableVectorizedReader" -> "false") {
        checkAnswer(
          sql("SELECT `_1`, `_2`, SUM(`_3`) FROM t GROUP BY `_1`, `_2`"),
          (0 until 10).map(i => Row("same", "run_" + i, 100)))

        checkAnswer(
          sql("SELECT `_1`, `_2`, SUM(`_3`) FROM t WHERE `_2` = 'run_5' GROUP BY `_1`, `_2`"),
          List(Row("same", "run_5", 100)))
      }
    }
  }
}

class GlutenOrcV1QuerySuite extends GlutenOrcQuerySuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "orc")
}

class GlutenOrcV2QuerySuite extends GlutenOrcQuerySuite {
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
