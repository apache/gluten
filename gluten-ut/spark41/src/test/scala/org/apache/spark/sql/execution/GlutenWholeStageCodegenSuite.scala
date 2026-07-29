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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, GlutenSQLTestsTrait, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.reflect.ClassTag

class GlutenWholeStageCodegenSuite extends WholeStageCodegenSuite with GlutenSQLTestsTrait {
  import testImplicits._

  // Spark's tests inspect WholeStageCodegenExec and row-based operators. Gluten replaces them
  // with WholeStageTransformer and native operator transformers, so the excluded parent tests
  // are repeated below with Gluten-aware plan assertions while preserving their result checks.
  // Disable the forced shuffled hash join rewrite so explicit join hints retain their semantics.
  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "false")
  }

  private def assertWholeStageCount[T <: SparkPlan: ClassTag](
      df: Dataset[_],
      expectedCount: Int): Unit = {
    val targetClass = implicitly[ClassTag[T]].runtimeClass
    val plan = df.queryExecution.executedPlan
    val stages = plan.collect {
      case stage: WholeStageTransformer if stage.child.exists(targetClass.isInstance) => stage
    }
    assert(
      stages.size === expectedCount,
      s"Expected $expectedCount WholeStageTransformer stage(s) containing " +
        s"${targetClass.getSimpleName}, but found ${stages.size}:\n${plan.treeString}"
    )
  }

  private def assertWholeStageContains[T <: SparkPlan: ClassTag](df: Dataset[_]): Unit = {
    val targetClass = implicitly[ClassTag[T]].runtimeClass
    val plan = df.queryExecution.executedPlan
    assert(
      plan.exists {
        case stage: WholeStageTransformer => stage.child.exists(targetClass.isInstance)
        case _ => false
      },
      s"Expected a WholeStageTransformer containing ${targetClass.getSimpleName}:\n" +
        plan.treeString
    )
  }

  private def assertWholeStage(df: Dataset[_]): Unit = {
    assert(df.queryExecution.executedPlan.exists(_.isInstanceOf[WholeStageTransformer]))
  }

  private def assertShuffledJoinStageCount(
      df: Dataset[_],
      hint: String,
      expectedCount: Int): Unit = {
    if (hint == "SHUFFLE_HASH") {
      assertWholeStageCount[ShuffledHashJoinExecTransformer](df, expectedCount)
    } else {
      assertWholeStageCount[SortMergeJoinExecTransformer](df, expectedCount)
    }
  }

  testGluten("range/filter should be combined") {
    val df = spark.range(10).filter("id = 1").selectExpr("id + 1")
    assertWholeStage(df)
    checkAnswer(df, Row(2))
  }

  testGluten("HashAggregate should be included in WholeStageCodegen") {
    val df = spark.range(10).agg(max(col("id")), avg(col("id")))
    assertWholeStageContains[RegularHashAggregateExecTransformer](df)
    checkAnswer(df, Row(9, 4.5))
  }

  testGluten("SortAggregate should be included in WholeStageCodegen") {
    withSQLConf("spark.sql.test.forceApplySortAggregate" -> "true") {
      val df = spark.range(10).agg(max(col("id")), avg(col("id")))
      assertWholeStageContains[SortHashAggregateExecTransformer](df)
      checkAnswer(df, Row(9, 4.5))
    }
  }

  testGluten("GenerateExec should be included in WholeStageCodegen (whole-stage-codegen on)") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      val data = Seq(("James", Seq("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")))
        .toDF("name", "knownLanguages", "properties")

      assertWholeStageContains[GenerateExecTransformer](
        data.select($"name", explode($"knownLanguages"), $"properties"))
      checkAnswer(
        data.select($"name", explode($"knownLanguages"), $"properties"),
        Seq(
          Row("James", "Java", Map("hair" -> "black", "eye" -> "brown")),
          Row("James", "Scala", Map("hair" -> "black", "eye" -> "brown")))
      )
      checkAnswer(
        data.select($"name", $"knownLanguages", explode($"properties")),
        Seq(
          Row("James", Seq("Java", "Scala"), "hair", "black"),
          Row("James", Seq("Java", "Scala"), "eye", "brown"))
      )
      checkAnswer(
        data.select($"name", posexplode($"knownLanguages")),
        Seq(Row("James", 0, "Java"), Row("James", 1, "Scala")))
      checkAnswer(
        data.select($"name", posexplode($"properties")),
        Seq(Row("James", 0, "hair", "black"), Row("James", 1, "eye", "brown")))
      checkAnswer(
        data.select($"*", explode($"knownLanguages")),
        Seq(
          Row(
            "James",
            Seq("Java", "Scala"),
            Map("hair" -> "black", "eye" -> "brown"),
            "Java"),
          Row(
            "James",
            Seq("Java", "Scala"),
            Map("hair" -> "black", "eye" -> "brown"),
            "Scala")
        )
      )
      checkAnswer(
        data.select($"*", explode($"properties")),
        Seq(
          Row(
            "James",
            Seq("Java", "Scala"),
            Map("hair" -> "black", "eye" -> "brown"),
            "hair",
            "black"),
          Row(
            "James",
            Seq("Java", "Scala"),
            Map("hair" -> "black", "eye" -> "brown"),
            "eye",
            "brown")
        )
      )
    }
  }

  testGluten("HashAggregate with grouping keys should be included in WholeStageCodegen") {
    val df = spark.range(3).groupBy(col("id") * 2).count().orderBy(col("id") * 2)
    assertWholeStageContains[RegularHashAggregateExecTransformer](df)
    checkAnswer(df, Seq(Row(0, 1), Row(2, 1), Row(4, 1)))
  }

  testGluten("BroadcastHashJoin should be included in WholeStageCodegen") {
    val rows = spark.sparkContext.makeRDD(Seq(Row(1, "1"), Row(1, "1"), Row(2, "2")))
    val schema = new StructType().add("k", IntegerType).add("v", StringType)
    val smallDF = spark.createDataFrame(rows, schema)
    val df = spark.range(10).join(broadcast(smallDF), col("k") === col("id"))
    assertWholeStageContains[BroadcastHashJoinExecTransformer](df)
    checkAnswer(df, Seq(Row(1, 1, "1"), Row(1, 1, "1"), Row(2, 2, "2")))
  }

  testGluten("Inner ShuffledHashJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(5).select($"id".as("k1"))
    val df2 = spark.range(15).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    val oneJoinDF = df1.join(df2.hint("SHUFFLE_HASH"), $"k1" === $"k2")
    assertWholeStageCount[ShuffledHashJoinExecTransformer](oneJoinDF, expectedCount = 1)
    checkAnswer(oneJoinDF, (0L until 5).map(i => Row(i, i)))

    val twoJoinsDF = oneJoinDF.join(df3.hint("SHUFFLE_HASH"), $"k1" === $"k3")
    // Both native joins are collapsed into the same WholeStageTransformer.
    assertWholeStageCount[ShuffledHashJoinExecTransformer](twoJoinsDF, expectedCount = 1)
    checkAnswer(twoJoinsDF, (0L until 5).map(i => Row(i, i, i)))
  }

  testGluten(
    "Full Outer ShuffledHashJoin and SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(5).select($"id".as("k1"))
    val df2 = spark.range(10).select($"id".as("k2"))
    val df3 = spark.range(3).select($"id".as("k3"))

    Seq("SHUFFLE_HASH", "SHUFFLE_MERGE").foreach {
      hint =>
        val joinUniqueDF = df1.join(df2.hint(hint), $"k1" === $"k2", "full_outer")
        assertShuffledJoinStageCount(joinUniqueDF, hint, expectedCount = 1)
        checkAnswer(
          joinUniqueDF,
          Seq(
            Row(0, 0),
            Row(1, 1),
            Row(2, 2),
            Row(3, 3),
            Row(4, 4),
            Row(null, 5),
            Row(null, 6),
            Row(null, 7),
            Row(null, 8),
            Row(null, 9)))

        val joinNonUniqueDF = df1.join(df2.hint(hint), $"k1" === $"k2" % 3, "full_outer")
        assertShuffledJoinStageCount(joinNonUniqueDF, hint, expectedCount = 1)
        checkAnswer(
          joinNonUniqueDF,
          Seq(
            Row(0, 0),
            Row(0, 3),
            Row(0, 6),
            Row(0, 9),
            Row(1, 1),
            Row(1, 4),
            Row(1, 7),
            Row(2, 2),
            Row(2, 5),
            Row(2, 8),
            Row(3, null),
            Row(4, null)))

        val joinWithNonEquiDF = df1.join(
          df2.hint(hint),
          $"k1" === $"k2" % 3 && $"k1" + 3 =!= $"k2",
          "full_outer")
        assertShuffledJoinStageCount(joinWithNonEquiDF, hint, expectedCount = 1)
        checkAnswer(
          joinWithNonEquiDF,
          Seq(
            Row(0, 0),
            Row(0, 6),
            Row(0, 9),
            Row(1, 1),
            Row(1, 7),
            Row(2, 2),
            Row(2, 8),
            Row(3, null),
            Row(4, null),
            Row(null, 3),
            Row(null, 4),
            Row(null, 5)))

        val twoJoinsDF = joinUniqueDF
          .join(df3.hint(hint), $"k1" === $"k3" && $"k1" + $"k3" =!= 2, "full_outer")
        assertShuffledJoinStageCount(twoJoinsDF, hint, expectedCount = 2)
        checkAnswer(
          twoJoinsDF,
          Seq(
            Row(0, 0, 0),
            Row(1, 1, null),
            Row(2, 2, 2),
            Row(3, 3, null),
            Row(4, 4, null),
            Row(null, 5, null),
            Row(null, 6, null),
            Row(null, 7, null),
            Row(null, 8, null),
            Row(null, 9, null),
            Row(null, null, 1)
          )
        )
    }
  }

  testGluten("SPARK-44060 Code-gen for build side outer shuffled hash join") {
    val df1 = spark.range(0, 5).select($"id".as("k1"))
    val df2 = spark.range(1, 11).select($"id".as("k2"))
    val df3 = spark.range(2, 5).select($"id".as("k3"))

    withSQLConf(SQLConf.ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN.key -> "true") {
      Seq("SHUFFLE_HASH", "SHUFFLE_MERGE").foreach {
        hint =>
          val rightJoinUniqueDf = df1.join(df2.hint(hint), $"k1" === $"k2", "right_outer")
          assertShuffledJoinStageCount(rightJoinUniqueDf, hint, expectedCount = 1)
          checkAnswer(
            rightJoinUniqueDf,
            Seq(
              Row(1, 1),
              Row(2, 2),
              Row(3, 3),
              Row(4, 4),
              Row(null, 5),
              Row(null, 6),
              Row(null, 7),
              Row(null, 8),
              Row(null, 9),
              Row(null, 10)))

          val leftJoinUniqueDf = df1.hint(hint).join(df2, $"k1" === $"k2", "left_outer")
          assertShuffledJoinStageCount(leftJoinUniqueDf, hint, expectedCount = 1)
          checkAnswer(
            leftJoinUniqueDf,
            Seq(Row(0, null), Row(1, 1), Row(2, 2), Row(3, 3), Row(4, 4)))

          val rightJoinNonUniqueDf =
            df1.join(df2.hint(hint), $"k1" === $"k2" % 3, "right_outer")
          assertShuffledJoinStageCount(rightJoinNonUniqueDf, hint, expectedCount = 1)
          checkAnswer(
            rightJoinNonUniqueDf,
            Seq(
              Row(0, 3),
              Row(0, 6),
              Row(0, 9),
              Row(1, 1),
              Row(1, 4),
              Row(1, 7),
              Row(1, 10),
              Row(2, 2),
              Row(2, 5),
              Row(2, 8)))

          val leftJoinNonUniqueDf =
            df1.hint(hint).join(df2, $"k1" === $"k2" % 3, "left_outer")
          assertShuffledJoinStageCount(leftJoinNonUniqueDf, hint, expectedCount = 1)
          checkAnswer(
            leftJoinNonUniqueDf,
            Seq(
              Row(0, 3),
              Row(0, 6),
              Row(0, 9),
              Row(1, 1),
              Row(1, 4),
              Row(1, 7),
              Row(1, 10),
              Row(2, 2),
              Row(2, 5),
              Row(2, 8),
              Row(3, null),
              Row(4, null)))

          val rightJoinWithNonEquiDf = df1.join(
            df2.hint(hint),
            $"k1" === $"k2" % 3 && $"k1" + 3 =!= $"k2",
            "right_outer")
          assertShuffledJoinStageCount(rightJoinWithNonEquiDf, hint, expectedCount = 1)
          checkAnswer(
            rightJoinWithNonEquiDf,
            Seq(
              Row(0, 6),
              Row(0, 9),
              Row(1, 1),
              Row(1, 7),
              Row(1, 10),
              Row(2, 2),
              Row(2, 8),
              Row(null, 3),
              Row(null, 4),
              Row(null, 5)))

          val leftJoinWithNonEquiDf = df1.hint(hint).join(
            df2,
            $"k1" === $"k2" % 3 && $"k1" + 3 =!= $"k2",
            "left_outer")
          assertShuffledJoinStageCount(leftJoinWithNonEquiDf, hint, expectedCount = 1)
          checkAnswer(
            leftJoinWithNonEquiDf,
            Seq(
              Row(0, 6),
              Row(0, 9),
              Row(1, 1),
              Row(1, 7),
              Row(1, 10),
              Row(2, 2),
              Row(2, 8),
              Row(3, null),
              Row(4, null)))

          val twoRightJoinsDf = rightJoinUniqueDf
            .join(df3.hint(hint), $"k1" === $"k3" && $"k1" + $"k3" =!= 2, "right_outer")
          // Both native joins are collapsed into the same WholeStageTransformer.
          assertShuffledJoinStageCount(twoRightJoinsDf, hint, expectedCount = 1)
          checkAnswer(twoRightJoinsDf, Seq(Row(2, 2, 2), Row(3, 3, 3), Row(4, 4, 4)))

          val twoLeftJoinsDf = leftJoinUniqueDf
            .hint(hint)
            .join(df3, $"k1" === $"k3" && $"k1" + $"k3" =!= 2, "left_outer")
          // Both native joins are collapsed into the same WholeStageTransformer.
          assertShuffledJoinStageCount(twoLeftJoinsDf, hint, expectedCount = 1)
          checkAnswer(
            twoLeftJoinsDf,
            Seq(
              Row(0, null, null),
              Row(1, 1, null),
              Row(2, 2, 2),
              Row(3, 3, 3),
              Row(4, 4, 4)))
      }
    }
  }

  testGluten("Left/Right Outer SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(10).select($"id".as("k1"))
    val df2 = spark.range(4).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    val leftJoin = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", "left_outer")
    assertWholeStageCount[SortMergeJoinExecTransformer](leftJoin, expectedCount = 1)
    checkAnswer(
      leftJoin,
      Seq(
        Row(0, 0),
        Row(1, 1),
        Row(2, 2),
        Row(3, 3),
        Row(4, null),
        Row(5, null),
        Row(6, null),
        Row(7, null),
        Row(8, null),
        Row(9, null)))

    val rightJoin = df2.join(df3.hint("SHUFFLE_MERGE"), $"k2" === $"k3", "right_outer")
    assertWholeStageCount[SortMergeJoinExecTransformer](rightJoin, expectedCount = 1)
    checkAnswer(
      rightJoin,
      Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(3, 3), Row(null, 4), Row(null, 5)))

    val twoJoins = df3
      .join(df2.hint("SHUFFLE_MERGE"), $"k3" === $"k2", "left_outer")
      .join(df1.hint("SHUFFLE_MERGE"), $"k3" === $"k1", "right_outer")
    // Both native joins are collapsed into the same WholeStageTransformer.
    assertWholeStageCount[SortMergeJoinExecTransformer](twoJoins, expectedCount = 1)
    checkAnswer(
      twoJoins,
      Seq(
        Row(0, 0, 0),
        Row(1, 1, 1),
        Row(2, 2, 2),
        Row(3, 3, 3),
        Row(4, null, 4),
        Row(5, null, 5),
        Row(null, null, 6),
        Row(null, null, 7),
        Row(null, null, 8),
        Row(null, null, 9))
    )
  }

  testGluten("Left Semi SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(10).select($"id".as("k1"))
    val df2 = spark.range(4).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    val oneJoin = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", "left_semi")
    assertWholeStageCount[SortMergeJoinExecTransformer](oneJoin, expectedCount = 1)
    checkAnswer(oneJoin, (0L until 4).map(Row(_)))

    val twoJoins = df3
      .join(df2.hint("SHUFFLE_MERGE"), $"k3" === $"k2", "left_semi")
      .join(df1.hint("SHUFFLE_MERGE"), $"k3" === $"k1", "left_semi")
    // Both native joins are collapsed into the same WholeStageTransformer.
    assertWholeStageCount[SortMergeJoinExecTransformer](twoJoins, expectedCount = 1)
    checkAnswer(twoJoins, (0L until 4).map(Row(_)))
  }

  testGluten("Left Anti SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(10).select($"id".as("k1"))
    val df2 = spark.range(4).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    val oneJoin = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", "left_anti")
    assertWholeStageCount[SortMergeJoinExecTransformer](oneJoin, expectedCount = 1)
    checkAnswer(oneJoin, (4L until 10).map(Row(_)))

    val twoJoins = oneJoin.join(df3.hint("SHUFFLE_MERGE"), $"k1" === $"k3", "left_anti")
    // Both native joins are collapsed into the same WholeStageTransformer.
    assertWholeStageCount[SortMergeJoinExecTransformer](twoJoins, expectedCount = 1)
    checkAnswer(twoJoins, (6L until 10).map(Row(_)))
  }

  testGluten("Inner/Cross BroadcastNestedLoopJoinExec should be included in WholeStageCodegen") {
    val df1 = spark.range(4).select($"id".as("k1"))
    val df2 = spark.range(3).select($"id".as("k2"))
    val df3 = spark.range(2).select($"id".as("k3"))

    // Gluten's native whole-stage transformation is independent of Spark's Java codegen switch.
    Seq(true, false).foreach {
      codegenEnabled =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            df1.join(df2),
            expectedCount = 1)
          checkAnswer(
            df1.join(df2),
            Seq(
              Row(0, 0),
              Row(0, 1),
              Row(0, 2),
              Row(1, 0),
              Row(1, 1),
              Row(1, 2),
              Row(2, 0),
              Row(2, 1),
              Row(2, 2),
              Row(3, 0),
              Row(3, 1),
              Row(3, 2)))
          val conditionalJoin = df1.join(df2, $"k1" + 1 =!= $"k2")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            conditionalJoin,
            expectedCount = 1)
          checkAnswer(
            conditionalJoin,
            Seq(
              Row(0, 0),
              Row(0, 2),
              Row(1, 0),
              Row(1, 1),
              Row(2, 0),
              Row(2, 1),
              Row(2, 2),
              Row(3, 0),
              Row(3, 1),
              Row(3, 2)))
          val twoJoins = df1.join(df2, $"k1" < $"k2").crossJoin(df3)
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            twoJoins,
            expectedCount = 1)
          checkAnswer(
            twoJoins,
            Seq(
              Row(0, 1, 0),
              Row(0, 2, 0),
              Row(1, 2, 0),
              Row(0, 1, 1),
              Row(0, 2, 1),
              Row(1, 2, 1)))
        }
    }
  }

  testGluten(
    "Left/Right outer BroadcastNestedLoopJoinExec should be included in WholeStageCodegen") {
    val df1 = spark.range(4).select($"id".as("k1"))
    val df2 = spark.range(3).select($"id".as("k2"))
    val df3 = spark.range(2).select($"id".as("k3"))
    val empty = spark.range(0).select($"id".as("k4"))

    // Gluten's native whole-stage transformation is independent of Spark's Java codegen switch.
    Seq(true, false).foreach {
      codegenEnabled =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
          val leftOuterJoin = df1.join(df2, $"k1" > $"k2", "left_outer")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            leftOuterJoin,
            expectedCount = 1)
          checkAnswer(
            leftOuterJoin,
            Seq(
              Row(0, null),
              Row(1, 0),
              Row(2, 0),
              Row(2, 1),
              Row(3, 0),
              Row(3, 1),
              Row(3, 2)))
          val rightOuterJoin = df1.join(df2, $"k1" < $"k2", "right_outer")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            rightOuterJoin,
            expectedCount = 1)
          checkAnswer(
            rightOuterJoin,
            Seq(Row(null, 0), Row(0, 1), Row(0, 2), Row(1, 2)))
          val twoJoins = df1
            .join(df2, $"k1" > $"k2" + 1, "right_outer")
            .join(df3, $"k1" <= $"k3", "left_outer")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            twoJoins,
            expectedCount = 1)
          checkAnswer(
            twoJoins,
            Seq(Row(2, 0, null), Row(3, 0, null), Row(3, 1, null), Row(null, 2, null)))
          val emptyBuildSide = df3.join(empty, $"k3" > $"k4", "left_outer")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            emptyBuildSide,
            expectedCount = 1)
          checkAnswer(
            emptyBuildSide,
            Seq(Row(0, null), Row(1, null)))
        }
    }
  }

  testGluten("Left semi/anti BroadcastNestedLoopJoinExec should be included in WholeStageCodegen") {
    val df1 = spark.range(4).select($"id".as("k1"))
    val df2 = spark.range(3).select($"id".as("k2"))
    val df3 = spark.range(2).select($"id".as("k3"))

    // Velox does not support native left semi/anti broadcast nested loop joins, so these plans
    // fall back without a VeloxBroadcastNestedLoopJoinExecTransformer.
    Seq(true, false).foreach {
      codegenEnabled =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
          val semiJoin = df1.join(df2, $"k1" + 1 <= $"k2", "left_semi")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            semiJoin,
            expectedCount = 0)
          checkAnswer(semiJoin, Seq(Row(0), Row(1)))

          val antiJoin = df1.join(df2, $"k1" + 1 <= $"k2", "left_anti")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            antiJoin,
            expectedCount = 0)
          checkAnswer(antiJoin, Seq(Row(2), Row(3)))

          val twoJoins = df1
            .join(df2, $"k1" < $"k2", "left_semi")
            .join(df3, $"k1" > $"k3", "left_anti")
          assertWholeStageCount[VeloxBroadcastNestedLoopJoinExecTransformer](
            twoJoins,
            expectedCount = 0)
          checkAnswer(twoJoins, Row(0))
        }
    }
  }

  testGluten("Sort should be included in WholeStageCodegen") {
    val df = spark.range(3, 0, -1).toDF().sort(col("id"))
    assertWholeStageContains[SortExecTransformer](df)
    checkAnswer(df, Seq(Row(1), Row(2), Row(3)))
  }

  testGluten("Control splitting consume function by operators with config") {
    val df = spark.range(10).select(Seq.tabulate(2)(i => ($"id" + i).as(s"c$i")): _*)

    Seq(true, false).foreach {
      config =>
        withSQLConf(SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> config.toString) {
          assertWholeStage(df)
          checkAnswer(df, (0L until 10).map(i => Row(i, i + 1)))
        }
    }
  }

  testGluten("Skip splitting consume function when parameter number exceeds JVM limit") {
    Seq(128, 127).foreach {
      columnNum =>
        withTempPath {
          dir =>
            val path = dir.getCanonicalPath
            spark
              .range(10)
              .select(Seq.tabulate(columnNum)(i => lit(i).as(s"c$i")): _*)
              .write
              .mode(SaveMode.Overwrite)
              .parquet(path)

            withSQLConf(
              SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "255",
              SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> "true") {
              val projection = Seq.tabulate(columnNum)(i => s"c$i + c$i as newC$i")
              val df = spark.read.parquet(path).selectExpr(projection: _*)
              assertWholeStage(df)
              val expected = Seq.fill(10)(Row.fromSeq((0 until columnNum).map(_ * 2)))
              checkAnswer(df, expected)
            }
        }
    }
  }

  testGluten(
    "including codegen stage ID in generated class name should not regress codegen caching") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_USE_ID_IN_CLASS_NAME.key -> "true") {
      val df1 = spark.range(3).select($"id" + 2)
      val df2 = spark.range(3).select($"id" + 2)
      assertWholeStage(df1)
      assertWholeStage(df2)
      checkAnswer(df1, Seq(Row(2), Row(3), Row(4)))
      checkAnswer(df2, df1)
    }
  }

  testGluten("SPARK-26572: evaluate non-deterministic expressions for aggregate results") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val baseTable = Seq(1, 1).toDF("idx")

      val distinctWithId = baseTable
        .distinct()
        .withColumn("id", monotonically_increasing_id())
        .join(baseTable, "idx")
      assertWholeStageContains[BroadcastHashJoinExecTransformer](distinctWithId)
      assertWholeStageContains[RegularHashAggregateExecTransformer](distinctWithId)
      checkAnswer(distinctWithId, Seq(Row(1, 0), Row(1, 0)))

      val groupByWithId = baseTable
        .groupBy("idx")
        .sum()
        .withColumn("id", monotonically_increasing_id())
        .join(baseTable, "idx")
      assertWholeStageContains[BroadcastHashJoinExecTransformer](groupByWithId)
      assertWholeStageContains[RegularHashAggregateExecTransformer](groupByWithId)
      checkAnswer(groupByWithId, Seq(Row(1, 2, 0), Row(1, 2, 0)))
    }
  }

  testGluten("SPARK-28520: WholeStageCodegen does not work properly for LocalTableScanExec") {
    val localScan = Seq(1, 2, 3).toDF()
    assert(localScan.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])
    checkAnswer(localScan, Seq(Row(1), Row(2), Row(3)))

    val aggregate = localScan.groupBy("value").sum()
    assertWholeStageContains[RegularHashAggregateExecTransformer](aggregate)
    checkAnswer(aggregate, Seq(Row(1, 1), Row(2, 2), Row(3, 3)))
  }

  testGluten("Give up splitting aggregate code if a parameter length goes over the limit") {
    withSQLConf(
      SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC.key -> "true",
      SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1",
      "spark.sql.CodeGenerator.validParamLength" -> "0") {
      checkAnswer(sql("SELECT AVG(v) FROM VALUES(1) t(v)"), Row(1.0))
      checkAnswer(
        sql("SELECT k, AVG(v) FROM VALUES((1, 1)) t(k, v) GROUP BY k"),
        Row(1, 1.0))
    }
  }

  testGluten("Give up splitting subexpression code if a parameter length goes over the limit") {
    withSQLConf(
      SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC.key -> "false",
      SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1",
      "spark.sql.CodeGenerator.validParamLength" -> "0") {
      checkAnswer(
        sql("SELECT AVG(a + b), SUM(a + b + c) FROM VALUES((1, 1, 1)) t(a, b, c)"),
        Row(2.0, 3))
      checkAnswer(
        sql(
          "SELECT k, AVG(a + b), SUM(a + b + c) " +
            "FROM VALUES((1, 1, 1, 1)) t(k, a, b, c) GROUP BY k"),
        Row(1, 2.0, 3))
    }
  }

  testGluten("SPARK-47238: Test broadcast threshold for generated code") {
    Seq(-1, 1000000000, 0).foreach {
      threshold =>
        withSQLConf(
          SQLConf.WHOLESTAGE_BROADCAST_CLEANED_SOURCE_THRESHOLD.key -> threshold.toString,
          SQLConf.USE_PARTITION_EVALUATOR.key -> "true") {
          val df = Seq(0, 1, 2).toDF().groupBy("value").sum()
          assertWholeStageContains[RegularHashAggregateExecTransformer](df)
          checkAnswer(df, Seq(Row(0, 0), Row(1, 1), Row(2, 2)))
        }
    }
  }
}
