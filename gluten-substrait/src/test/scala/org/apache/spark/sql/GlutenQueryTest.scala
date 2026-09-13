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

/**
 * Why we need a GlutenQueryTest when we already have QueryTest?
 *   1. We need to modify the way org.apache.spark.sql.CHQueryTest#compare compares double
 */
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{GlutenPlan, TransformSupport, WholeStageTransformer}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan, SQLExecution, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.storage.StorageLevel

import org.junit.Assert
import org.scalatest.Assertions

import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

abstract class GlutenQueryTest extends PlanTest with AdaptiveSparkPlanHelper {

  // TODO: remove this if we can suppress unused import error.
  locally {
    new ColumnConstructorExt(Column)
  }

  protected def spark: SparkSession

  def isSparkVersionGE(minSparkVersion: String): Boolean = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    val minVersion = minSparkVersion.split("\\.")
    minVersion(0) < version(0) || (minVersion(0) == version(0) && minVersion(1) <= version(1))
  }

  def isSparkVersionLE(maxSparkVersion: String): Boolean = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    val maxVersion = maxSparkVersion.split("\\.")
    maxVersion(0) > version(0) || maxVersion(0) == version(0) && maxVersion(1) >= version(1)
  }

  /**
   * Check if the current spark version is between the minVersion and maxVersion.
   *
   * If the maxVersion is not specified, it will check if the current spark version is greater than
   * or equal to, and if the minVersion is not specified, it will check if the current spark version
   * is less than or equal to.
   *
   * @param minSparkVersion
   *   the minimum spark version
   * @param maxSparkVersion
   *   the maximum spark version
   * @return
   *   true if the current spark version is between the minVersion and maxVersion
   */
  def matchSparkVersion(
      minSparkVersion: Option[String] = None,
      maxSparkVersion: Option[String] = None): Boolean = {
    var shouldRun = true
    if (minSparkVersion.isDefined) {
      shouldRun = isSparkVersionGE(minSparkVersion.get)
      if (maxSparkVersion.isDefined) {
        shouldRun = shouldRun && isSparkVersionLE(maxSparkVersion.get)
      }
    } else {
      if (maxSparkVersion.isDefined) {
        shouldRun = isSparkVersionLE(maxSparkVersion.get)
      }
    }
    shouldRun
  }


  /** Run the test if the current spark version less than the maxVersion */
  def testWithMaxSparkVersion(testName: String, maxVersion: String)(testFun: => Any): Unit = {
    if (matchSparkVersion(maxSparkVersion = Some(maxVersion))) {
      test(testName) {
        testFun
      }
    }
  }

  /** Run the test if the current spark version greater than the minVersion */
  def testWithMinSparkVersion(testName: String, minVersion: String)(testFun: => Any): Unit = {
    if (matchSparkVersion(Some(minVersion))) {
      test(testName) {
        testFun
      }
    }
  }

  /** Run the test on the specified spark version */
  def testWithSpecifiedSparkVersion(testName: String, versions: String*)(testFun: => Any): Unit = {
    if (versions.exists(v => matchSparkVersion(Some(v), Some(v)))) {
      test(testName) {
        testFun
      }
    }
  }

  /** Runs the plan and makes sure the answer contains all of the keywords. */
  def checkKeywordsExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
    }
  }

  /** Runs the plan and makes sure the answer does NOT contain any of the keywords. */
  def checkKeywordsNotExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given expected
   * answer.
   */
  protected def checkDataset[T](ds: => Dataset[T], expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!GlutenQueryTest.compare(result.toSeq, expectedAnswer)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
              |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given expected
   * answer, after sort.
   */
  protected def checkDatasetUnorderly[T: Ordering](ds: => Dataset[T], expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!GlutenQueryTest.compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
              |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  private def getResult[T](ds: => Dataset[T]): Array[T] = {
    val analyzedDS =
      try ds
      catch {
        case ae: AnalysisException =>
          val plan = SparkShimLoader.getSparkShims.getAnalysisExceptionPlan(ae)
          if (plan.isDefined) {
            fail(s"""
                    |Failed to analyze query: $ae

                    |${plan.get}

                    |
                    |${stackTraceToString(ae)}
          """.stripMargin)
          } else {
            throw ae
          }
      }
    assertEmptyMissingInput(analyzedDS)

    try ds.collect()
    catch {
      case e: Exception =>
        fail(
          s"""
             |Exception collecting dataset as objects
             |${ds.exprEnc}
             |${ds.exprEnc.deserializer.treeString}
             |${ds.queryExecution}
           """.stripMargin,
          e
        )
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df
   *   the [[DataFrame]] to be executed
   * @param expectedAnswer
   *   the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF =
      try df
      catch {
        case ae: AnalysisException =>
          val plan = SparkShimLoader.getSparkShims.getAnalysisExceptionPlan(ae)
          if (plan.isDefined) {
            fail(s"""
                    |Failed to analyze query: $ae
                    |${plan.get}
                    |
                    |${stackTraceToString(ae)}
                    |""".stripMargin)
          } else {
            throw ae
          }
      }

    assertEmptyMissingInput(analyzedDF)

    GlutenQueryTest.checkAnswer(analyzedDF, expectedAnswer)
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param dataFrame
   *   the [[DataFrame]] to be executed
   * @param expectedAnswer
   *   the expected result in a [[Seq]] of [[Row]]s.
   * @param absTol
   *   the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(
      dataFrame: DataFrame,
      expectedAnswer: Seq[Row],
      absTol: Double): Unit = {
    // TODO: catch exceptions in data frame execution
    val actualAnswer = dataFrame.collect()
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach {
      case (actualRow, expectedRow) =>
        GlutenQueryTest.checkAggregatesWithTol(actualRow, expectedRow, absTol)
    }
  }

  protected def checkAggregatesWithTol(
      dataFrame: DataFrame,
      expectedAnswer: Row,
      absTol: Double): Unit = {
    checkAggregatesWithTol(dataFrame, Seq(expectedAnswer), absTol)
  }

  /** Asserts that a given [[Dataset]] will be executed using the given number of cached results. */
  def assertCached(query: Dataset[_], numCachedTables: Int = 1): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching.collect { case cached: InMemoryRelation => cached }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

  /**
   * Asserts that a given [[Dataset]] will be executed using the cache with the given name and
   * storage level.
   */
  def assertCached(query: Dataset[_], cachedName: String, storageLevel: StorageLevel): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val matched = planWithCaching
      .collectFirst {
        case cached: InMemoryRelation =>
          val cacheBuilder = cached.cacheBuilder
          cachedName == cacheBuilder.tableName.get &&
          (storageLevel == cacheBuilder.storageLevel)
      }
      .getOrElse(false)

    assert(
      matched,
      s"Expected query plan to hit cache $cachedName with storage " +
        s"level $storageLevel, but it doesn't.")
  }

  /** Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans. */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(
      query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(
      query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(
      query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

  def checkLengthAndPlan(df: DataFrame, len: Int = 100): Unit = {
    assert(df.collect().length == len)
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.find(_.isInstanceOf[TransformSupport]).isDefined))
  }

  private def getExecutedPlan(plan: SparkPlan): Seq[SparkPlan] = {
    val subTree = plan match {
      case exec: AdaptiveSparkPlanExec =>
        getExecutedPlan(stripAQEPlan(exec))
      case cmd: CommandResultExec =>
        getExecutedPlan(cmd.commandPhysicalPlan)
      case s: ShuffleQueryStageExec =>
        getExecutedPlan(s.plan)
      case plan =>
        plan.children.flatMap(getExecutedPlan)
    }

    plan match {
      case WholeStageCodegenExec(_) => subTree
      case WholeStageTransformer(_, _) => subTree
      case _ => subTree :+ plan
    }
  }

  /**
   * Get the executed plan of a data frame.
   * @param df:
   *   dataframe.
   * @return
   *   A sequence of executed plans.
   */
  def getExecutedPlan(df: DataFrame): Seq[SparkPlan] = {
    getExecutedPlan(df.queryExecution.executedPlan)
  }

  /**
   * Check whether the executed plan of a dataframe contains the expected plan chain.
   *
   * @param df
   *   : the input dataframe.
   * @param tag
   *   : class of the expected plan.
   * @param childTag
   *   : class of the expected plan's child.
   * @tparam T
   *   : type of the expected plan.
   * @tparam PT
   *   : type of the expected plan's child.
   */
  def checkSparkOperatorChainMatch[T <: UnaryExecNode, PT <: UnaryExecNode](
      df: DataFrame)(implicit tag: ClassTag[T], childTag: ClassTag[PT]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(
      executedPlan.exists(
        plan =>
          tag.runtimeClass.isInstance(plan)
            && childTag.runtimeClass.isInstance(plan.children.head)),
      s"Expect an operator chain of [${tag.runtimeClass.getSimpleName} ->"
        + s"${childTag.runtimeClass.getSimpleName}] exists in executedPlan: \n"
        + s"${executedPlan.last}"
    )
  }

  /**
   * Check whether the executed plan of a dataframe contains the expected plan.
   * @param df:
   *   the input dataframe.
   * @param tag:
   *   class of the expected plan.
   * @tparam T:
   *   type of the expected plan.
   */
  def checkGlutenPlan[T <: GlutenPlan](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(
      executedPlan.exists(plan => tag.runtimeClass.isInstance(plan)),
      s"Expect ${tag.runtimeClass.getSimpleName} exists " +
        s"in executedPlan:\n ${executedPlan.last}"
    )
  }

  def checkSparkPlan[T <: SparkPlan](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => tag.runtimeClass.isInstance(plan)))
  }

  /**
   * Check whether the executed plan of a dataframe contains expected number of expected plans.
   *
   * @param df:
   *   the input dataframe.
   * @param count:
   *   expected number of expected plan.
   * @param tag:
   *   class of the expected plan.
   * @tparam T:
   *   type of the expected plan.
   */
  def checkGlutenPlanCount[T <: GlutenPlan](df: DataFrame, count: Int)(implicit
      tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(
      executedPlan.count(plan => tag.runtimeClass.isInstance(plan)) == count,
      s"Expect $count ${tag.runtimeClass.getSimpleName} " +
        s"in executedPlan:\n ${executedPlan.last}"
    )
  }
}

object GlutenQueryTest extends Assertions {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df
   *   the DataFrame to be executed
   * @param expectedAnswer
   *   the expected result in a Seq of Rows.
   * @param checkToRDD
   *   whether to verify deserialization to an RDD. This runs the query twice.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row], checkToRDD: Boolean = true): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer, checkToRDD) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result. If there was exception
   * during the execution or the contents of the DataFrame does not match the expected result, an
   * error message will be returned. Otherwise, a None will be returned.
   *
   * @param df
   *   the DataFrame to be executed
   * @param expectedAnswer
   *   the expected result in a Seq of Rows.
   * @param checkToRDD
   *   whether to verify deserialization to an RDD. This runs the query twice.
   */
  def getErrorMessageInCheckAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Option[String] = {
    if (checkToRDD) {
      val executionId = getNextExecutionId
      SQLExecution.withExecutionId(df.sparkSession, executionId) {
        df.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
      }
      BackendsApiManager.getTransformerApiInstance.invalidateSQLExecutionResource(executionId)
    }

    val sparkAnswer =
      try df.collect().toSeq
      catch {
        case e: Exception =>
          val errorMessage =
            s"""
               |Exception thrown while executing query:
               |${df.queryExecution}
               |== Exception ==
               |$e
               |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
          return Some(errorMessage)
      }

    val sortedColIdxes = getOuterSortedColIdxes(df)
    sameRows(expectedAnswer, sparkAnswer, sortedColIdxes, df.schema.length).map {
      results =>
        s"""
           |Results do not match for query:
           |Timezone: ${TimeZone.getDefault}
           |Timezone Env: ${sys.env.getOrElse("TZ", "")}
           |
           |${df.queryExecution}
           |== Results ==
           |$results
       """.stripMargin
    }
  }

  def getNextExecutionId: String = {
    val classMirror = universe.runtimeMirror(SQLExecution.getClass.getClassLoader)
    val staticMirror = classMirror.staticModule(SQLExecution.getClass.getName)
    val moduleMirror = classMirror.reflectModule(staticMirror)
    val objectMirror = classMirror.reflect(moduleMirror.instance)
    val strInObjSymbol = objectMirror.symbol.typeSignature
      .member(universe.TermName("nextExecutionId"))
      .asMethod
    val nextExecutionId = objectMirror.reflectMethod(strInObjSymbol)
    val newid = nextExecutionId.apply().toString
    newid
  }

  def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    answer.map(prepareRow)
  }

  def getOuterSortedColIdxes(df: DataFrame): Seq[Int] = {

    def findOuterSortColExprIds(plan: LogicalPlan): Seq[Long] = {
      plan match {
        case s: Sort =>
          s.order.map(_.child match {
            case a: Attribute => a.exprId.id
            case _ => -1
          })
        case _: Join => Nil
        case _: Subquery => Nil
        case _: SubqueryAlias => Nil
        case _ => plan.children.flatMap(child => findOuterSortColExprIds(child))
      }
    }

    val plan = df.queryExecution.optimizedPlan
    val projectColIds: Seq[Long] = plan.output.map(_.exprId.id)
    val sortedColIds: Seq[Long] = findOuterSortColExprIds(plan)

    sortedColIds.map(projectColIds.indexOf(_))
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] =>
        seq.map {
          case b: java.lang.Byte => b.byteValue
          case s: java.lang.Short => s.shortValue
          case i: java.lang.Integer => i.intValue
          case l: java.lang.Long => l.longValue
          case f: java.lang.Float => f.floatValue
          case d: java.lang.Double => d.doubleValue
          case x => x
        }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  private def genError(expected: Seq[Row], actual: Seq[Row]): String = {
    val getRowType: Option[Row] => String = row =>
      row
        .map(
          row =>
            if (row.schema == null) {
              "struct<>"
            } else {
              s"${row.schema.catalogString}"
            })
        .getOrElse("struct<>")

    s"""
       |== Results ==
       |${sideBySide(
        s"== Correct Answer - ${expected.size} ==" +:
          getRowType(expected.headOption) +:
          prepareAnswer(expected).map(_.toString()),
        s"== Gluten Answer - ${actual.size} ==" +:
          getRowType(actual.headOption) +:
          prepareAnswer(actual).map(_.toString())
      ).mkString("\n")}
    """.stripMargin
  }

  def includesRows(expectedRows: Seq[Row], sparkAnswer: Seq[Row]): Option[String] = {
    val expected = prepareAnswer(expectedRows)
    val actual = prepareAnswer(sparkAnswer)
    if (!expected.toSet.subsetOf(actual.toSet)) {
      return Some(genError(expectedRows, sparkAnswer))
    }
    None
  }

  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall {
        aKey => b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) =>
      if (isNaNOrInf(a) || isNaNOrInf(b)) {
        java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
      } else {
        Math.abs(a - b) < 0.00001
      }
    case (a: Float, b: Float) =>
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a: BigDecimal, b: BigDecimal) =>
      val maxDeviation: BigDecimal = BigDecimal(1, math.max(a.scale, b.scale))
      (a - b).abs.compare(maxDeviation) <= 0
    case (a, b) => a == b
  }

  def isNaNOrInf(num: Double): Boolean = {
    num.isNaN || num.isInfinite || num.isNegInfinity || num.isPosInfinity
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      sortedColIdxes: Seq[Int],
      colLength: Int): Option[String] = {
    val expected = prepareAnswer(expectedAnswer)
    val actual = prepareAnswer(sparkAnswer)

    // if answer is fully sorted or there are unknown sorted cols, we can just compare the answer
    if (sortedColIdxes.contains(-1) || sortedColIdxes.length == colLength) {
      if (!compare(expected, actual)) {
        return Some(genError(expected, actual))
      }
      return None
    }

    if (sortedColIdxes.nonEmpty) {
      // if answer is partially sorted, we should compare the sorted part
      val expectedPart = expected.map(row => sortedColIdxes.map(row.get))
      val actualPart = actual.map(row => sortedColIdxes.map(row.get))
      if (!compare(expectedPart, actualPart)) {
        return Some(genError(expected, actual))
      }
    } else {
      // if answer is not fully sorted, we should sort the answer first, then compare them
      val sortedExpected = expected.sortBy(_.toString())
      val sortedActual = actual.sortBy(_.toString())
      if (!compare(sortedExpected, sortedActual)) {
        return Some(genError(sortedExpected, sortedActual))
      }
    }

    None
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param actualAnswer
   *   the actual result in a [[Row]].
   * @param expectedAnswer
   *   the expected result in a[[Row]].
   * @param absTol
   *   the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(actualAnswer: Row, expectedAnswer: Row, absTol: Double) = {
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(
          math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer.asScala.toSeq) match {
      case Some(errorMessage) => Assert.fail(errorMessage)
      case None =>
    }
  }
}

class QueryTestSuite extends QueryTest with test.SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.ui.enabled", "false")

  test("SPARK-16940: checkAnswer should raise TestFailedException for wrong results") {
    intercept[org.scalatest.exceptions.TestFailedException] {
      checkAnswer(sql("SELECT 1"), Row(2) :: Nil)
    }
  }
}
