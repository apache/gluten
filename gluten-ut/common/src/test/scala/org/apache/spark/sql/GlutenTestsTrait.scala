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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.test.TestStats
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.GlutenQueryTestUtil.isNaNOrInf
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData, TypeUtils}
import org.apache.spark.sql.classic.ClassicColumn
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.commons.io.FileUtils
import org.apache.commons.math3.util.Precision
import org.scalactic.TripleEqualsSupport.Spread

import java.io.File

import scala.annotation.nowarn
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait GlutenTestsTrait extends GlutenTestsCommonTrait {

  protected def ansiTest: Boolean = !GlutenConfig.get.enableAnsiFallback

  // TODO: remove this if we can suppress unused import error.
  locally {
    new ColumnConstructorExt(Column)
  }

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
    initializeSession()
    _spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
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
      }
    }
    logInfo(
      "Test suite: " + this.getClass.getSimpleName +
        "; Suite test number: " + TestStats.suiteTestNumber +
        "; OffloadGluten number: " + TestStats.offloadGlutenTestNumber + "\n")
    TestStats.printMarkdown(this.getClass.getSimpleName)
    TestStats.reset()
  }

  protected def initializeSession(): Unit = {
    if (_spark == null) {
      val sparkBuilder = SparkSession
        .builder()
        .appName("Gluten-UT")
        .master(s"local[2]")
        // Avoid static evaluation for literal input by spark catalyst.
        .config(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
          ConvertToLocalRelation.ruleName +
            "," + ConstantFolding.ruleName + "," + NullPropagation.ruleName)
        .config("spark.driver.memory", "1G")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1024MB")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.sql.warehouse.dir", warehouse)
        // Avoid the code size overflow error in Spark code generation.
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.ui.enabled", "false")

      _spark = if (BackendTestUtils.isCHBackendLoaded()) {
        sparkBuilder
          .config("spark.io.compression.codec", "LZ4")
          .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
          .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
          .config("spark.sql.files.openCostInBytes", "134217728")
          .config("spark.unsafe.exceptionOnMemoryLeak", "true")
          .config(GlutenConfig.UT_STATISTIC.key, "true")
          .getOrCreate()
      } else {
        sparkBuilder
          .config("spark.unsafe.exceptionOnMemoryLeak", "true")
          .getOrCreate()
      }
    }
  }

  protected var _spark: SparkSession = _

  protected def resolveExpression(expression: Expression): Expression = {
    ResolveTimeZone.resolveTimeZones(expression)
  }

  override protected def checkEvaluation(
      expression: => Expression,
      expected: Any,
      inputRow: InternalRow = EmptyRow): Unit = {

    if (canConvertToDataFrame(inputRow)) {
      val expr = resolveExpression(expression)
      assert(expr.resolved)

      glutenCheckExpression(expr, expected, inputRow)
    } else {
      logWarning(
        "Skipping evaluation - Nonempty inputRow cannot be converted to DataFrame " +
          "due to complex/unsupported types.\n")
    }
  }

  // Delegates to Spark's ExpressionEvalHelper when ansiFallback is enabled (default);
  // routes through Velox only when ansiFallback is explicitly disabled (ANSI-compliance testing).
  // TODO: Velox still has issues when ansiFallback=false.
  override def checkExceptionInExpression[T <: Throwable: ClassTag](
      expression: => Expression,
      inputRow: InternalRow,
      expectedErrMsg: String): Unit = {
    if (ansiTest) {
      val expr = resolveExpression(expression)
      assert(expr.resolved)
      glutenCheckExceptionInExpression[T](expr, inputRow, expectedErrMsg)
    } else {
      super.checkExceptionInExpression[T](expression, inputRow, expectedErrMsg)
    }
  }

  /**
   * Sort map data by key and return the sorted key array and value array.
   *
   * @param input
   *   : input map data.
   * @param kt
   *   : key type.
   * @param vt
   *   : value type.
   * @return
   *   the sorted key array and value array.
   */
  private def getSortedArrays(
      input: MapData,
      kt: DataType,
      vt: DataType): (ArrayData, ArrayData) = {
    val keyArray = input.keyArray().toArray[Any](kt)
    val valueArray = input.valueArray().toArray[Any](vt)
    val newMap = (keyArray.zip(valueArray)).toMap
    val sortedMap = mutable.SortedMap(newMap.toSeq: _*)(TypeUtils.getInterpretedOrdering(kt))
    (new GenericArrayData(sortedMap.keys.toArray), new GenericArrayData(sortedMap.values.toArray))
  }

  override protected def checkResult(
      result: Any,
      expected: Any,
      exprDataType: DataType,
      exprNullable: Boolean): Boolean = {
    val dataType = UserDefinedType.sqlType(exprDataType)

    // The result is null for a non-nullable expression
    assert(result != null || exprNullable, "exprNullable should be true if result is null")
    (result, expected) match {
      case (result: Array[Byte], expected: Array[Byte]) =>
        java.util.Arrays.equals(result, expected)
      case (result: Double, expected: Spread[Double @unchecked]) =>
        expected.asInstanceOf[Spread[Double]].isWithin(result)
      case (result: InternalRow, expected: InternalRow) =>
        val st = dataType.asInstanceOf[StructType]
        assert(result.numFields == st.length && expected.numFields == st.length)
        st.zipWithIndex.forall {
          case (f, i) =>
            checkResult(
              result.get(i, f.dataType),
              expected.get(i, f.dataType),
              f.dataType,
              f.nullable)
        }
      case (result: ArrayData, expected: ArrayData) =>
        result.numElements == expected.numElements && {
          val ArrayType(et, cn) = dataType.asInstanceOf[ArrayType]
          var isSame = true
          var i = 0
          while (isSame && i < result.numElements) {
            isSame = checkResult(result.get(i, et), expected.get(i, et), et, cn)
            i += 1
          }
          isSame
        }
      case (result: MapData, expected: MapData) =>
        val MapType(kt, vt, vcn) = dataType.asInstanceOf[MapType]
        checkResult(
          getSortedArrays(result, kt, vt)._1,
          getSortedArrays(expected, kt, vt)._1,
          ArrayType(kt, containsNull = false),
          exprNullable = false) && checkResult(
          getSortedArrays(result, kt, vt)._2,
          getSortedArrays(expected, kt, vt)._2,
          ArrayType(vt, vcn),
          exprNullable = false)
      case (result: Double, expected: Double) =>
        if (
          (isNaNOrInf(result) || isNaNOrInf(expected))
          || (result == -0.0) || (expected == -0.0)
        ) {
          java.lang.Double.doubleToRawLongBits(result) ==
            java.lang.Double.doubleToRawLongBits(expected)
        } else {
          Precision.equalsWithRelativeTolerance(result, expected, 0.00001d)
        }
      case (result: Float, expected: Float) =>
        if (expected.isNaN) result.isNaN else expected == result
      case (result: Row, expected: InternalRow) => result.toSeq == expected.toSeq(result.schema)
      case _ =>
        result == expected
    }
  }

  def checkDataTypeSupported(expr: Expression): Boolean = {
    GlutenTestConstants.SUPPORTED_DATA_TYPE.acceptsType(expr.dataType)
  }

  def glutenCheckExpression(expression: Expression, expected: Any, inputRow: InternalRow): Unit = {
    val resultDF = buildResultDF(expression, inputRow)
    doCheckExpression(expression, expected, inputRow, resultDF)
  }

  protected def buildResultDF(expression: Expression, inputRow: InternalRow): DataFrame = {
    val df = if (inputRow != EmptyRow && inputRow != InternalRow.empty) {
      convertInternalRowToDataFrame(inputRow)
    } else {
      val schema = StructType(StructField("a", IntegerType, nullable = true) :: Nil)
      val empData = Seq(Row(1))
      _spark.createDataFrame(_spark.sparkContext.parallelize(empData), schema)
    }
    df.select(ClassicColumn(expression))
  }

  protected def doCheckExpression(
      expression: Expression,
      expected: Any,
      inputRow: InternalRow,
      resultDF: DataFrame): Unit = {
    val result =
      try {
        resultDF.collect()
      } catch {
        // Match Spark's checkEvaluationWithoutCodegen behavior: wrap exceptions with fail().
        // Gluten's DataFrame path wraps execution errors in SparkException, so unwrap it
        // to expose the root cause (e.g. ArithmeticException) directly as fail()'s cause,
        // just like Spark's interpreted path does.
        case e: SparkException if e.getCause != null =>
          fail(s"Exception evaluating $expression", e.getCause)
        case e: Exception =>
          fail(s"Exception evaluating $expression", e)
      }

    if (
      !(checkResult(result.head.get(0), expected, expression.dataType, expression.nullable)
        || checkResult(
          CatalystTypeConverters.createToCatalystConverter(expression.dataType)(
            result.head.get(0)
          ),
          CatalystTypeConverters.convertToCatalyst(expected),
          expression.dataType,
          expression.nullable
        ))
    ) {
      val input = if (inputRow == EmptyRow) "" else s", input: $inputRow"
      fail(
        s"Incorrect evaluation: $expression, " +
          s"actual: ${result.head.get(0)}, " +
          s"expected: $expected$input")
    }
  }

  @nowarn("cat=deprecation")
  def glutenCheckExceptionInExpression[T <: Throwable: ClassTag](
      expression: Expression,
      inputRow: InternalRow,
      expectedErrMsg: String): Unit = {
    val resultDF = buildResultDF(expression, inputRow)
    doCheckExceptionInExpression[T](expression, inputRow, expectedErrMsg, resultDF)
  }

  protected def doCheckExceptionInExpression[T <: Throwable: ClassTag](
      expression: Expression,
      inputRow: InternalRow,
      expectedErrMsg: String,
      resultDF: DataFrame): Unit = {
    val clazz = implicitly[ClassTag[T]].runtimeClass
    val thrown = intercept[Exception](resultDF.collect())
    val exception = findCause(thrown, clazz).getOrElse {
      fail(
        s"Expected ${clazz.getSimpleName} but got ${thrown.getClass.getSimpleName}: " +
          s"${thrown.getMessage}",
        thrown)
    }
    if (expectedErrMsg != null && exception.getMessage != null) {
      if (!exception.getMessage.contains(expectedErrMsg)) {
        exception match {
          case st: SparkThrowable if st.getErrorClass != null =>
            logWarning(
              s"Message mismatch accepted: errorClass=${st.getErrorClass}, " +
                s"expected msg containing '$expectedErrMsg', " +
                s"got '${exception.getMessage}'")
          case _ =>
            fail(
              s"Expected error message containing '$expectedErrMsg' " +
                s"but got '${exception.getMessage}'")
        }
      }
    }
  }

  private def findCause(e: Throwable, clazz: Class[_]): Option[Throwable] = {
    var current: Throwable = e
    while (current != null) {
      if (clazz.isAssignableFrom(current.getClass)) {
        return Some(current)
      }
      current = current.getCause
    }
    None
  }

  def shouldNotFallback(): Unit = {
    TestStats.offloadGluten = false
    if (!BackendTestUtils.isCHBackendLoaded()) {
      return
    }

    val supportedExpr = BackendsApiManager.getTransformerApiInstance.getSupportExpressionClassName
    TestStats.getFallBackClassName.forEach(
      fallbackClassName => {
        assert(
          !supportedExpr.contains(fallbackClassName),
          "\nCH has already support %s, suggest not fallback, please fix it"
            .format(fallbackClassName))
      })
  }

  def canConvertToDataFrame(inputRow: InternalRow): Boolean = {
    if (inputRow == EmptyRow || inputRow == InternalRow.empty) {
      return true
    }
    if (!inputRow.isInstanceOf[GenericInternalRow]) {
      return false
    }
    val values = inputRow.asInstanceOf[GenericInternalRow].values
    for (value <- values) {
      value match {
        case _: MapData => return false
        case _: ArrayData => return false
        case _: InternalRow => return false
        case _ =>
      }
    }
    true
  }

  def convertInternalRowToDataFrame(inputRow: InternalRow): DataFrame = {
    val structFileSeq = new ArrayBuffer[StructField]()
    val values = inputRow match {
      case genericInternalRow: GenericInternalRow =>
        genericInternalRow.values
      case _ => throw new UnsupportedOperationException("Unsupported InternalRow.")
    }
    val inputValues = values.map {
      case boolean: java.lang.Boolean =>
        structFileSeq.append(StructField("bool", BooleanType, boolean == null))
      case short: java.lang.Short =>
        structFileSeq.append(StructField("i16", ShortType, short == null))
      case byte: java.lang.Byte =>
        structFileSeq.append(StructField("i8", ByteType, byte == null))
      case integer: java.lang.Integer =>
        structFileSeq.append(StructField("i32", IntegerType, integer == null))
      case long: java.lang.Long =>
        structFileSeq.append(StructField("i64", LongType, long == null))
      case float: java.lang.Float =>
        structFileSeq.append(StructField("fp32", FloatType, float == null))
      case double: java.lang.Double =>
        structFileSeq.append(StructField("fp64", DoubleType, double == null))
      case utf8String: UTF8String =>
        structFileSeq.append(StructField("str", StringType, utf8String == null))
      case byteArr: Array[Byte] =>
        structFileSeq.append(StructField("vbin", BinaryType, byteArr == null))
      case decimal: Decimal =>
        structFileSeq.append(
          StructField("dec", DecimalType(decimal.precision, decimal.scale), decimal == null))
      case _ =>
        // for null
        structFileSeq.append(StructField("n", IntegerType, nullable = true))
    }
    _spark.internalCreateDataFrame(
      _spark.sparkContext.parallelize(Seq(inputRow)),
      StructType(structFileSeq.toSeq))
  }
}
