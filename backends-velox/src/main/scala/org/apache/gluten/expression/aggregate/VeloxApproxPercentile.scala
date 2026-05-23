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
package org.apache.gluten.expression.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

import java.util

/**
 * Velox-compatible DeclarativeAggregate for approx_percentile.
 *
 * Unlike Spark's ApproximatePercentile (which uses QuantileSummaries/GK algorithm with BinaryType
 * intermediate data), this implementation uses KLL sketch with a 9-field StructType intermediate
 * that is fully compatible with Velox's approx_percentile accumulator layout:
 *
 * 0: percentiles - Array(Double) 1: percentilesIsArray - Boolean 2: accuracy - Double (Spark
 * accuracy, e.g. 10000.0; Velox internally computes epsilon = 1.0/accuracy) 3: k - Integer (KLL
 * parameter) 4: n - Long (total count) 5: minValue - childType 6: maxValue - childType 7: items -
 * Array(childType) 8: levels - Array(Integer)
 *
 * Because aggBufferAttributes has 9 fields (> 1), the existing VeloxIntermediateData.Type default
 * branch (aggBufferAttributes.size > 1) will match automatically, meaning:
 *   - No special handling needed in HashAggregateExecTransformer
 *   - extractStruct / rowConstruct projections work out of the box
 *   - Partial fallback (Velox partial -> Spark final) is supported
 *
 * This follows the same pattern as VeloxCollectList/VeloxCollectSet.
 */
case class VeloxApproximatePercentile(
    child: Expression,
    percentageExpression: Expression,
    accuracyExpression: Expression)
  extends DeclarativeAggregate
  with TernaryLike[Expression] {

  override def first: Expression = child
  override def second: Expression = percentageExpression
  override def third: Expression = accuracyExpression

  override def prettyName: String = "velox_approx_percentile"

  // Mark as lazy so that expressions are not evaluated during tree transformation.
  private lazy val accuracy: Int = accuracyExpression.eval() match {
    case null => ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY.toInt
    case num: Number => num.intValue()
  }

  // Compute K from accuracy using the same formula as Velox kFromEpsilon.
  private lazy val kValue: Int = KllSketchFieldIndex.kFromAccuracy(accuracy)

  private lazy val (returnPercentileArray, percentages): (Boolean, Array[Double]) =
    percentageExpression.eval() match {
      case null => (false, null)
      case num: Double => (false, Array(num))
      case arrayData: ArrayData => (true, arrayData.toDoubleArray())
    }

  override def checkInputDataTypes(): TypeCheckResult = {
    // Delegate to Spark's ApproximatePercentile for validation
    new ApproximatePercentile(child, percentageExpression, accuracyExpression)
      .checkInputDataTypes()
  }

  override def nullable: Boolean = true

  override def dataType: DataType = {
    if (returnPercentileArray) ArrayType(child.dataType, containsNull = false)
    else child.dataType
  }

  // --- The 9 aggBuffer attributes matching Velox KLL sketch intermediate type ---

  private lazy val percentilesBuf: AttributeReference =
    AttributeReference("percentiles", ArrayType(DoubleType))()
  private lazy val percentilesIsArrayBuf: AttributeReference =
    AttributeReference("percentilesIsArray", BooleanType)()
  private lazy val accuracyBuf: AttributeReference =
    AttributeReference("accuracy", IntegerType)()
  private lazy val kBuf: AttributeReference =
    AttributeReference("k", IntegerType)()
  private lazy val nBuf: AttributeReference =
    AttributeReference("n", LongType)()
  private lazy val minValueBuf: AttributeReference =
    AttributeReference("minValue", child.dataType)()
  private lazy val maxValueBuf: AttributeReference =
    AttributeReference("maxValue", child.dataType)()
  private lazy val itemsBuf: AttributeReference =
    AttributeReference("items", ArrayType(child.dataType))()
  private lazy val levelsBuf: AttributeReference =
    AttributeReference("levels", ArrayType(IntegerType))()

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(
    percentilesBuf,
    percentilesIsArrayBuf,
    accuracyBuf,
    kBuf,
    nBuf,
    minValueBuf,
    maxValueBuf,
    itemsBuf,
    levelsBuf
  )

  // --- Initial values: create an empty KLL sketch ---

  private lazy val percentilesLiteral: Literal = {
    if (percentages == null) {
      Literal.create(null, ArrayType(DoubleType))
    } else {
      Literal.create(
        new GenericArrayData(percentages.map(_.asInstanceOf[Any])),
        ArrayType(DoubleType))
    }
  }

  override lazy val initialValues: Seq[Expression] = Seq(
    percentilesLiteral, // percentiles
    Literal.create(returnPercentileArray, BooleanType), // percentilesIsArray
    Literal.create(accuracy, IntegerType), // accuracy
    Literal.create(kValue, IntegerType), // k
    Literal.create(0L, LongType), // n
    Literal.create(null, child.dataType), // minValue
    Literal.create(null, child.dataType), // maxValue
    Literal.create(
      new GenericArrayData(Array.empty[Any]),
      ArrayType(child.dataType)
    ), // items
    Literal.create(
      new GenericArrayData(Array(0, 0)),
      ArrayType(IntegerType)
    ) // levels
  )

  // --- Update expressions: add a value to the sketch ---

  override lazy val updateExpressions: Seq[Expression] = {
    // When input is null, keep buffer unchanged; otherwise call KllSketchAdd
    val structExpr = CreateStruct(aggBufferAttributes)
    val updated = If(
      IsNull(child),
      structExpr,
      KllSketchAdd(structExpr, child, child.dataType)
    )
    // Extract fields from the updated struct back to individual buffer attributes
    aggBufferAttributes.zipWithIndex.map {
      case (attr, idx) =>
        GetStructField(updated, idx, Some(attr.name))
    }
  }

  // --- Merge expressions: merge two sketches ---

  override lazy val mergeExpressions: Seq[Expression] = {
    val leftStruct = CreateStruct(aggBufferAttributes.map(_.left))
    val rightStruct = CreateStruct(aggBufferAttributes.map(_.right))
    val merged = KllSketchMerge(leftStruct, rightStruct, child.dataType)
    aggBufferAttributes.zipWithIndex.map {
      case (attr, idx) =>
        GetStructField(merged, idx, Some(attr.name))
    }
  }

  // --- Evaluate expression: extract percentiles from the sketch ---

  override lazy val evaluateExpression: Expression = {
    val structExpr = CreateStruct(aggBufferAttributes)
    KllSketchEval(structExpr, returnPercentileArray, dataType, child.dataType)
  }

  override def defaultResult: Option[Literal] = Option(Literal.create(null, dataType))

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): VeloxApproximatePercentile =
    copy(child = newFirst, percentageExpression = newSecond, accuracyExpression = newThird)
}

/**
 * KLL sketch field indices matching Velox's ApproxPercentileIntermediateTypeChildIndex.
 *
 * The intermediate StructType has 9 fields: 0: percentiles - Array(Double) 1: percentilesIsArray -
 * Boolean 2: accuracy - Double (Spark accuracy) 3: k - Integer (KLL parameter) 4: n - Long (total
 * count) 5: minValue \- childType 6: maxValue - childType 7: items - Array(childType) 8: levels -
 * Array(Integer)
 */
object KllSketchFieldIndex {
  val PERCENTILES = 0
  val PERCENTILES_IS_ARRAY = 1
  val ACCURACY = 2
  val K = 3
  val N = 4
  val MIN_VALUE = 5
  val MAX_VALUE = 6
  val ITEMS = 7
  val LEVELS = 8
  val NUM_FIELDS = 9

  /** Build the StructType for KLL sketch intermediate data. */
  def intermediateStructType(childType: DataType): StructType = StructType(
    Array(
      StructField("percentiles", ArrayType(DoubleType), nullable = true),
      StructField("percentilesIsArray", BooleanType, nullable = true),
      StructField("accuracy", IntegerType, nullable = true),
      StructField("k", IntegerType, nullable = true),
      StructField("n", LongType, nullable = true),
      StructField("minValue", childType, nullable = true),
      StructField("maxValue", childType, nullable = true),
      StructField("items", ArrayType(childType), nullable = true),
      StructField("levels", ArrayType(IntegerType), nullable = true)
    ))

  /** Default KLL k parameter (same as Velox default). */
  val DEFAULT_K: Int = 200

  /**
   * Compute K from accuracy using the same formula as Velox kFromEpsilon. accuracy is the
   * reciprocal of epsilon (e.g., 10000 means epsilon = 1/10000). K = ceil(exp(1.0285 * log(2.296 /
   * epsilon)))
   */
  def kFromAccuracy(accuracy: Int): Int = {
    if (accuracy <= 0) return DEFAULT_K
    val epsilon = 1.0 / accuracy.toDouble
    math.ceil(math.exp(1.0285 * math.log(2.296 / epsilon))).toInt
  }
}

/**
 * Helper object encapsulating the core KLL sketch algorithm logic.
 *
 * This is a multi-level implementation that runs on the Spark side during fallback. The struct
 * layout is fully compatible with Velox's KLL sketch intermediate type, enabling partial fallback
 * (Velox partial -> Spark final).
 *
 * The algorithm follows Velox's KLL sketch implementation:
 *   - items array stores elements across multiple levels, levels array tracks boundaries
 *   - Level 0 receives new inserts. When levels get full, compaction promotes items upward.
 *   - Each level i's items represent 2^i original values (weighted).
 *   - Quantile estimation uses weighted frequencies from all levels.
 *
 * See https://arxiv.org/abs/1603.05346v2 for more details.
 */
object KllSketchHelper {

  private val MIN_BUFFER_WIDTH = 8
  // Absolute hard upper bound for any KLL sketch buffer allocation. The KLL
  // sketch is logarithmic in N, so even for trillions of items the buffer
  // never legitimately exceeds tens of thousands of doubles. We cap at 1M
  // doubles (~8 MiB) to defend against corrupted intermediate state coming
  // from upstream merges (e.g. negative or stale `levels` boundaries) that
  // would otherwise inflate the allocation to GiB scale and OOM the executor.
  private val MAX_KLL_BUFFER_SIZE = 1 << 20 // 1,048,576
  private val random = new java.util.Random()

  /** Compute level capacity: max(8, k * (2/3)^(numLevels - height - 1)). */
  private def levelCapacity(k: Int, numLevels: Int, height: Int): Int = {
    math.max(MIN_BUFFER_WIDTH, (k * math.pow(2.0 / 3.0, numLevels - height - 1)).toInt)
  }

  /** Compute total capacity across all levels. */
  private def computeTotalCapacity(k: Int, numLevels: Int): Int = {
    var total = 0
    var h = 0
    while (h < numLevels) {
      total += levelCapacity(k, numLevels, h)
      h += 1
    }
    total
  }

  /**
   * Create an empty KLL sketch as an InternalRow (struct).
   *
   * @param percentiles
   *   Array of percentile values
   * @param isArray
   *   Whether the percentile argument is an array
   * @param accuracy
   *   The accuracy parameter (maps to relativeError = 1/accuracy)
   * @param childType
   *   The data type of values being aggregated
   */
  def createEmpty(
      percentiles: ArrayData,
      isArray: Boolean,
      accuracy: Int,
      childType: DataType): InternalRow = {
    val k = KllSketchFieldIndex.kFromAccuracy(accuracy)
    InternalRow(
      percentiles, // percentiles
      isArray, // percentilesIsArray
      accuracy, // accuracy
      k, // k
      0L, // n
      null, // minValue
      null, // maxValue
      new GenericArrayData(Array.empty[Any]), // items
      new GenericArrayData(Array(0, 0)) // levels: [0, 0] means 1 level with 0 items
    )
  }

  /**
   * Add a value to the KLL sketch. Returns a new InternalRow representing the updated sketch.
   *
   * Items are inserted into level 0. When the sketch becomes too full, compaction is triggered
   * following Velox's algorithm: the lowest over-capacity level is sorted, randomly halved, and
   * merged into the level above.
   */
  def add(sketch: InternalRow, value: Any, childType: DataType): InternalRow = {
    if (value == null) return sketch

    val n = sketch.getLong(KllSketchFieldIndex.N)
    val k = sketch.getInt(KllSketchFieldIndex.K)
    val items = sketch.getArray(KllSketchFieldIndex.ITEMS)
    val levels = sketch.getArray(KllSketchFieldIndex.LEVELS)

    val doubleValue = toDouble(value, childType)

    // Update min/max
    val newMin = if (sketch.isNullAt(KllSketchFieldIndex.MIN_VALUE)) {
      doubleValue
    } else {
      math.min(
        toDouble(sketch.get(KllSketchFieldIndex.MIN_VALUE, childType), childType),
        doubleValue)
    }
    val newMax = if (sketch.isNullAt(KllSketchFieldIndex.MAX_VALUE)) {
      doubleValue
    } else {
      math.max(
        toDouble(sketch.get(KllSketchFieldIndex.MAX_VALUE, childType), childType),
        doubleValue)
    }

    // Convert current state to mutable arrays
    val numLevels = levels.numElements() - 1
    var curItems = new Array[Double](items.numElements())
    var i = 0
    while (i < items.numElements()) {
      curItems(i) = toDouble(items.get(i, childType), childType)
      i += 1
    }
    var curLevels = new Array[Int](levels.numElements())
    i = 0
    while (i < levels.numElements()) {
      curLevels(i) = levels.getInt(i)
      i += 1
    }

    // For early inserts (small sketch), just append to level 0
    if (curItems.length < k && numLevels == 1) {
      val newItems = curItems :+ doubleValue
      val newLevels = curLevels.clone()
      newLevels(newLevels.length - 1) += 1
      return buildRow(sketch, k, n + 1, newMin, newMax, newItems, newLevels, childType)
    }

    // Insert into level 0 with possible compaction
    val result = insertWithCompaction(curItems, curLevels, doubleValue, k)
    buildRow(sketch, k, n + 1, newMin, newMax, result._1, result._2, childType)
  }

  /**
   * Insert a value into level 0, compacting a level if necessary to make space. Returns (items,
   * levels) after insertion.
   */
  private def insertWithCompaction(
      items: Array[Double],
      levels: Array[Int],
      value: Double,
      k: Int): (Array[Double], Array[Int]) = {
    var curItems = items
    var curLevels = levels

    // If level 0 start is already at 0, we need to compact to make room
    if (curLevels(0) == 0) {
      val numLevels = curLevels.length - 1

      // Find which level to compact
      val level = findLevelToCompact(curLevels, k)

      if (level == numLevels - 1) {
        // Check if we can just grow level 0
        val totalCap = computeTotalCapacity(k, numLevels)
        if (totalCap > curItems.length) {
          val delta = totalCap - curItems.length
          val shifted = shiftItems(curItems, curLevels, delta)
          curItems = shifted._1
          curLevels = shifted._2
        } else {
          // Add empty top level
          val deltaCap = levelCapacity(k, numLevels + 1, 0)
          val shifted = shiftItems(curItems, curLevels, deltaCap)
          curItems = shifted._1
          curLevels = shifted._2
          curLevels = curLevels :+ curLevels.last
        }
        // Re-find level after potential restructuring
        val numLevels2 = curLevels.length - 1
        val level2 = findLevelToCompact(curLevels, k)
        if (curLevels(0) == 0) {
          compactLevel(curItems, curLevels, level2, k)
        }
      } else {
        compactLevel(curItems, curLevels, level, k)
      }
    }

    // Now there should be room at curLevels(0) - 1
    curLevels(0) -= 1
    curItems(curLevels(0)) = value

    (curItems, curLevels)
  }

  /** Find the lowest level that is at or over capacity. */
  private def findLevelToCompact(levels: Array[Int], k: Int): Int = {
    val numLevels = levels.length - 1
    var level = 0
    var found = false
    while (!found) {
      val pop = levels(level + 1) - levels(level)
      val cap = levelCapacity(k, numLevels, level)
      if (pop >= cap || level + 1 == numLevels) {
        found = true
      } else {
        level += 1
      }
    }
    level
  }

  /** Shift all items right by delta positions to make room at the bottom. */
  private def shiftItems(
      items: Array[Double],
      levels: Array[Int],
      delta: Int): (Array[Double], Array[Int]) = {
    val newItems = new Array[Double](items.length + delta)
    System.arraycopy(items, 0, newItems, delta, items.length)
    val newLevels = levels.map(_ + delta)
    (newItems, newLevels)
  }

  /**
   * Compact a given level: sort it (if level 0), randomly halve, merge with level above. This
   * modifies items and levels arrays in place.
   */
  private def compactLevel(items: Array[Double], levels: Array[Int], level: Int, k: Int): Unit = {
    val numLevels = levels.length - 1

    val rawBeg = levels(level)
    val rawLim = levels(level + 1)
    val rawPop = rawLim - rawBeg
    val oddPop = rawPop & 1
    val adjBeg = rawBeg + oddPop
    val adjPop = rawPop - oddPop
    val halfAdjPop = adjPop / 2

    // Ensure level + 2 is accessible
    val popAbove = if (level + 2 < levels.length) {
      levels(level + 2) - rawLim
    } else {
      0
    }

    // Sort level 0 if needed
    if (level == 0) {
      util.Arrays.sort(items, adjBeg, adjBeg + adjPop)
    }

    if (popAbove == 0) {
      // Level above is empty: halve up (keep items in upper half)
      randomlyHalveUp(items, adjBeg, adjPop)
    } else {
      // Level above is nonempty: halve down (keep items in lower half), then merge up
      randomlyHalveDown(items, adjBeg, adjPop)
      mergeOverlap(items, adjBeg, halfAdjPop, rawLim, popAbove, adjBeg + halfAdjPop)
    }

    // Adjust the boundary of the level above
    levels(level + 1) -= halfAdjPop

    // Handle the current level
    if (oddPop != 0) {
      levels(level) = levels(level + 1) - 1
      if (levels(level) != rawBeg) {
        items(levels(level)) = items(rawBeg)
      }
    } else {
      levels(level) = levels(level + 1)
    }

    // Shift lower levels up to reclaim the freed space
    if (level > 0) {
      val amount = rawBeg - levels(0)
      // Move items backward (from lower position to higher)
      System.arraycopy(items, levels(0), items, levels(0) + halfAdjPop, amount)
      var lvl = 0
      while (lvl < level) {
        levels(lvl) += halfAdjPop
        lvl += 1
      }
    }
  }

  /** Randomly halve up: collect elements in odd or even positions to second half. */
  private def randomlyHalveUp(buf: Array[Double], start: Int, length: Int): Unit = {
    val halfLength = length / 2
    val offset = if (random.nextBoolean()) 1 else 0
    var j = (start + length) - 1 - offset
    var i = (start + length) - 1
    while (i >= start + halfLength) {
      buf(i) = buf(j)
      j -= 2
      i -= 1
    }
  }

  /** Randomly halve down: collect elements in odd or even positions to first half. */
  private def randomlyHalveDown(buf: Array[Double], start: Int, length: Int): Unit = {
    val halfLength = length / 2
    val offset = if (random.nextBoolean()) 1 else 0
    var j = start + offset
    var i = start
    while (i < start + halfLength) {
      buf(i) = buf(j)
      j += 2
      i += 1
    }
  }

  /**
   * Merge 2 sorted ranges where target can overlap with range B. Range A: buf[startA..startA+lenA),
   * Range B: buf[startB..startB+lenB) Target: starting at buf[startC]
   */
  private def mergeOverlap(
      buf: Array[Double],
      startA: Int,
      lenA: Int,
      startB: Int,
      lenB: Int,
      startC: Int): Unit = {
    val limA = startA + lenA
    val limB = startB + lenB
    var a = startA
    var b = startB
    var c = startC
    while (a < limA && b < limB) {
      if (buf(a) <= buf(b)) {
        buf(c) = buf(a)
        a += 1
      } else {
        buf(c) = buf(b)
        b += 1
      }
      c += 1
    }
    while (a < limA) {
      buf(c) = buf(a)
      a += 1
      c += 1
    }
    while (b < limB) {
      buf(c) = buf(b)
      b += 1
      c += 1
    }
  }

  /**
   * General compress following Velox's algorithm. Processes all levels and compacts those that are
   * over capacity. Returns (finalNumLevels, finalCapacity, finalNumItems).
   */
  private def generalCompress(
      k: Int,
      numLevelsIn: Int,
      items: Array[Double],
      inLevels: Array[Int],
      outLevels: Array[Int],
      isLevelZeroSorted: Boolean): (Int, Int, Int) = {
    var currentNumLevels = numLevelsIn
    var currentItemCount = inLevels(numLevelsIn) - inLevels(0)
    var targetItemCount = computeTotalCapacity(k, currentNumLevels)
    outLevels(0) = 0

    var level = 0
    while (level < currentNumLevels) {
      // If at current top level, add an empty level above for convenience.
      // Guard against writing past the end of inLevels (which would corrupt
      // memory and inflate targetItemCount via stale reads later).
      if (level == currentNumLevels - 1 && level + 2 < inLevels.length) {
        inLevels(level + 2) = inLevels(level + 1)
      }

      val rawBeg = inLevels(level)
      val rawLim = inLevels(level + 1)
      val rawPop = rawLim - rawBeg

      if (
        currentItemCount < targetItemCount ||
        rawPop < levelCapacity(k, currentNumLevels, level)
      ) {
        // Move level over as is
        System.arraycopy(items, rawBeg, items, outLevels(level), rawPop)
        outLevels(level + 1) = outLevels(level) + rawPop
      } else {
        // Compact this level
        val popAbove =
          if (level + 2 < inLevels.length) inLevels(level + 2) - rawLim else 0
        val oddPop = rawPop & 1
        val adjBeg = rawBeg + oddPop
        val adjPop = rawPop - oddPop
        val halfAdjPop = adjPop / 2

        if (oddPop != 0) {
          items(outLevels(level)) = items(rawBeg)
          outLevels(level + 1) = outLevels(level) + 1
        } else {
          outLevels(level + 1) = outLevels(level)
        }

        if (level == 0 && !isLevelZeroSorted) {
          util.Arrays.sort(items, adjBeg, adjBeg + adjPop)
        }

        if (popAbove == 0) {
          randomlyHalveUp(items, adjBeg, adjPop)
        } else {
          randomlyHalveDown(items, adjBeg, adjPop)
          mergeOverlap(items, adjBeg, halfAdjPop, rawLim, popAbove, adjBeg + halfAdjPop)
        }

        currentItemCount -= halfAdjPop
        inLevels(level + 1) = inLevels(level + 1) - halfAdjPop

        // Only promote a new top level if we have room for it; otherwise stop
        // growing to avoid runaway targetItemCount inflation.
        if (level == currentNumLevels - 1 && currentNumLevels + 1 < inLevels.length) {
          currentNumLevels += 1
          targetItemCount += levelCapacity(k, currentNumLevels, 0)
        }
      }
      level += 1
    }

    (currentNumLevels, targetItemCount, currentItemCount)
  }

  /** Merge two KLL sketches. Returns a new InternalRow representing the merged sketch. */
  def merge(left: InternalRow, right: InternalRow, childType: DataType): InternalRow = {
    if (left == null || left.getLong(KllSketchFieldIndex.N) == 0) return right
    if (right == null || right.getLong(KllSketchFieldIndex.N) == 0) return left

    val leftN = left.getLong(KllSketchFieldIndex.N)
    val rightN = right.getLong(KllSketchFieldIndex.N)
    val k = math.max(left.getInt(KllSketchFieldIndex.K), right.getInt(KllSketchFieldIndex.K))

    // Merge min/max
    val leftMin = toDouble(left.get(KllSketchFieldIndex.MIN_VALUE, childType), childType)
    val rightMin = toDouble(right.get(KllSketchFieldIndex.MIN_VALUE, childType), childType)
    val leftMax = toDouble(left.get(KllSketchFieldIndex.MAX_VALUE, childType), childType)
    val rightMax = toDouble(right.get(KllSketchFieldIndex.MAX_VALUE, childType), childType)
    val mergedMin = math.min(leftMin, rightMin)
    val mergedMax = math.max(leftMax, rightMax)

    // Extract items and levels from both sketches
    val leftItems = left.getArray(KllSketchFieldIndex.ITEMS)
    val rightItems = right.getArray(KllSketchFieldIndex.ITEMS)
    val leftLevels = left.getArray(KllSketchFieldIndex.LEVELS)
    val rightLevels = right.getArray(KllSketchFieldIndex.LEVELS)

    val leftNumLevels = leftLevels.numElements() - 1
    val rightNumLevels = rightLevels.numElements() - 1

    // Convert to double arrays
    val leftItemsArr = new Array[Double](leftItems.numElements())
    var i = 0
    while (i < leftItems.numElements()) {
      leftItemsArr(i) = toDouble(leftItems.get(i, childType), childType)
      i += 1
    }
    val rightItemsArr = new Array[Double](rightItems.numElements())
    i = 0
    while (i < rightItems.numElements()) {
      rightItemsArr(i) = toDouble(rightItems.get(i, childType), childType)
      i += 1
    }

    val leftLevelsArr = new Array[Int](leftLevels.numElements())
    i = 0
    while (i < leftLevels.numElements()) {
      leftLevelsArr(i) = leftLevels.getInt(i)
      i += 1
    }
    val rightLevelsArr = new Array[Int](rightLevels.numElements())
    i = 0
    while (i < rightLevels.numElements()) {
      rightLevelsArr(i) = rightLevels.getInt(i)
      i += 1
    }

    val newN = leftN + rightN
    val provisionalNumLevels = math.max(leftNumLevels, rightNumLevels)

    // Compute total number of items needed. Defensively clamp each side to
    // its actual items array size: if the levels array is corrupted (e.g.
    // levels.last is huge or negative), we must not trust it for sizing the
    // workbuf allocation.
    val safeLeftSpan =
      if (leftLevelsArr.length > 1) {
        val span = leftLevelsArr.last - leftLevelsArr(0)
        math.max(0, math.min(span, leftItemsArr.length))
      } else 0
    val safeRightSpan =
      if (rightLevelsArr.length > 1) {
        val span = rightLevelsArr.last - rightLevelsArr(0)
        math.max(0, math.min(span, rightItemsArr.length))
      } else 0
    val tmpNumItems = math.min(safeLeftSpan + safeRightSpan, MAX_KLL_BUFFER_SIZE)

    // Build work buffer merging all levels
    val workbuf = new Array[Double](tmpNumItems)
    val ub = 1 + floorLog2(newN, 1)
    // The work levels array must be large enough to hold:
    //   - at least `provisionalNumLevels + 1` boundary entries already present in input
    //   - plus extra slots that generalCompress may add when the top level is compacted
    //     (each compaction at the current top promotes a new empty level above).
    // Without enough headroom, generalCompress writes inLevels(level+2) past the end,
    // causing subsequent reads to pick up garbage and inflating finalCapacity to GiB
    // scale, which leads to OOM when allocating finalItems.
    val workLevelsSize = math.max(ub, provisionalNumLevels) + 8
    val worklevels = new Array[Int](workLevelsSize)
    val outlevels = new Array[Int](workLevelsSize)

    // Level 0: concatenate (unsorted is ok, generalCompress will sort)
    worklevels(0) = 0
    var outIdx = 0

    // Copy left level 0 items
    val leftLvl0Start = leftLevelsArr(0)
    val leftLvl0End = if (leftLevelsArr.length > 1) leftLevelsArr(1) else leftLvl0Start
    i = leftLvl0Start
    while (i < leftLvl0End) {
      workbuf(outIdx) = leftItemsArr(i)
      outIdx += 1
      i += 1
    }
    // Copy right level 0 items
    val rightLvl0Start = rightLevelsArr(0)
    val rightLvl0End = if (rightLevelsArr.length > 1) rightLevelsArr(1) else rightLvl0Start
    i = rightLvl0Start
    while (i < rightLvl0End) {
      workbuf(outIdx) = rightItemsArr(i)
      outIdx += 1
      i += 1
    }
    worklevels(1) = outIdx

    // Higher levels: merge sorted level-by-level using priority queue style merge
    var lvl = 1
    while (lvl < provisionalNumLevels) {
      val leftSz = safeLevelSize(leftLevelsArr, lvl)
      val rightSz = safeLevelSize(rightLevelsArr, lvl)

      if (leftSz > 0 && rightSz > 0) {
        // Merge two sorted ranges
        val leftStart = leftLevelsArr(lvl)
        val rightStart = rightLevelsArr(lvl)
        var li = leftStart
        var ri = rightStart
        val leftEnd = leftStart + leftSz
        val rightEnd = rightStart + rightSz
        while (li < leftEnd && ri < rightEnd) {
          if (leftItemsArr(li) <= rightItemsArr(ri)) {
            workbuf(outIdx) = leftItemsArr(li)
            li += 1
          } else {
            workbuf(outIdx) = rightItemsArr(ri)
            ri += 1
          }
          outIdx += 1
        }
        while (li < leftEnd) {
          workbuf(outIdx) = leftItemsArr(li)
          li += 1
          outIdx += 1
        }
        while (ri < rightEnd) {
          workbuf(outIdx) = rightItemsArr(ri)
          ri += 1
          outIdx += 1
        }
      } else if (leftSz > 0) {
        System.arraycopy(leftItemsArr, leftLevelsArr(lvl), workbuf, outIdx, leftSz)
        outIdx += leftSz
      } else if (rightSz > 0) {
        System.arraycopy(rightItemsArr, rightLevelsArr(lvl), workbuf, outIdx, rightSz)
        outIdx += rightSz
      }
      worklevels(lvl + 1) = outIdx
      lvl += 1
    }

    // Fill remaining worklevels entries
    lvl = provisionalNumLevels + 1
    while (lvl < workLevelsSize) {
      worklevels(lvl) = outIdx
      lvl += 1
    }

    // Run generalCompress
    val (finalNumLevels, finalCapacityRaw, finalNumItems) =
      generalCompress(k, provisionalNumLevels, workbuf, worklevels, outlevels, false)

    // Sanity-clamp finalCapacity. In pathological cases (e.g. corrupted intermediate
    // state coming from upstream merges), generalCompress may return an inflated
    // capacity that would allocate gigabytes for finalItems. Bound it by:
    //   - the actual number of items that survived compaction
    //   - the theoretical maximum capacity of a sketch with `finalNumLevels` levels
    //   - tmpNumItems (we cannot end up with more items than were fed in)
    //   - MAX_KLL_BUFFER_SIZE as an absolute backstop
    val safeFinalNumLevels = math.max(1, math.min(finalNumLevels, 64))
    val theoreticalCap = computeTotalCapacity(k, safeFinalNumLevels)
    val safeFinalNumItems = math.max(0, math.min(finalNumItems, MAX_KLL_BUFFER_SIZE))
    val finalCapacity = math.max(
      safeFinalNumItems,
      math.min(
        math.min(finalCapacityRaw, math.max(theoreticalCap, tmpNumItems)),
        MAX_KLL_BUFFER_SIZE))

    // Transfer results
    val freeSpaceAtBottom = finalCapacity - safeFinalNumItems
    val finalItems = new Array[Double](finalCapacity)
    System.arraycopy(workbuf, outlevels(0), finalItems, freeSpaceAtBottom, safeFinalNumItems)
    val finalLevels = new Array[Int](safeFinalNumLevels + 1)
    val offset = freeSpaceAtBottom - outlevels(0)
    i = 0
    while (i <= safeFinalNumLevels) {
      finalLevels(i) = outlevels(i) + offset
      i += 1
    }

    InternalRow(
      left.getArray(KllSketchFieldIndex.PERCENTILES),
      left.getBoolean(KllSketchFieldIndex.PERCENTILES_IS_ARRAY),
      left.getInt(KllSketchFieldIndex.ACCURACY),
      k,
      newN,
      fromDouble(mergedMin, childType),
      fromDouble(mergedMax, childType),
      new GenericArrayData(finalItems.map(v => fromDouble(v, childType))),
      new GenericArrayData(finalLevels.map(_.asInstanceOf[Any]))
    )
  }

  /** Safe level size: returns 0 if the level doesn't exist. */
  private def safeLevelSize(levels: Array[Int], level: Int): Int = {
    val numLevels = levels.length - 1
    if (level < numLevels) levels(level + 1) - levels(level) else 0
  }

  /** Floor of log2(p/q). */
  private def floorLog2(p: Long, q: Long): Int = {
    var qq = q
    var ans = 0
    while (true) {
      qq <<= 1
      if (p < qq) return ans
      ans += 1
    }
    ans // unreachable but needed for compilation
  }

  /**
   * Evaluate percentiles from a KLL sketch using weighted frequencies.
   *
   * Each item at level i has weight 2^i, reflecting the compaction process. Items are merged across
   * levels in sorted order, and cumulative weights are used to find the requested quantile
   * positions.
   */
  def eval(sketch: InternalRow, childType: DataType): Any = {
    val n = sketch.getLong(KllSketchFieldIndex.N)
    if (n == 0) return null

    val percentiles = sketch.getArray(KllSketchFieldIndex.PERCENTILES)
    val isArray = sketch.getBoolean(KllSketchFieldIndex.PERCENTILES_IS_ARRAY)
    val items = sketch.getArray(KllSketchFieldIndex.ITEMS)
    val levels = sketch.getArray(KllSketchFieldIndex.LEVELS)

    val numLevels = levels.numElements() - 1

    // Build weighted frequency entries: (value, cumulativeWeight)
    // Each level i item has weight 2^i
    val entries = new java.util.ArrayList[(Double, Long)]()
    var level = 0
    while (level < numLevels) {
      val start = levels.getInt(level)
      val end = levels.getInt(level + 1)
      val weight = 1L << level
      var j = start
      while (j < end) {
        entries.add((toDouble(items.get(j, childType), childType), weight))
        j += 1
      }
      level += 1
    }

    // Sort by value
    entries.sort((a: (Double, Long), b: (Double, Long)) => java.lang.Double.compare(a._1, b._1))

    // Merge duplicates and compute cumulative weights
    val sortedEntries = new Array[(Double, Long)](entries.size())
    var k = 0
    var idx = 0
    while (idx < entries.size()) {
      var entry = entries.get(idx)
      var j2 = idx + 1
      var mergedWeight = entry._2
      while (j2 < entries.size() && entries.get(j2)._1 == entry._1) {
        mergedWeight += entries.get(j2)._2
        j2 += 1
      }
      sortedEntries(k) = (entry._1, mergedWeight)
      k += 1
      idx = j2
    }

    // Compute cumulative weights
    var totalWeight = 0L
    var ci = 0
    while (ci < k) {
      totalWeight += sortedEntries(ci)._2
      sortedEntries(ci) = (sortedEntries(ci)._1, totalWeight)
      ci += 1
    }

    val numPercentiles = percentiles.numElements()
    val results = new Array[Any](numPercentiles)
    var pi = 0
    while (pi < numPercentiles) {
      val p = percentiles.getDouble(pi)
      if (p == 0.0) {
        results(pi) = sketch.get(KllSketchFieldIndex.MIN_VALUE, childType)
      } else if (p == 1.0) {
        results(pi) = sketch.get(KllSketchFieldIndex.MAX_VALUE, childType)
      } else {
        val maxWeight = (p * totalWeight).toLong
        // Binary search for the first entry with cumulative weight > maxWeight
        var lo = 0
        var hi = k - 1
        while (lo < hi) {
          val mid = (lo + hi) / 2
          if (sortedEntries(mid)._2 <= maxWeight) {
            lo = mid + 1
          } else {
            hi = mid
          }
        }
        results(pi) = fromDouble(sortedEntries(lo)._1, childType)
      }
      pi += 1
    }

    if (results.isEmpty) {
      null
    } else if (isArray) {
      new GenericArrayData(results)
    } else {
      results(0)
    }
  }

  /** Build InternalRow from double items and int levels arrays. */
  private def buildRow(
      sketch: InternalRow,
      k: Int,
      n: Long,
      minVal: Double,
      maxVal: Double,
      items: Array[Double],
      levels: Array[Int],
      childType: DataType): InternalRow = {
    InternalRow(
      sketch.getArray(KllSketchFieldIndex.PERCENTILES),
      sketch.getBoolean(KllSketchFieldIndex.PERCENTILES_IS_ARRAY),
      sketch.getInt(KllSketchFieldIndex.ACCURACY),
      k,
      n,
      fromDouble(minVal, childType),
      fromDouble(maxVal, childType),
      new GenericArrayData(items.map(v => fromDouble(v, childType))),
      new GenericArrayData(levels.map(_.asInstanceOf[Any]))
    )
  }

  /** Convert a value to Double for comparison/sorting. */
  private[aggregate] def toDouble(value: Any, dataType: DataType): Double = {
    if (value == null) return Double.NaN
    dataType match {
      case DoubleType => value.asInstanceOf[Double]
      case FloatType => value.asInstanceOf[Float].toDouble
      case IntegerType | DateType | _: YearMonthIntervalType =>
        value.asInstanceOf[Int].toDouble
      case LongType | TimestampType | _: DayTimeIntervalType =>
        value.asInstanceOf[Long].toDouble
      case ShortType => value.asInstanceOf[Short].toDouble
      case ByteType => value.asInstanceOf[Byte].toDouble
      case _: DecimalType =>
        value.asInstanceOf[Decimal].toDouble
      case _ =>
        value match {
          case n: Number => n.doubleValue()
          case _ if dataType.defaultSize == 8 => value.asInstanceOf[Long].toDouble
          case _ if dataType.defaultSize == 4 => value.asInstanceOf[Int].toDouble
          case _ =>
            throw new UnsupportedOperationException(
              s"KllSketchHelper.toDouble: unsupported type $dataType with value $value")
        }
    }
  }

  /** Convert a Double back to the original data type. */
  private[aggregate] def fromDouble(value: Double, dataType: DataType): Any = {
    dataType match {
      case DoubleType => value
      case FloatType => value.toFloat
      case IntegerType | DateType | _: YearMonthIntervalType => value.toInt
      case LongType | TimestampType | _: DayTimeIntervalType => value.toLong
      case ShortType => value.toShort
      case ByteType => value.toByte
      case dt: DecimalType => Decimal(value, dt.precision, dt.scale)
      case _ =>
        if (dataType.defaultSize == 8) value.toLong
        else if (dataType.defaultSize == 4) value.toInt
        else {
          throw new UnsupportedOperationException(
            s"KllSketchHelper.fromDouble: unsupported type $dataType")
        }
    }
  }
}

/**
 * Expression that adds a value to a KLL sketch. Used as the update expression in
 * VeloxApproximatePercentile's DeclarativeAggregate.
 *
 * @param sketch
 *   The current sketch (struct expression)
 * @param value
 *   The value to add
 * @param childType
 *   The data type of the value being aggregated
 */
case class KllSketchAdd(sketch: Expression, value: Expression, childType: DataType)
  extends BinaryExpression
  with CodegenFallback {

  override def left: Expression = sketch
  override def right: Expression = value
  override def dataType: DataType = sketch.dataType
  override def nullable: Boolean = false

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(sketch = newLeft, value = newRight)

  override def eval(input: InternalRow): Any = {
    val sketchRow = left.eval(input).asInstanceOf[InternalRow]
    val v = right.eval(input)
    if (v == null) return sketchRow
    KllSketchHelper.add(sketchRow, v, childType)
  }
}

/**
 * Expression that merges two KLL sketches. Used as the merge expression in
 * VeloxApproximatePercentile's DeclarativeAggregate.
 *
 * @param left
 *   The left sketch
 * @param right
 *   The right sketch
 * @param childType
 *   The data type of the values being aggregated
 */
case class KllSketchMerge(left: Expression, right: Expression, childType: DataType)
  extends BinaryExpression
  with CodegenFallback {

  override def dataType: DataType = left.dataType
  override def nullable: Boolean = false

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)

  override def eval(input: InternalRow): Any = {
    val leftRow = left.eval(input).asInstanceOf[InternalRow]
    val rightRow = right.eval(input).asInstanceOf[InternalRow]
    if (leftRow == null) return rightRow
    if (rightRow == null) return leftRow
    KllSketchHelper.merge(leftRow, rightRow, childType)
  }
}

/**
 * Expression that evaluates percentiles from a KLL sketch. Used as the evaluate expression in
 * VeloxApproximatePercentile's DeclarativeAggregate.
 *
 * @param sketch
 *   The sketch expression
 * @param returnArray
 *   Whether to return an array of percentiles
 * @param resultType
 *   The result data type
 * @param childType
 *   The data type of values being aggregated
 */
case class KllSketchEval(
    sketch: Expression,
    returnArray: Boolean,
    resultType: DataType,
    childType: DataType)
  extends UnaryExpression
  with CodegenFallback {

  override def child: Expression = sketch
  override def dataType: DataType = resultType
  override def nullable: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(sketch = newChild)

  override def eval(input: InternalRow): Any = {
    val sketchRow = child.eval(input).asInstanceOf[InternalRow]
    if (sketchRow == null) return null
    KllSketchHelper.eval(sketchRow, childType)
  }
}
