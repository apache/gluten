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

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, GreaterThan}
import org.apache.spark.sql.catalyst.expressions.{In, IsNotNull, IsNull, LessThan}
import org.apache.spark.sql.catalyst.expressions.{Literal, Or, StartsWith}
// CollationFactory + StringType(collationId) are Spark 4.0+ only.
// Use reflection so this suite still compiles against Spark 3.5 shim.
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for the lazy-split wrapper's stats!=null branch: batches whose [lowerBound, upperBound]
 * does not cover the literal must be pruned; batches whose range covers it must be returned.
 *
 * Pure JVM. Uses GenericInternalRow with the vanilla 5-slot-per-col schema (lower, upper,
 * nullCount, count, sizeInBytes).
 */
class ColumnarCachedBatchBuildFilterPruneSuite extends AnyFunSuite {

  // Build a CachedColumnarBatch with BIGINT 1-col stats [lower, upper].
  private def batchWithStats(numRows: Int, lower: Long, upper: Long): CachedColumnarBatch = {
    val stats = new GenericInternalRow(Array[Any](lower, upper, 0, numRows, numRows * 8L))
    CachedColumnarBatch(
      numRows = numRows,
      sizeInBytes = numRows * 8L,
      bytes = Array.fill[Byte](numRows * 4)(0),
      stats = stats)
  }

  test("EqualTo literal in [lower, upper] keeps the batch") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(50L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val batches: Iterator[CachedBatch] = Iterator(batchWithStats(10, 0L, 100L))
    val result = filter(0, batches).toList

    assert(result.length === 1, "batch with [0, 100] covers literal 50, must be kept")
    assert(result.head.numRows === 10)
  }

  test("EqualTo literal outside [lower, upper] prunes the batch") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(999L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val batches: Iterator[CachedBatch] = Iterator(batchWithStats(10, 0L, 100L))
    val result = filter(0, batches).toList

    assert(result.length === 0, "batch with [0, 100] cannot contain 999, must be pruned")
  }

  test("mixed null/non-null stats: null through, non-null pruned by predicate") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(50L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val nullStats = CachedColumnarBatch(
      numRows = 7,
      sizeInBytes = 28L,
      bytes = Array[Byte](9, 9),
      stats = null)
    val keptBatch = batchWithStats(10, 0L, 100L) // covers 50, kept
    val prunedBatch = batchWithStats(5, 200L, 300L) // does not cover 50, pruned

    val batches: Iterator[CachedBatch] = Iterator(nullStats, prunedBatch, keptBatch)
    val result = filter(0, batches).toList

    // Expected order: nullStats first (direct), then keptBatch.
    // prunedBatch dropped by parent.
    assert(
      result.length === 2,
      s"expected 2 (nullStats + keptBatch), got ${result.length}")
    assert(
      result.map(_.numRows) === Seq(7, 10),
      "order: stats=null pass-through first, then stats-covers-literal kept")
  }

  // Regression: a batch whose stats row is non-null but carries a NULL lower/upper bound for a
  // predicate-referenced column must NOT be pruned. Such per-batch null bounds arise from
  // data-dependent writer demotions invisible to the schema-level stripUnsupportedConjuncts:
  // a binary-collation VARCHAR whose 256B upper-bound prefix is all 0xFF (carry overflow), or a
  // dictionary-encoded numeric on the V2 fallback path. Without the per-batch bypass, vanilla
  // buildFilter evaluates `null <= 999 && 999 <= null` -> null -> coerced false -> the batch is
  // silently dropped (data loss).
  test("null lower/upper bound on referenced column bypasses pruning (batch kept)") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(999L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val nullBoundStats = new GenericInternalRow(Array[Any](null, null, 0, 10, 80L))
    val batch = CachedColumnarBatch(
      numRows = 10,
      sizeInBytes = 80L,
      bytes = Array.fill[Byte](40)(0),
      stats = nullBoundStats)

    val result = filter(0, Iterator[CachedBatch](batch)).toList
    assert(
      result.length === 1,
      "null-bound referenced column must bypass pruning -> batch kept (no silent data loss)")
    assert(result.head.numRows === 10)
  }

  // The bypass must split a contiguous run correctly: a finite-bound batch that the predicate
  // excludes is still pruned, a null-bound batch in the middle is passed through, and a finite
  // covering batch after it is still kept -- no batch double-emitted or skipped.
  test("null-bound bypass splits a contiguous run: pruned dropped, null-bound + covering kept") {
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("id", LongType, nullable = false)()
    val predicate = EqualTo(attr, Literal(999L))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val prunable = batchWithStats(5, 0L, 100L) // finite, excludes 999 -> pruned
    val nullBound = CachedColumnarBatch(
      numRows = 7,
      sizeInBytes = 56L,
      bytes = Array.fill[Byte](28)(0),
      stats = new GenericInternalRow(Array[Any](null, null, 0, 7, 56L)))
    val covering = batchWithStats(9, 900L, 1000L) // finite, covers 999 -> kept

    val result = filter(0, Iterator[CachedBatch](prunable, nullBound, covering)).toList
    assert(
      result.map(_.numRows) === Seq(7, 9),
      "prunable dropped; null-bound bypassed (kept); covering kept -- run split correctly")
  }

  // ---------------------------------------------------------------------------
  // W1-W8 -- non-binary collation StringType wrapper behavior.
  // The wrapper strips AND-conjuncts referencing non-binary collation StringType
  // attributes via splitConjunctivePredicates, leaving binary attribute predicates
  // intact. See ColumnarCachedBatchSerializer.stripUnsupportedConjuncts above.
  // ---------------------------------------------------------------------------

  private val binaryString: StringType = StringType
  // Build StringType("UTF8_LCASE") reflectively -- Spark 4.0+ only.
  // We resolve the companion's `apply(int)` method (Spark types.StringType$.apply(int))
  // rather than the case-class constructor (which has private/defaulted secondary args).
  // Fragile only if Spark renames the companion apply signature; if that happens, this
  // suite would fail loudly at test load on 4.0+, so the break is visible, not silent.
  // On Spark 3.5 there is no collation system; tests guarded by `assume(isCollationAware)`.
  private val isCollationAware: Boolean = {
    try {
      // scalastyle:off classforname
      Class.forName("org.apache.spark.sql.catalyst.util.CollationFactory")
      // scalastyle:on classforname
      true
    } catch { case _: ClassNotFoundException => false }
  }
  private val nbString: StringType = {
    if (!isCollationAware) {
      StringType
    } else {
      // scalastyle:off classforname
      val cf = Class.forName("org.apache.spark.sql.catalyst.util.CollationFactory")
      val moduleCls = Class.forName("org.apache.spark.sql.types.StringType$")
      // scalastyle:on classforname
      val idField = cf.getField("UTF8_LCASE_COLLATION_ID")
      val collationId = idField.getInt(null)
      val module = moduleCls.getField("MODULE$").get(null)
      val applyM = moduleCls.getMethod("apply", java.lang.Integer.TYPE)
      applyM.invoke(module, Integer.valueOf(collationId)).asInstanceOf[StringType]
    }
  }

  // Build a stats row + batch for a single non-binary collation StringType attr.
  private def stringBatch(
      lower: String,
      upper: String,
      numRows: Int = 10): CachedColumnarBatch = {
    val stats = new GenericInternalRow(
      Array[Any](
        UTF8String.fromString(lower),
        UTF8String.fromString(upper),
        0,
        numRows,
        numRows.toLong * 4L))
    CachedColumnarBatch(
      numRows = numRows,
      sizeInBytes = numRows.toLong * 4L,
      bytes = Array.fill[Byte](numRows * 4)(0),
      stats = stats)
  }

  // Build a stats row + batch for [String nb, Int int] schema (5 slots per col).
  private def mixedBatch(
      strLower: String,
      strUpper: String,
      intLower: Int,
      intUpper: Int,
      numRows: Int = 10,
      strNullCount: Int = 0): CachedColumnarBatch = {
    val stats = new GenericInternalRow(
      Array[Any](
        UTF8String.fromString(strLower),
        UTF8String.fromString(strUpper),
        strNullCount,
        numRows,
        numRows.toLong * 4L,
        intLower,
        intUpper,
        0,
        numRows,
        numRows.toLong * 4L
      ))
    CachedColumnarBatch(
      numRows = numRows,
      sizeInBytes = numRows.toLong * 8L,
      bytes = Array.fill[Byte](numRows * 8)(0),
      stats = stats)
  }

  test("W1: wrapper strips predicate on non-binary collation StringType attribute") {
    assume(isCollationAware)
    // Stats range [aaa, aaz] does NOT cover literal "zzz". Without strip: super would
    // generate `lower <= 'zzz' && 'zzz' <= upper` -> false -> drop. With wrapper: stripped
    // -> no predicate -> kept.
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("c", nbString, nullable = false)()
    val predicate = EqualTo(attr, Literal.create("zzz", nbString))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val result = filter(0, Iterator(stringBatch("aaa", "aaz"))).toList
    assert(result.length === 1, "non-binary attr predicate must be stripped -> batch kept")
  }

  test("W2: wrapper preserves predicate on binary collation StringType attribute") {
    assume(isCollationAware)
    // Binary collation: predicate untouched, super applies -> batch pruned.
    val serializer = new ColumnarCachedBatchSerializer
    val attr = AttributeReference("c", binaryString, nullable = false)()
    val predicate = EqualTo(attr, Literal.create("zzz", binaryString))
    val filter = serializer.buildFilter(Seq(predicate), Seq(attr))

    val result = filter(0, Iterator(stringBatch("aaa", "aaz"))).toList
    assert(
      result.length === 0,
      "binary attr predicate preserved -> super prunes batch outside [aaa, aaz]")
  }

  test("W3: mixed-attr conjunct: nb='zzz' AND int>=300 keeps int predicate, batch pruned") {
    assume(isCollationAware)
    // Stats: nb=[aaa, aaz], int=[100, 200]. Predicate And(nb='zzz', int>=300).
    // Conjunct-level strip: nb stripped, int>=300 remains -> super applies
    //   `300 <= upperBound(200)` -> false -> drop. We assert drop to prove conjunct level.
    val serializer = new ColumnarCachedBatchSerializer
    val nbAttr = AttributeReference("nb", nbString, nullable = false)()
    val intAttr = AttributeReference("i", IntegerType, nullable = false)()
    val pred = And(
      EqualTo(nbAttr, Literal.create("zzz", nbString)),
      GreaterThan(intAttr, Literal(300)))
    val filter = serializer.buildFilter(Seq(pred), Seq(nbAttr, intAttr))

    val result = filter(0, Iterator(mixedBatch("aaa", "aaz", 100, 200))).toList
    assert(
      result.length === 0,
      "conjunct-level strip: int>=300 survives strip -> batch w/ int=[100,200] pruned")
  }

  test("W4: nested And: nb='zzz' AND (int>=300 AND int<=400) splits deeply, batch pruned") {
    assume(isCollationAware)
    // Stats: nb=[aaa, aaz], int=[100, 200]. Predicate And(nb='zzz', And(int>=300, int<=400)).
    // splitConjunctivePredicates must unpack nested And: 3 conjuncts -> strip nb -> keep
    // [int>=300, int<=400] -> reduce(And) -> super prunes (int=[100,200] disjoint from [300,400]).
    val serializer = new ColumnarCachedBatchSerializer
    val nbAttr = AttributeReference("nb", nbString, nullable = false)()
    val intAttr = AttributeReference("i", IntegerType, nullable = false)()
    val pred = And(
      EqualTo(nbAttr, Literal.create("zzz", nbString)),
      And(GreaterThan(intAttr, Literal(300)), LessThan(intAttr, Literal(400))))
    val filter = serializer.buildFilter(Seq(pred), Seq(nbAttr, intAttr))

    val result = filter(0, Iterator(mixedBatch("aaa", "aaz", 100, 200))).toList
    assert(
      result.length === 0,
      "splitConjunctivePredicates unpacks nested And -> int conjuncts survive -> batch pruned")
  }

  test("W5: Or branch: Or(nb='zzz', int<150) stripped entirely (Or conservative), batch kept") {
    assume(isCollationAware)
    // Stats: nb=[aaa, aaz], int=[300, 400]. Without wrapper:
    //   nb branch: 'aaa' <= 'zzz' && 'zzz' <= 'aaz' -> false (collation-dependent)
    //   int branch: lowerBound(300) < 150 -> false
    //   Or = false -> drop.
    // With wrapper: Or references nb -> entire Or stripped (splitConjunctivePredicates
    // does not split Or) -> kept=empty -> no predicate -> batch kept.
    val serializer = new ColumnarCachedBatchSerializer
    val nbAttr = AttributeReference("nb", nbString, nullable = false)()
    val intAttr = AttributeReference("i", IntegerType, nullable = false)()
    val pred = Or(
      EqualTo(nbAttr, Literal.create("zzz", nbString)),
      LessThan(intAttr, Literal(150)))
    val filter = serializer.buildFilter(Seq(pred), Seq(nbAttr, intAttr))

    val result = filter(0, Iterator(mixedBatch("aaa", "aaz", 300, 400))).toList
    assert(
      result.length === 1,
      "Or containing nb attr stripped wholesale -> no predicate -> batch kept (pass-through)")
  }

  test("W6: IsNull(nb attr) stripped, IsNotNull(int) kept, batch evaluated by int only") {
    assume(isCollationAware)
    // Stats: nb nullCount=0, int has rows. Predicates [IsNull(nb), IsNotNull(int)].
    // Without wrapper: IsNull(nb) -> nullCount>0 -> 0>0=false -> batch dropped.
    // With wrapper: IsNull(nb) stripped; IsNotNull(int) survives -> count-nullCount>0 ->
    //   10-0=10>0=true -> kept.
    val serializer = new ColumnarCachedBatchSerializer
    val nbAttr = AttributeReference("nb", nbString, nullable = true)()
    val intAttr = AttributeReference("i", IntegerType, nullable = true)()
    val preds: Seq[Expression] = Seq(IsNull(nbAttr), IsNotNull(intAttr))
    val filter = serializer.buildFilter(preds, Seq(nbAttr, intAttr))

    val result = filter(0, Iterator(mixedBatch("aaa", "aaz", 100, 200))).toList
    assert(
      result.length === 1,
      "IsNull(nb) stripped -> only IsNotNull(int) drives decision -> kept")
  }

  test("W7: In(nb attr, list) and StartsWith(nb attr, lit) both stripped, batch kept") {
    assume(isCollationAware)
    // Both predicates reference nb only -> both stripped -> no predicate -> kept.
    val serializer = new ColumnarCachedBatchSerializer
    val nbAttr = AttributeReference("nb", nbString, nullable = false)()
    val preds: Seq[Expression] = Seq(
      In(nbAttr, Seq(Literal.create("xx", nbString), Literal.create("yy", nbString))),
      StartsWith(nbAttr, Literal.create("zz", nbString)))
    val filter = serializer.buildFilter(preds, Seq(nbAttr))

    val result = filter(0, Iterator(stringBatch("aaa", "aaz"))).toList
    assert(result.length === 1, "In and StartsWith on nb attr both stripped -> batch kept")
  }

  test("W8 (anti-regression): bypassing wrapper would let UTF8_LCASE bound prune batch") {
    assume(isCollationAware)
    // Anchors the non-binary collation attack scenario: if the wrapper is ever
    // moved/removed, super's partition filter on a non-binary collation attr
    // uses cpp-written byte-order bounds in a collation-aware comparator --
    // behavior is implementation-defined and can drop valid rows. Use vanilla
    // DefaultCachedBatchSerializer (extends SimpleMetricsCachedBatchSerializer,
    // no wrapper) to demonstrate the unsafe path.
    val vanilla = new org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
    val attr = AttributeReference("c", nbString, nullable = false)()
    val predicate = EqualTo(attr, Literal.create("zzz", nbString))
    val filter = vanilla.buildFilter(Seq(predicate), Seq(attr))

    val batch = stringBatch("aaa", "aaz")
    val result = filter(0, Iterator[CachedBatch](batch)).toList
    // Vanilla (no wrapper): super applies the predicate via collation-aware comparator.
    // For UTF8_LCASE 'aaa' <= 'zzz' && 'zzz' <= 'aaz' -> false -> batch dropped.
    // Asserting drop locks in the negative ground truth -- wrapper is the only thing
    // standing between user data and silent loss for non-binary collation columns.
    assert(
      result.length === 0,
      "without wrapper: non-binary attr predicate applied -> batch dropped")
  }
}
