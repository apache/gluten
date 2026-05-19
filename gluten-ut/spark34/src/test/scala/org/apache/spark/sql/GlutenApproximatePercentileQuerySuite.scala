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

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.Decimal

import java.sql.{Date, Timestamp}

/**
 * Gluten override of ApproximatePercentileQuerySuite.
 *
 * Velox uses KLL sketch algorithm while Spark uses GK algorithm for approx_percentile. Both
 * algorithms are approximate and produce results within the error bound, but they may select
 * different concrete values within that bound. For example, for integers 1..1000, the exact 25th
 * percentile is 250.25 - GK returns 250 while KLL may return 251. This inherent algorithm
 * difference cannot be eliminated by increasing precision.
 *
 * Tests that compare exact values are overridden with tolerance-based assertions. Tests that don't
 * depend on exact approximate values (empty input, null handling, etc.) are inherited from the
 * parent suite without changes.
 */
class GlutenApproximatePercentileQuerySuite
  extends ApproximatePercentileQuerySuite
  with GlutenSQLTestsTrait {
  import testImplicits._

  override def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }

  // Ignore parent test that does exact value comparison - KLL vs GK produces off-by-one results.
  override def testNameBlackList: Seq[String] = Seq(
    "percentile_approx, different column types"
  )

  private val ptable = "percentile_approx"

  // KLL vs GK algorithm may pick different values at percentile boundaries.
  // For N=1000, the difference is typically 1-2 elements.
  private val kllTolerance = 2

  private def assertApproxEqual(actual: Any, expected: Double, tolerance: Double): Unit = {
    val actualDouble = actual match {
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case d: Double => d
      case d: java.math.BigDecimal => d.doubleValue()
      case d: Decimal => d.toDouble
      case d: Date => DateTimeUtils.fromJavaDate(d).toDouble
      case t: Timestamp => DateTimeUtils.fromJavaTimestamp(t).toDouble
      case other =>
        throw new IllegalArgumentException(s"Unexpected type: ${other.getClass} value=$other")
    }
    assert(
      Math.abs(actualDouble - expected) <= tolerance,
      s"Expected $expected +/- $tolerance, but got $actualDouble")
  }

  private def assertApproxSeqEqual(actual: Any, expected: Seq[Double], tolerance: Double): Unit = {
    val actualSeq = actual match {
      case s: Seq[_] => s
      case t: Traversable[_] => t.toSeq
      case other =>
        throw new IllegalArgumentException(s"Unexpected collection type: ${other.getClass}")
    }
    assert(
      actualSeq.length == expected.length,
      s"Length mismatch: got ${actualSeq.length}, expected ${expected.length}")
    actualSeq.zip(expected).foreach { case (a, e) => assertApproxEqual(a, e, tolerance) }
  }

  // Override: KLL and GK algorithms produce slightly different results for decimal/date/timestamp.
  testGluten("percentile_approx, different column types") {
    withTempView(ptable) {
      val intSeq = 1 to 1000
      val data: Seq[(java.math.BigDecimal, Date, Timestamp)] = intSeq.map {
        i =>
          (
            new java.math.BigDecimal(i),
            DateTimeUtils.toJavaDate(i),
            DateTimeUtils.toJavaTimestamp(i))
      }
      data.toDF("cdecimal", "cdate", "ctimestamp").createOrReplaceTempView(ptable)
      val result = spark
        .sql(s"""SELECT
                |  percentile_approx(cdecimal, array(0.25, 0.5, 0.75D)),
                |  percentile_approx(cdate, array(0.25, 0.5, 0.75D)),
                |  percentile_approx(ctimestamp, array(0.25, 0.5, 0.75D))
                |FROM $ptable
         """.stripMargin)
        .collect()
        .head
      assertApproxSeqEqual(result.get(0), Seq(250.0, 500.0, 750.0), kllTolerance)
      assertApproxSeqEqual(result.get(1), Seq(250.0, 500.0, 750.0), kllTolerance)
      assertApproxSeqEqual(result.get(2), Seq(250.0, 500.0, 750.0), kllTolerance)
    }
  }

  // Override: KLL and GK algorithms may select different values at percentile boundaries.
  // For 1..1000, exact 25th percentile = 250.25; GK returns 250, KLL may return 251.
  testGluten("percentile_approx, single percentile value") {
    withTempView(ptable) {
      (1 to 1000).toDF("col").createOrReplaceTempView(ptable)
      val result = spark
        .sql(s"""
                |SELECT
                |  percentile_approx(col, 0.25),
                |  percentile_approx(col, 0.5),
                |  percentile_approx(col, 0.75d),
                |  percentile_approx(col, 0.0),
                |  percentile_approx(col, 1.0),
                |  percentile_approx(col, 0),
                |  percentile_approx(col, 1)
                |FROM $ptable
         """.stripMargin)
        .collect()
        .head
      assertApproxEqual(result.get(0), 250.0, kllTolerance)
      assertApproxEqual(result.get(1), 500.0, kllTolerance)
      assertApproxEqual(result.get(2), 750.0, kllTolerance)
      assertApproxEqual(result.get(3), 1.0, 0) // min is exact
      assertApproxEqual(result.get(4), 1000.0, 0) // max is exact
      assertApproxEqual(result.get(5), 1.0, 0)
      assertApproxEqual(result.get(6), 1000.0, 0)
    }
  }

  // Override: small dataset (10 elements) - KLL and GK may differ by 1.
  testGluten("percentile_approx, the first element satisfies small percentages") {
    withTempView(ptable) {
      (1 to 10).toDF("col").createOrReplaceTempView(ptable)
      val result = spark
        .sql(s"""
                |SELECT
                |  percentile_approx(col, array(0.01, 0.1, 0.11))
                |FROM $ptable
         """.stripMargin)
        .collect()
        .head
      assertApproxSeqEqual(result.get(0), Seq(1.0, 1.0, 2.0), 1)
    }
  }

  // Override: same boundary difference as "single percentile value".
  testGluten("percentile_approx, array of percentile value") {
    withTempView(ptable) {
      (1 to 1000).toDF("col").createOrReplaceTempView(ptable)
      val result = spark
        .sql(s"""SELECT
                |  percentile_approx(col, array(0.25, 0.5, 0.75D)),
                |  count(col),
                |  percentile_approx(col, array(0.0, 1.0)),
                |  sum(col)
                |FROM $ptable
         """.stripMargin)
        .collect()
        .head
      assertApproxSeqEqual(result.get(0), Seq(250.0, 500.0, 750.0), kllTolerance)
      assert(result.get(1) === 1000L)
      assertApproxSeqEqual(result.get(2), Seq(1.0, 1000.0), 0) // min/max are exact
      assert(result.get(3) === 500500L)
    }
  }

  // Override: KLL error bound formula differs from GK's floor(N/accuracy).
  testGluten("percentile_approx, with different accuracies") {
    withTempView(ptable) {
      val tableCount = 1000
      (1 to tableCount).toDF("col").createOrReplaceTempView(ptable)

      val accuracies = Array(1, 10, 100, 1000, 10000)
      val expectedPercentiles = Array(100d, 200d, 250d, 314d, 777d)
      for (accuracy <- accuracies) {
        for (expectedPercentile <- expectedPercentiles) {
          val df = spark.sql(s"""SELECT
                                | percentile_approx(col, $expectedPercentile/$tableCount, $accuracy)
                                |FROM $ptable
             """.stripMargin)
          val approximatePercentile = df.collect().head.getInt(0)
          val error = Math.abs(approximatePercentile - expectedPercentile)
          val maxError =
            math.max(math.floor(tableCount.toDouble / accuracy.toDouble), kllTolerance.toDouble)
          assert(
            error <= maxError,
            s"accuracy=$accuracy, expected=$expectedPercentile, " +
              s"actual=$approximatePercentile, error=$error, maxError=$maxError")
        }
      }
    }
  }

  // Override: same boundary difference.
  testGluten(
    "percentile_approx, supports constant folding for parameter accuracy and " +
      "percentages") {
    withTempView(ptable) {
      (1 to 1000).toDF("col").createOrReplaceTempView(ptable)
      val result = spark
        .sql(s"SELECT percentile_approx(col, array(0.25 + 0.25D), 200 + 800) FROM $ptable")
        .collect()
        .head
      assertApproxSeqEqual(result.get(0), Seq(500.0), kllTolerance)
    }
  }

  // Override: same boundary difference with null-mixed input.
  testGluten("percentile_approx(col, ...), input rows contains null, with out group by") {
    withTempView(ptable) {
      (1 to 1000)
        .map(Integer.valueOf(_))
        .flatMap(Seq(null: Integer, _))
        .toDF("col")
        .createOrReplaceTempView(ptable)
      val result = spark
        .sql(s"""SELECT
                |  percentile_approx(col, 0.5),
                |  sum(null),
                |  percentile_approx(col, 0.5)
                |FROM $ptable
           """.stripMargin)
        .collect()
        .head
      assertApproxEqual(result.get(0), 500.0, kllTolerance)
      assert(result.get(1) === null)
      assertApproxEqual(result.get(2), 500.0, kllTolerance)
    }
  }

  // Override: same boundary difference with null-mixed group by input.
  testGluten("percentile_approx(col, ...), input rows contains null, with group by") {
    withTempView(ptable) {
      (1 to 1000)
        .map(Integer.valueOf(_))
        .map(v => (Integer.valueOf(v % 2), v))
        .flatMap(Seq(_, (null: Integer, null: Integer)))
        .toDF("key", "value")
        .createOrReplaceTempView(ptable)
      val rows = spark
        .sql(s"""SELECT
                |  percentile_approx(value, 0.5),
                |  sum(value),
                |  percentile_approx(value, 0.5)
                |FROM $ptable
                |GROUP BY key
           """.stripMargin)
        .collect()
        .sortBy(r => if (r.isNullAt(1)) Long.MaxValue else r.getLong(1))
      // key=1 (odd): values 1,3,5,...,999 -> sum=250000, median~=499
      assertApproxEqual(rows(0).get(0), 499.0, kllTolerance)
      assert(rows(0).get(1) === 250000L)
      assertApproxEqual(rows(0).get(2), 499.0, kllTolerance)
      // key=0 (even): values 2,4,6,...,1000 -> sum=250500, median~=500
      assertApproxEqual(rows(1).get(0), 500.0, kllTolerance)
      assert(rows(1).get(1) === 250500L)
      assertApproxEqual(rows(1).get(2), 500.0, kllTolerance)
      // null group
      assert(rows(2).get(0) === null)
      assert(rows(2).get(1) === null)
      assert(rows(2).get(2) === null)
    }
  }
}
