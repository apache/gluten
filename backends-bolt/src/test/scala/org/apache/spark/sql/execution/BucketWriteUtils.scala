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

import org.apache.spark.sql.{DataFrame, GlutenQueryTest}
import org.apache.spark.sql.catalyst.expressions.{BitwiseAnd, Expression, HiveHash, Literal, Pmod, UnsafeProjection}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestUtils

import java.io.File

trait BucketWriteUtils extends GlutenQueryTest with SQLTestUtils {

  def tableDir(table: String): File = {
    val identifier = spark.sessionState.sqlParser.parseTableIdentifier(table)
    new File(spark.sessionState.catalog.defaultTablePath(identifier))
  }

  protected def testBucketing(
      dataDir: File,
      source: String = "parquet",
      numBuckets: Int,
      bucketCols: Seq[String],
      sortCols: Seq[String] = Nil,
      inputDF: DataFrame,
      bucketIdExpression: (Seq[Expression], Int) => Expression,
      getBucketIdFromFileName: String => Option[Int]): Unit = {
    val allBucketFiles =
      dataDir.listFiles().filterNot(f => f.getName.startsWith(".") || f.getName.startsWith("_"))

    for (bucketFile <- allBucketFiles) {
      val bucketId = getBucketIdFromFileName(bucketFile.getName).getOrElse {
        fail(s"Unable to find the related bucket files.")
      }

      // Remove the duplicate columns in bucketCols and sortCols;
      // Otherwise, we got analysis errors due to duplicate names
      val selectedColumns = (bucketCols ++ sortCols).distinct
      // We may lose the type information after write(e.g. json format doesn't keep schema
      // information), here we get the types from the original dataframe.
      val types = inputDF.select(selectedColumns.map(col): _*).schema.map(_.dataType)
      val columns = selectedColumns.zip(types).map { case (colName, dt) => col(colName).cast(dt) }

      // Read the bucket file into a dataframe, so that it's easier to test.
      val readBack = spark.read
        .format(source)
        .load(bucketFile.getAbsolutePath)
        .select(columns: _*)

      // If we specified sort columns while writing bucket table, make sure the data in this
      // bucket file is already sorted.
      if (sortCols.nonEmpty) {
        checkAnswer(readBack.sort(sortCols.map(col): _*), readBack.collect())
      }

      // Go through all rows in this bucket file, calculate bucket id according to bucket column
      // values, and make sure it equals to the expected bucket id that inferred from file name.
      val qe = readBack.select(bucketCols.map(col): _*).queryExecution
      val rows = qe.toRdd.map(_.copy()).collect()
      val getBucketId = UnsafeProjection.create(
        bucketIdExpression(qe.analyzed.output, numBuckets) :: Nil,
        qe.analyzed.output)

      for (row <- rows) {
        val actualBucketId = getBucketId(row).getInt(0)
        assert(actualBucketId == bucketId)
      }
    }
  }

  def bucketIdExpression(expressions: Seq[Expression], numBuckets: Int): Expression =
    Pmod(BitwiseAnd(HiveHash(expressions), Literal(Int.MaxValue)), Literal(numBuckets))

  def getBucketIdFromFileName(fileName: String): Option[Int] = {
    val hiveBucketedFileName = """^(\d+)_0_.*$""".r
    fileName match {
      case hiveBucketedFileName(bucketId) => Some(bucketId.toInt)
      case _ => None
    }
  }
}
