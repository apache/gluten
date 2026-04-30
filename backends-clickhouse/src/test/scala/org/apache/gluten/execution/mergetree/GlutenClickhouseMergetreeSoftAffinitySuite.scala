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
package org.apache.gluten.execution.mergetree

import org.apache.gluten.affinity.{CHUTAffinity, CHUTSoftAffinityManager}
import org.apache.gluten.execution.{GlutenClickHouseWholeStageTransformerSuite, GlutenMergeTreePartition, MergeTreePartSplit}

import org.apache.spark.{Partition, SparkConf}
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.execution.datasources.clickhouse.utils.MergeTreePartsPartitionsUtil
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.Path
import org.scalatest.DoNotDiscover

import java.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@DoNotDiscover
class GlutenClickhouseMergetreeSoftAffinitySuite
  extends GlutenClickHouseWholeStageTransformerSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    assertResult(0)(CHUTSoftAffinityManager.nodesExecutorsMap.size)
    CHUTSoftAffinityManager.handleExecutorAdded(("1", "host-1"))
    CHUTSoftAffinityManager.handleExecutorAdded(("2", "host-2"))
    CHUTSoftAffinityManager.handleExecutorAdded(("3", "host-3"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    CHUTSoftAffinityManager.handleExecutorRemoved("1")
    CHUTSoftAffinityManager.handleExecutorRemoved("2")
    CHUTSoftAffinityManager.handleExecutorRemoved("3")
    assertResult(0)(CHUTSoftAffinityManager.nodesExecutorsMap.size)
  }

  test("Soft Affinity Scheduler with duplicate reading detection") {

    val partitions: ArrayBuffer[Partition] = new ArrayBuffer[Partition]()
    var splitFiles: Seq[MergeTreePartSplit] = Seq()
    val relativeTablePath = "tmp/"

    for (i <- 1 to 10) {
      splitFiles =
        splitFiles :+ MergeTreePartSplit(i.toString, i.toString, i.toString, i, 30L, 40L, "", "")
    }

    val (partNameWithLocation, locationDistinct) =
      calculatedLocationForSoftAffinity(splitFiles, relativeTablePath)

    MergeTreePartsPartitionsUtil.genInputPartitionSeqBySplitFiles(
      "mergetree",
      "test",
      "test_table",
      "123",
      relativeTablePath,
      "/tmp",
      StructType(Seq()),
      partitions,
      new ClickHouseTableV2(spark, new Path("/")),
      mutable.Map[String, String]().toMap,
      splitFiles,
      1,
      1000,
      partNameWithLocation,
      locationDistinct
    )

    assertResult(2)(partitions.size)

    for (partition <- partitions) {
      val names =
        partition
          .asInstanceOf[GlutenMergeTreePartition]
          .partList
          .map(_.name.toInt)
          .sorted
          .mkString(",")
      assert(names == "1,2,3,5,6,7" | names == "4,8,9,10")
    }
  }

  def calculatedLocationForSoftAffinity(
      splits: Seq[MergeTreePartSplit],
      relativeTablePath: String): (util.HashMap[String, String], util.HashSet[String]) = {
    val partNameWithLocation = new util.HashMap[String, String]()
    val locationDistinct = new util.HashSet[String]()

    splits.foreach(
      part => {
        if (!partNameWithLocation.containsKey(part.name)) {
          val locations = CHUTAffinity.getNativeMergeTreePartLocations(part.name, relativeTablePath)
          val localtionKey = locations.sorted.mkString(",")
          locationDistinct.add(localtionKey)
          partNameWithLocation.put(part.name, localtionKey)
        }
      })
    (partNameWithLocation, locationDistinct)
  }
}
