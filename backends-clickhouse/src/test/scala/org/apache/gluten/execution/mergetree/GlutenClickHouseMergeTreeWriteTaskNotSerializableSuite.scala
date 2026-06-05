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

import org.apache.gluten.backendsapi.clickhouse.{CHConfig, RuntimeSettings}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.CreateMergeTreeSuite

import org.apache.spark.SparkConf

import org.scalatest.DoNotDiscover

@DoNotDiscover
class GlutenClickHouseMergeTreeWriteTaskNotSerializableSuite extends CreateMergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.memory.offHeap.size", "4G")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
  }

  test("GLUTEN-6470: Fix Task not serializable error when inserting mergetree data") {

    val externalSortKey = RuntimeSettings.MAX_BYTES_BEFORE_EXTERNAL_SORT.key
    assertResult(3435973836L)(spark.conf.get(externalSortKey).toLong)

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_task_not_serializable;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_task_not_serializable
                 |(
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$dataHome/lineitem_task_not_serializable'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_task_not_serializable
                 | select * from lineitem
                 |""".stripMargin)

    checkQuery(q1("lineitem_task_not_serializable"))
  }
}
