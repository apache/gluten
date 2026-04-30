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
package org.apache.gluten.execution

import org.apache.spark.SparkConf

import org.scalatest.DoNotDiscover

import java.io.File

@DoNotDiscover
class GlutenClickHouseTPCHNullableSkipIndexSuite extends NullableMergeTreeSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "SNAPPY")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  test("test simple minmax index") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_minmax;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_minmax
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
                 |LOCATION '$dataHome/lineitem_mergetree_minmax'
                 |TBLPROPERTIES('minmaxIndexKey'='l_receiptdate')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_minmax
                 | select * from lineitem
                 |""".stripMargin)

    val df = spark
      .sql(s"""
              |select count(*) from lineitem_mergetree_minmax  where l_receiptdate = '1998-12-27'
              |""".stripMargin)

    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.size == 1)
    val mergetreeScan = scanExec.head
    val ret = df.collect()
    assert(ret.apply(0).get(0) == 1)
    val marks = mergetreeScan.metrics("selectedMarks").value
    assert(marks == 1)

    val directory = new File(s"$dataHome/lineitem_mergetree_minmax")
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(
      partDir.listFiles().exists(p => p.getName.contains("skp_idx__minmax_l_receiptdate.idx2")))
  }

  test("test simple bloom filter index") {

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_bf;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_bf
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
                 |LOCATION '$dataHome/lineitem_mergetree_bf'
                 |TBLPROPERTIES('bloomfilterIndexKey'='l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_bf
                 | select * from lineitem
                 |""".stripMargin)

    val df = spark
      .sql(s"""
              |select count(*) from lineitem_mergetree_bf  where l_orderkey = '600000'
              |""".stripMargin)

    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.size == 1)
    val mergetreeScan = scanExec.head
    val ret = df.collect()
    assert(ret.apply(0).get(0) == 2)
    val marks = mergetreeScan.metrics("selectedMarks").value
    assert(marks == 1)

    val directory = new File(s"$dataHome/lineitem_mergetree_bf")
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(
      partDir.listFiles().exists(p => p.getName.contains("skp_idx__bloomfilter_l_orderkey.idx")))

  }

  test("test simple set index") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_set;
                 |""".stripMargin)

    spark.sql(s"""
               CREATE TABLE IF NOT EXISTS lineitem_mergetree_set
                 |(
                 | l_orderkey      bigint ,
                 | l_partkey       bigint ,
                 | l_suppkey       bigint ,
                 | l_linenumber    bigint ,
                 | l_quantity      double ,
                 | l_extendedprice double ,
                 | l_discount      double ,
                 | l_tax           double ,
                 | l_returnflag    string ,
                 | l_linestatus    string ,
                 | l_shipdate      date ,
                 | l_commitdate    date ,
                 | l_receiptdate   date ,
                 | l_shipinstruct  string ,
                 | l_shipmode      string ,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |LOCATION '$dataHome/lineitem_mergetree_set'
                 |TBLPROPERTIES('setIndexKey'='l_orderkey')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_set
                 | select * from lineitem
                 |""".stripMargin)

    val df = spark
      .sql(s"""
              |select count(*) from lineitem_mergetree_set  where l_orderkey = '600000'
              |""".stripMargin)

    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.size == 1)
    val mergetreeScan = scanExec.head
    val ret = df.collect()
    assert(ret.apply(0).get(0) == 2)
    val marks = mergetreeScan.metrics("selectedMarks").value
    assert(marks == 1)

    val directory = new File(s"$dataHome/lineitem_mergetree_set")
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(partDir.listFiles().exists(p => p.getName.contains("skp_idx__set_l_orderkey.idx")))
  }

  test("test nullable dataset inserted into not null schema") {

    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_mergetree_minmax2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_mergetree_minmax2
                 |(
                 | l_orderkey      bigint not null,
                 | l_partkey       bigint not null,
                 | l_suppkey       bigint not null,
                 | l_linenumber    bigint not null,
                 | l_quantity      double not null,
                 | l_extendedprice double not null,
                 | l_discount      double not null,
                 | l_tax           double not null,
                 | l_returnflag    string not null,
                 | l_linestatus    string not null,
                 | l_shipdate      date not null,
                 | l_commitdate    date not null,
                 | l_receiptdate   date not null,
                 | l_shipinstruct  string not null,
                 | l_shipmode      string not null,
                 | l_comment       string not null
                 |)
                 |USING clickhouse
                 |LOCATION '$dataHome/lineitem_mergetree_minmax2'
                 |TBLPROPERTIES('minmaxIndexKey'='l_receiptdate')
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_mergetree_minmax2
                 | select * from lineitem
                 |""".stripMargin)

    val df = spark
      .sql(s"""
              |select count(*) from lineitem_mergetree_minmax2  where l_receiptdate = '1998-12-27'
              |""".stripMargin)

    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.size == 1)
    val mergetreeScan = scanExec.head
    val ret = df.collect()
    assert(ret.apply(0).get(0) == 1)
    val marks = mergetreeScan.metrics("selectedMarks").value
    assert(marks == 1)

    val directory = new File(s"$dataHome/lineitem_mergetree_minmax2")
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(!partDir.listFiles().exists(p => p.getName.contains("null")))
    assert(
      partDir.listFiles().exists(p => p.getName.contains("skp_idx__minmax_l_receiptdate.idx2")))
  }

  // TODO:
  // 1. auto check visited granule (effectiveness of index)
  // 2. set index is implemented, but not encouraged because we by default does not cap set size
  // 3. need to test minmax/bf/set index on every type (bloom filter on date32, e.g. is not working)
  // 4. complex case where a column has many types of indexes / a type of index on many columns
}
