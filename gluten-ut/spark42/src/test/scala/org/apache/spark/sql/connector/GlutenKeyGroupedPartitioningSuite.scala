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
package org.apache.spark.sql.connector

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ShuffledHashJoinExecTransformerBase, SortExecTransformer, SortMergeJoinExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.SortedMergeCoalescedRDD
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.connector.distributions.Distributions
import org.apache.spark.sql.connector.expressions.{FieldReference, NullOrdering, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.Expressions.{bucket, days, identity, sort, years}
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, RDDScanExec, RDDScanTransformer, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.GroupPartitionsExec
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util.Collections

class GlutenKeyGroupedPartitioningSuite
  extends KeyGroupedPartitioningSuite
  with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf = {
    // Native SQL configs
    super.sparkConf
      .set(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key, "false")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.shuffle.partitions", "5")
  }

  private val emptyProps: java.util.Map[String, String] = {
    Collections.emptyMap[String, String]
  }

  private val columns: Array[Column] = Array(
    Column.create("id", IntegerType),
    Column.create("data", StringType),
    Column.create("ts", TimestampType))

  private val columns2: Array[Column] = Array(
    Column.create("store_id", IntegerType),
    Column.create("dept_id", IntegerType),
    Column.create("data", StringType))

  private def createTable(
      table: String,
      columns: Array[Column],
      partitions: Array[Transform],
      ordering: Array[SortOrder] = Array.empty,
      catalog: InMemoryTableCatalog = catalog): Unit = {
    catalog.createTable(
      Identifier.of(Array("ns"), table),
      columns,
      partitions,
      emptyProps,
      Distributions.unspecified(),
      ordering,
      None,
      None,
      numRowsPerSplit = 1)
  }

  private def collectColumnarShuffleExchangeExec(
      plan: SparkPlan): Seq[ColumnarShuffleExchangeExec] = {
    // here we skip collecting shuffle operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExecTransformer => s
      case s: SortMergeJoinExec => s
    }.flatMap(smj => collect(smj) { case s: ColumnarShuffleExchangeExec => s })
  }

  override protected def collectShuffles(plan: SparkPlan): Seq[ShuffleExchangeLike] = {
    // here we skip collecting shuffle operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExec => s
      case s: SortMergeJoinExecTransformer => s
    }.flatMap(
      smj =>
        collect(smj) {
          case s: ShuffleExchangeExec => s
          case s: ColumnarShuffleExchangeExec => s
        })
  }

  override protected def collectGroupPartitions(plan: SparkPlan): Seq[GroupPartitionsExec] = {
    // here we skip collecting group partitions operators that are not associated with SMJ
    collect(plan) {
      case s: SortMergeJoinExec => s
      case s: SortMergeJoinExecTransformer => s
    }.flatMap(smj => collect(smj) { case g: GroupPartitionsExec => g })
  }.toSet.toSeq

  override protected def collectAllShuffles(plan: SparkPlan): Seq[ShuffleExchangeLike] = {
    collect(plan) {
      case s: ShuffleExchangeExec => s
      case s: ColumnarShuffleExchangeExec => s
    }
  }

  private def collectAllColumnarShuffles(plan: SparkPlan): Seq[ColumnarShuffleExchangeExec] = {
    collect(plan) { case s: ColumnarShuffleExchangeExec => s }
  }

  private def collectVanillaShuffles(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    collect(plan) { case s: ShuffleExchangeExec => s }
  }

  private def selectWithMergeJoinHint(t1: String, t2: String): String = {
    s"SELECT /*+ MERGE($t1, $t2) */ "
  }

  private def createJoinTestDF(
      keys: Seq[(String, String)],
      extraColumns: Seq[String] = Nil,
      joinType: String = ""): DataFrame = {
    val extraColList = if (extraColumns.isEmpty) "" else extraColumns.mkString(", ", ", ", "")
    sql(s"""
           |${selectWithMergeJoinHint("i", "p")}
           |id, name, i.price as purchase_price, p.price as sale_price $extraColList
           |FROM testcat.ns.$items i $joinType JOIN testcat.ns.$purchases p
           |ON ${keys.map(k => s"i.${k._1} = p.${k._2}").mkString(" AND ")}
           |ORDER BY id, purchase_price, sale_price $extraColList
           |""".stripMargin)
  }

  private val customers: String = "customers"
  private val customersColumns: Array[Column] = Array(
    Column.create("customer_name", StringType),
    Column.create("customer_age", IntegerType),
    Column.create("customer_id", LongType))

  private val orders: String = "orders"
  private val ordersColumns: Array[Column] =
    Array(Column.create("order_amount", DoubleType), Column.create("customer_id", LongType))

  private def testWithCustomersAndOrders(
      customers_partitions: Array[Transform],
      orders_partitions: Array[Transform],
      expectedNumOfShuffleExecs: Int): Unit = {
    createTable(customers, customersColumns, customers_partitions)
    sql(
      s"INSERT INTO testcat.ns.$customers VALUES " +
        s"('aaa', 10, 1), ('bbb', 20, 2), ('ccc', 30, 3)")

    createTable(orders, ordersColumns, orders_partitions)
    sql(
      s"INSERT INTO testcat.ns.$orders VALUES " +
        s"(100.0, 1), (200.0, 1), (150.0, 2), (250.0, 2), (350.0, 2), (400.50, 3)")

    val df = sql(
      "SELECT customer_name, customer_age, order_amount " +
        s"FROM testcat.ns.$customers c JOIN testcat.ns.$orders o " +
        "ON c.customer_id = o.customer_id ORDER BY c.customer_id, order_amount")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.length == expectedNumOfShuffleExecs)

    checkAnswer(
      df,
      Seq(
        Row("aaa", 10, 100.0),
        Row("aaa", 10, 200.0),
        Row("bbb", 20, 150.0),
        Row("bbb", 20, 250.0),
        Row("bbb", 20, 350.0),
        Row("ccc", 30, 400.50)))
  }

  testGluten("partitioned join: only one side reports partitioning") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(2, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 2)
  }
  testGluten("partitioned join: exact distribution (same number of buckets) from both sides") {
    val customers_partitions = Array(bucket(4, "customer_id"))
    val orders_partitions = Array(bucket(4, "customer_id"))

    testWithCustomersAndOrders(customers_partitions, orders_partitions, 0)
  }

  private val items: String = "items"
  private val itemsColumns: Array[Column] = Array(
    Column.create("id", LongType),
    Column.create("name", StringType),
    Column.create("price", FloatType),
    Column.create("arrive_time", TimestampType))
  private val purchases: String = "purchases"
  private val purchasesColumns: Array[Column] = Array(
    Column.create("item_id", LongType),
    Column.create("price", FloatType),
    Column.create("time", TimestampType))

  testGluten(
    "SPARK-41413: partitioned join: partition values" +
      " from one side are subset of those from the other side") {
    val items_partitions = Array(bucket(4, "id"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(4, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)

    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
        }
    }
  }

  testGluten("SPARK-41413: partitioned join: partition values from both sides overlaps") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 19.5, cast('2020-02-01' as timestamp)), " +
        "(4, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 19.5)))
        }
    }
  }

  testGluten("SPARK-41413: partitioned join: non-overlapping partition values from both sides") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(4, 42.0, cast('2020-01-01' as timestamp)), " +
        "(5, 19.5, cast('2020-02-01' as timestamp)), " +
        "(6, 30.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = sql(
            "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
              s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
              "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq.empty)
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered:" +
      " with same partition keys and one side fully clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 3)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              assert(shuffles.isEmpty, "should not contain any shuffle")
              if (pushDownValues) {
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered:" +
      " with same partition keys and both sides partially clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 22.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 7), ("false", 3)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              assert(shuffles.isEmpty, "should not contain any shuffle")
              if (pushDownValues) {
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 40.0, 55.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(1, "aa", 41.0, 55.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 10.0, 22.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered: with different" +
      " partition keys and both sides partially clustered") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(1, 55.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 10), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 40.0, 55.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(1, "aa", 41.0, 55.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 10.0, 25.0),
                  Row(2, "bb", 10.0, 30.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered: with different" +
      " partition keys and missing keys on left-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(4, 'dd', 18.0, cast('2023-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 50.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 25.0, cast('2020-01-03' as timestamp)), " +
        s"(2, 30.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 9), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 40.0, 50.0),
                  Row(1, "aa", 41.0, 45.0),
                  Row(1, "aa", 41.0, 50.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten(
    "SPARK-42038: partially clustered:" +
      " with different partition keys and missing keys on right-hand side") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 15.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-03' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 6), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(Row(2, "bb", 10.0, 15.0), Row(2, "bb", 10.0, 20.0), Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten("SPARK-42038: partially clustered: left outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 15.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // In a left-outer join, and when the left side has larger stats, partially clustered
    // distribution should kick in and pick the right hand side to replicate partitions.
    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 7), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i LEFT JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, null),
                  Row(1, "aa", 41.0, null),
                  Row(2, "bb", 10.0, 20.0),
                  Row(2, "bb", 15.0, null),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten("SPARK-42038: partially clustered: right outer join") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 20.0, cast('2020-02-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-02-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    // The left-hand side is picked as the side to replicate partitions based on stats, but since
    // this is right outer join, partially clustered distribution won't kick in, and Spark should
    // only push down partition values on both side.
    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i RIGHT JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(null, null, null, 25.0),
                  Row(null, null, null, 30.0),
                  Row(1, "aa", 40.0, 45.0),
                  Row(2, "bb", 10.0, 15.0),
                  Row(2, "bb", 10.0, 20.0),
                  Row(3, "cc", 15.5, 20.0)))
            }
        }
    }
  }

  testGluten("SPARK-42038: partially clustered: full outer join is not applicable") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 45.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 15.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 20.0, cast('2020-01-02' as timestamp)), " +
        s"(3, 20.0, cast('2020-01-01' as timestamp)), " +
        s"(4, 25.0, cast('2020-01-01' as timestamp)), " +
        s"(5, 30.0, cast('2023-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(("true", 5), ("false", 5)).foreach {
          case (enable, expected) =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> false.toString,
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> enable
            ) {
              val df = sql(
                "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
                  s"FROM testcat.ns.$items i FULL OUTER JOIN testcat.ns.$purchases p " +
                  "ON i.id = p.item_id AND i.arrive_time = p.time " +
                  "ORDER BY id, purchase_price, sale_price")

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              if (pushDownValues) {
                assert(shuffles.isEmpty, "should not contain any shuffle")
                val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                  .map(_.outputPartitioning.numPartitions)
                assert(
                  groupPartitions.nonEmpty && groupPartitions.forall(_ == expected),
                  s"Expected $expected but got $groupPartitions")
              } else {
                assert(
                  shuffles.nonEmpty,
                  "should contain shuffle when not pushing down partition values")
              }
              checkAnswer(
                df,
                Seq(
                  Row(null, null, null, 20.0),
                  Row(null, null, null, 25.0),
                  Row(null, null, null, 30.0),
                  Row(1, "aa", 40.0, 45.0),
                  Row(1, "aa", 41.0, null),
                  Row(2, "bb", 10.0, 15.0),
                  Row(3, "cc", 15.5, 20.0)
                )
              )
            }
        }
    }
  }

  testGluten("SPARK-44641: duplicated records when SPJ is not triggered") {
    val items_partitions = Array(bucket(8, "id"))
    createTable(items, itemsColumns, items_partitions)
    sql(s"""
        INSERT INTO testcat.ns.$items VALUES
        (1, 'aa', 40.0, cast('2020-01-01' as timestamp)),
        (1, 'aa', 41.0, cast('2020-01-15' as timestamp)),
        (2, 'bb', 10.0, cast('2020-01-01' as timestamp)),
        (2, 'bb', 10.5, cast('2020-01-01' as timestamp)),
        (3, 'cc', 15.5, cast('2020-02-01' as timestamp))""")

    val purchases_partitions = Array(bucket(8, "item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(s"""INSERT INTO testcat.ns.$purchases VALUES
        (1, 42.0, cast('2020-01-01' as timestamp)),
        (1, 44.0, cast('2020-01-15' as timestamp)),
        (1, 45.0, cast('2020-01-15' as timestamp)),
        (2, 11.0, cast('2020-01-01' as timestamp)),
        (3, 19.5, cast('2020-02-01' as timestamp))""")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(true, false).foreach {
          partiallyClusteredEnabled =>
            withSQLConf(
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClusteredEnabled.toString
            ) {

              // join keys are not the same as the partition keys, therefore SPJ is not triggered.
              val df = sql(s"""
               SELECT id, name, i.price as purchase_price, p.item_id, p.price as sale_price
               FROM testcat.ns.$items i JOIN testcat.ns.$purchases p
               ON i.arrive_time = p.time ORDER BY id, purchase_price, p.item_id, sale_price
               """)

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              assert(shuffles.nonEmpty, "shuffle should exist when SPJ is not used")

              checkAnswer(
                df,
                Seq(
                  Row(1, "aa", 40.0, 1, 42.0),
                  Row(1, "aa", 40.0, 2, 11.0),
                  Row(1, "aa", 41.0, 1, 44.0),
                  Row(1, "aa", 41.0, 1, 45.0),
                  Row(2, "bb", 10.0, 1, 42.0),
                  Row(2, "bb", 10.0, 2, 11.0),
                  Row(2, "bb", 10.5, 1, 42.0),
                  Row(2, "bb", 10.5, 2, 11.0),
                  Row(3, "cc", 15.5, 3, 19.5)
                )
              )
            }
        }
    }
  }

  testGluten("partitioned join:  join with two partition keys and matching & sorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(
      df,
      Seq(
        Row(1, "aa", 40.0, 42.0),
        Row(1, "aa", 41.0, 44.0),
        Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0),
        Row(2, "bb", 10.5, 11.0),
        Row(3, "cc", 15.5, 19.5)))
  }

  testGluten("partitioned join: join with two partition keys and unsorted partitions") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    val df = sql(
      "SELECT id, name, i.price as purchase_price, p.price as sale_price " +
        s"FROM testcat.ns.$items i JOIN testcat.ns.$purchases p " +
        "ON i.id = p.item_id AND i.arrive_time = p.time ORDER BY id, purchase_price, sale_price")

    val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
    assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
    checkAnswer(
      df,
      Seq(
        Row(1, "aa", 40.0, 42.0),
        Row(1, "aa", 41.0, 44.0),
        Row(1, "aa", 41.0, 45.0),
        Row(2, "bb", 10.0, 11.0),
        Row(2, "bb", 10.5, 11.0),
        Row(3, "cc", 15.5, 19.5)))
  }

  testGluten("partitioned join: join with two partition keys and different # of partition keys") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        withSQLConf(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
          val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
          val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
          if (pushDownValues) {
            assert(shuffles.isEmpty, "should not add shuffle when partition values mismatch")
          } else {
            assert(
              shuffles.nonEmpty,
              "should add shuffle when partition values mismatch, and " +
                "pushing down partition values is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(2, "bb", 10.0, 11.0)))
        }
    }
  }

  testGluten("data source partitioning + dynamic partition filtering") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "10"
    ) {
      val items_partitions = Array(identity("id"))
      createTable(items, itemsColumns, items_partitions)
      sql(
        s"INSERT INTO testcat.ns.$items VALUES " +
          s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
          s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
          s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

      val purchases_partitions = Array(identity("item_id"))
      createTable(purchases, purchasesColumns, purchases_partitions)
      sql(
        s"INSERT INTO testcat.ns.$purchases VALUES " +
          s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
          s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
          s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
          s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
          s"(3, 19.5, cast('2020-02-01' as timestamp))")

      Seq(true, false).foreach {
        pushDownValues =>
          withSQLConf(
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString) {
            // number of unique partitions changed after dynamic filtering - the gap
            // should be filled with empty partitions and the job should still succeed
            var df = sql(
              s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p " +
                "WHERE i.id = p.item_id AND i.price > 40.0")
            checkAnswer(df, Seq(Row(131)))

            // dynamic filtering doesn't change partitioning so storage-partitioned join should kick
            // in
            df = sql(
              s"SELECT sum(p.price) from testcat.ns.$items i, testcat.ns.$purchases p " +
                "WHERE i.id = p.item_id AND i.price >= 10.0")
            val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
            assert(shuffles.isEmpty, "should not add shuffle for both sides of the join")
            checkAnswer(df, Seq(Row(303.5)))
          }
      }
    }
  }

  testGluten(
    "SPARK-41471: shuffle one side: only one side reports partitioning with two identity") {
    val items_partitions = Array(identity("id"), identity("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      shuffle =>
        withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
          val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (shuffle) {
            assert(shuffles.size == 1, "only shuffle one side not report partitioning")
          } else {
            assert(
              shuffles.size == 2,
              "should add two side shuffle when bucketing shuffle one side" +
                " is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
        }
    }
  }

  testGluten(
    "GLUTEN-10992: KeyGroupedPartitioning shuffle falls back to vanilla Spark") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    // With V2 bucketing shuffle enabled and only one side reporting partitioning, Spark
    // shuffles the other side with a ShuffleExchangeExec whose output partitioning is
    // KeyGroupedPartitioning. Gluten native shuffle does not support it, so the exchange
    // must fall back to vanilla Spark. Offloading it to ColumnarShuffleExchangeExec would
    // crash with a scala.MatchError in ExecUtil.genShuffleDependency (GLUTEN-10992).
    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      val df = createJoinTestDF(Seq("id" -> "item_id"))
      val plan = df.queryExecution.executedPlan

      val keyGroupedShuffles = collect(plan) {
        case s: ShuffleExchangeExec
            if s.outputPartitioning.isInstanceOf[KeyedPartitioning] =>
          s
      }
      assert(
        keyGroupedShuffles.nonEmpty,
        "KeyGroupedPartitioning shuffle should fall back to a vanilla ShuffleExchangeExec")

      val columnarKeyGroupedShuffles = collectAllColumnarShuffles(plan)
        .filter(_.outputPartitioning.isInstanceOf[KeyedPartitioning])
      assert(
        columnarKeyGroupedShuffles.isEmpty,
        "KeyGroupedPartitioning must not be offloaded to ColumnarShuffleExchangeExec")

      checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
    }
  }

  testGluten("SPARK-41471: shuffle one side: only one side reports partitioning") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      shuffle =>
        withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
          val df = createJoinTestDF(Seq("id" -> "item_id"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (shuffle) {
            assert(shuffles.size == 1, "only shuffle one side not report partitioning")
          } else {
            assert(
              shuffles.size == 2,
              "should add two side shuffle when bucketing shuffle one side" +
                " is not enabled")
          }

          checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
        }
    }
  }

  testGluten("SPARK-41471: shuffle one side: shuffle side has more partition value") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp)), " +
        "(5, 26.0, cast('2023-01-01' as timestamp)), " +
        "(6, 50.0, cast('2023-02-01' as timestamp))")

    Seq(true, false).foreach {
      shuffle =>
        withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
          Seq("", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER").foreach {
            joinType =>
              val df = createJoinTestDF(Seq("id" -> "item_id"), joinType = joinType)
              val shuffles = collectShuffles(df.queryExecution.executedPlan)
              if (shuffle) {
                assert(shuffles.size == 1, "only shuffle one side not report partitioning")
              } else {
                assert(
                  shuffles.size == 2,
                  "should add two side shuffle when bucketing shuffle one " +
                    "side is not enabled")
              }
              joinType match {
                case "" =>
                  checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 19.5)))
                case "LEFT OUTER" =>
                  checkAnswer(
                    df,
                    Seq(
                      Row(1, "aa", 40.0, 42.0),
                      Row(3, "bb", 10.0, 19.5),
                      Row(4, "cc", 15.5, null)))
                case "RIGHT OUTER" =>
                  checkAnswer(
                    df,
                    Seq(
                      Row(null, null, null, 26.0),
                      Row(null, null, null, 50.0),
                      Row(1, "aa", 40.0, 42.0),
                      Row(3, "bb", 10.0, 19.5)))
                case "FULL OUTER" =>
                  checkAnswer(
                    df,
                    Seq(
                      Row(null, null, null, 26.0),
                      Row(null, null, null, 50.0),
                      Row(1, "aa", 40.0, 42.0),
                      Row(3, "bb", 10.0, 19.5),
                      Row(4, "cc", 15.5, null)))
              }
          }
        }
    }
  }

  testGluten("SPARK-41471: shuffle one side: partitioning with transform") {
    val items_partitions = Array(years("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2021-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(3, 19.5, cast('2021-02-01' as timestamp))")

    Seq(true, false).foreach {
      shuffle =>
        withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> shuffle.toString) {
          val df = createJoinTestDF(Seq("arrive_time" -> "time"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          if (shuffle) {
            assert(shuffles.size == 1, "partitioning with transform should trigger SPJ")
          } else {
            assert(
              shuffles.size == 2,
              "should add two side shuffle when bucketing shuffle one side" +
                " is not enabled")
          }

          checkAnswer(
            df,
            Seq(Row(1, "aa", 40.0, 42.0), Row(3, "bb", 10.0, 42.0), Row(4, "cc", 15.5, 19.5)))
        }
    }
  }

  testGluten(
    "SPARK-44647: SPJ: test join key is subset of cluster key " +
      "with push values and partially-clustered") {
    val table1 = "tab1e1"
    val table2 = "table2"
    val partition = Array(identity("id"), identity("data"))
    createTable(table1, columns, partition)
    sql(
      s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(1, 'aa', cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', cast('2020-01-01' as timestamp)), " +
        "(2, 'cc', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'dd', cast('2020-01-01' as timestamp)), " +
        "(3, 'ee', cast('2020-01-01' as timestamp)), " +
        "(3, 'ee', cast('2020-01-01' as timestamp))")

    createTable(table2, columns, partition)
    sql(
      s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(4, 'zz', cast('2020-01-01' as timestamp)), " +
        "(4, 'zz', cast('2020-01-01' as timestamp)), " +
        "(3, 'yy', cast('2020-01-01' as timestamp)), " +
        "(3, 'yy', cast('2020-01-01' as timestamp)), " +
        "(3, 'xx', cast('2020-01-01' as timestamp)), " +
        "(3, 'xx', cast('2020-01-01' as timestamp)), " +
        "(2, 'ww', cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(true, false).foreach {
          filter =>
            Seq(true, false).foreach {
              partiallyClustered =>
                Seq(true, false).foreach {
                  allowJoinKeysSubsetOfPartitionKeys =>
                    withSQLConf(
                      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
                      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
                      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                        partiallyClustered.toString,
                      SQLConf.V2_BUCKETING_PARTITION_FILTER_ENABLED.key -> filter.toString,
                      SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                        allowJoinKeysSubsetOfPartitionKeys.toString
                    ) {
                      val df = sql(s"""
                                      |${selectWithMergeJoinHint("t1", "t2")}
                                      |t1.id AS id, t1.data AS t1data, t2.data AS t2data
                                      |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                                      |ON t1.id = t2.id ORDER BY t1.id, t1data, t2data
                                      |""".stripMargin)
                      val shuffles =
                        collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
                      if (allowJoinKeysSubsetOfPartitionKeys) {
                        assert(shuffles.isEmpty, "SPJ should be triggered")
                      } else {
                        assert(shuffles.nonEmpty, "SPJ should not be triggered")
                      }

                      val groupPartitions = collectGroupPartitions(df.queryExecution.executedPlan)
                        .map(_.outputPartitioning.numPartitions)
                      (allowJoinKeysSubsetOfPartitionKeys, partiallyClustered, filter) match {
                        // SPJ, partially-clustered, with filter
                        case (true, true, true) => assert(groupPartitions == Seq(6, 6))

                        // SPJ, partially-clustered, no filter
                        case (true, true, false) => assert(groupPartitions == Seq(8, 8))

                        // SPJ and not partially-clustered, with filter
                        case (true, false, true) => assert(groupPartitions == Seq(2, 2))

                        // SPJ and not partially-clustered, no filter
                        case (true, false, false) => assert(groupPartitions == Seq(4, 4))

                        // No SPJ
                        case _ => assert(groupPartitions.isEmpty)
                      }

                      checkAnswer(
                        df,
                        Seq(
                          Row(2, "bb", "ww"),
                          Row(2, "cc", "ww"),
                          Row(3, "dd", "xx"),
                          Row(3, "dd", "xx"),
                          Row(3, "dd", "xx"),
                          Row(3, "dd", "xx"),
                          Row(3, "dd", "yy"),
                          Row(3, "dd", "yy"),
                          Row(3, "dd", "yy"),
                          Row(3, "dd", "yy"),
                          Row(3, "ee", "xx"),
                          Row(3, "ee", "xx"),
                          Row(3, "ee", "xx"),
                          Row(3, "ee", "xx"),
                          Row(3, "ee", "yy"),
                          Row(3, "ee", "yy"),
                          Row(3, "ee", "yy"),
                          Row(3, "ee", "yy")
                        )
                      )
                    }
                }
            }
        }
    }
  }

  testGluten("SPARK-44647: test join key is the second cluster key") {
    val table1 = "tab1e1"
    val table2 = "table2"
    val partition = Array(identity("id"), identity("data"))
    createTable(table1, columns, partition)
    sql(
      s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(1, 'aa', cast('2020-01-01' as timestamp)), " +
        "(2, 'bb', cast('2020-01-02' as timestamp)), " +
        "(3, 'cc', cast('2020-01-03' as timestamp))")

    createTable(table2, columns, partition)
    sql(
      s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(4, 'aa', cast('2020-01-01' as timestamp)), " +
        "(5, 'bb', cast('2020-01-02' as timestamp)), " +
        "(6, 'cc', cast('2020-01-03' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(true, false).foreach {
          partiallyClustered =>
            Seq(true, false).foreach {
              allowJoinKeysSubsetOfPartitionKeys =>
                withSQLConf(
                  SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
                  SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key ->
                    pushDownValues.toString,
                  SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                    partiallyClustered.toString,
                  SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                    allowJoinKeysSubsetOfPartitionKeys.toString
                ) {

                  val df = sql(s"""
                                  |${selectWithMergeJoinHint("t1", "t2")}
                                  |t1.id AS t1id, t2.id as t2id, t1.data AS data
                                  |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                                  |ON t1.data = t2.data
                                  |ORDER BY t1id, t1id, data
                                  |""".stripMargin)
                  checkAnswer(df, Seq(Row(1, 4, "aa"), Row(2, 5, "bb"), Row(3, 6, "cc")))

                  val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
                  if (allowJoinKeysSubsetOfPartitionKeys) {
                    assert(shuffles.isEmpty, "SPJ should be triggered")
                  } else {
                    assert(shuffles.nonEmpty, "SPJ should not be triggered")
                  }

                  val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
                    .map(_.outputPartitioning.numPartitions)
                  (pushDownValues, allowJoinKeysSubsetOfPartitionKeys, partiallyClustered) match {
                    // SPJ and partially-clustered
                    case (_, true, _) => assert(partitions == Seq(3, 3))
                    // non-SPJ or SPJ/partially-clustered
                    case _ => assert(partitions.isEmpty)
                  }
                }
            }
        }
    }
  }

  testGluten("SPARK-44647: test join key is the second partition key and a transform") {
    val items_partitions = Array(bucket(8, "id"), days("arrive_time"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(2, 'bb', 10.5, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    val purchases_partitions = Array(bucket(8, "item_id"), days("time"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        s"(1, 42.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 44.0, cast('2020-01-15' as timestamp)), " +
        s"(1, 45.0, cast('2020-01-15' as timestamp)), " +
        s"(2, 11.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 19.5, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushDownValues =>
        Seq(true, false).foreach {
          partiallyClustered =>
            Seq(true, false).foreach {
              allowJoinKeysSubsetOfPartitionKeys =>
                withSQLConf(
                  SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
                  SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushDownValues.toString,
                  SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                    partiallyClustered.toString,
                  SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                    allowJoinKeysSubsetOfPartitionKeys.toString
                ) {
                  val df =
                    createJoinTestDF(Seq("arrive_time" -> "time"), extraColumns = Seq("p.item_id"))
                  // Currently SPJ for case where join key not same as partition key
                  // only supported when push-part-values enabled
                  val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
                  if (allowJoinKeysSubsetOfPartitionKeys) {
                    assert(shuffles.isEmpty, "SPJ should be triggered")
                  } else {
                    assert(shuffles.nonEmpty, "SPJ should not be triggered")
                  }

                  val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
                    .map(_.outputPartitioning.numPartitions)
                  (allowJoinKeysSubsetOfPartitionKeys, partiallyClustered) match {
                    // SPJ and partially-clustered
                    case (true, true) => assert(partitions == Seq(5, 5))
                    // SPJ and not partially-clustered
                    case (true, false) => assert(partitions == Seq(3, 3))
                    // No SPJ
                    case _ => assert(partitions.isEmpty)
                  }

                  checkAnswer(
                    df,
                    Seq(
                      Row(1, "aa", 40.0, 11.0, 2),
                      Row(1, "aa", 40.0, 42.0, 1),
                      Row(1, "aa", 41.0, 44.0, 1),
                      Row(1, "aa", 41.0, 45.0, 1),
                      Row(2, "bb", 10.0, 11.0, 2),
                      Row(2, "bb", 10.0, 42.0, 1),
                      Row(2, "bb", 10.5, 11.0, 2),
                      Row(2, "bb", 10.5, 42.0, 1),
                      Row(3, "cc", 15.5, 19.5, 3)
                    )
                  )
                }
            }
        }
    }
  }

  testGluten("SPARK-44647: shuffle one side and join keys are less than partition keys") {
    val items_partitions = Array(identity("id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'aa', 30.0, cast('2020-01-02' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(1, 89.0, cast('2020-01-03' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp)), " +
        "(5, 26.0, cast('2023-01-01' as timestamp)), " +
        "(6, 50.0, cast('2023-02-01' as timestamp))")

    Seq(true, false).foreach {
      pushdownValues =>
        withSQLConf(
          SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushdownValues.toString,
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true"
        ) {
          val df = createJoinTestDF(Seq("id" -> "item_id"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.size == 1, "SPJ should be triggered")
          checkAnswer(
            df,
            Seq(
              Row(1, "aa", 30.0, 42.0),
              Row(1, "aa", 30.0, 89.0),
              Row(1, "aa", 40.0, 42.0),
              Row(1, "aa", 40.0, 89.0),
              Row(3, "bb", 10.0, 19.5)))
        }
    }
  }

  testGluten(
    "SPARK-47094: Compatible buckets does not support SPJ with " +
      "push-down values or partially-clustered") {
    val table1 = "tab1e1"
    val table2 = "table2"

    val partition1 = Array(bucket(4, "store_id"), bucket(2, "dept_id"))
    val partition2 = Array(bucket(2, "store_id"), bucket(2, "dept_id"))

    createTable(table1, columns2, partition1)
    sql(
      s"INSERT INTO testcat.ns.$table1 VALUES " +
        "(0, 0, 'aa'), " +
        "(1, 1, 'bb'), " +
        "(2, 2, 'cc')"
    )

    createTable(table2, columns2, partition2)
    sql(
      s"INSERT INTO testcat.ns.$table2 VALUES " +
        "(0, 0, 'aa'), " +
        "(1, 1, 'bb'), " +
        "(2, 2, 'cc')"
    )

    Seq(true, false).foreach {
      allowPushDown =>
        Seq(true, false).foreach {
          partiallyClustered =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> allowPushDown.toString,
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key ->
                partiallyClustered.toString,
              SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
              SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true"
            ) {
              val df = sql(s"""
                              |${selectWithMergeJoinHint("t1", "t2")}
                              |t1.store_id, t1.store_id, t1.dept_id, t2.dept_id, t1.data, t2.data
                              |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                              |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id
                              |ORDER BY t1.store_id, t1.dept_id, t1.data, t2.data
                              |""".stripMargin)

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              val partitions = collectGroupPartitions(df.queryExecution.executedPlan)
                .map(_.outputPartitioning.numPartitions)

              (allowPushDown, partiallyClustered) match {
                case (true, false) =>
                  assert(shuffles.isEmpty, "SPJ should be triggered")
                  assert(partitions == Seq(2, 2))
                case (_, _) =>
                  assert(shuffles.nonEmpty, "SPJ should not be triggered")
                  assert(partitions.isEmpty)
              }

              checkAnswer(
                df,
                Seq(
                  Row(0, 0, 0, 0, "aa", "aa"),
                  Row(1, 1, 1, 1, "bb", "bb"),
                  Row(2, 2, 2, 2, "cc", "cc")
                ))
            }
        }
    }
  }

  testGluten(
    "SPARK-47094: SPJ: Does not trigger when incompatible number of buckets on both side") {
    val table1 = "tab1e1"
    val table2 = "table2"

    Seq(
      (2, 3),
      (3, 4)
    ).foreach {
      case (table1buckets1, table2buckets1) =>
        catalog.clearTables()

        val partition1 = Array(bucket(table1buckets1, "store_id"))
        val partition2 = Array(bucket(table2buckets1, "store_id"))

        Seq((table1, partition1), (table2, partition2)).foreach {
          case (tab, part) =>
            createTable(tab, columns2, part)
            val insertStr = s"INSERT INTO testcat.ns.$tab VALUES " +
              "(0, 0, 'aa'), " +
              "(1, 0, 'ab'), " + // duplicate partition key
              "(2, 2, 'ac'), " +
              "(3, 3, 'ad'), " +
              "(4, 2, 'bc') "

            sql(insertStr)
        }

        Seq(true, false).foreach {
          allowJoinKeysSubsetOfPartitionKeys =>
            withSQLConf(
              SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
              SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
              SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key ->
                allowJoinKeysSubsetOfPartitionKeys.toString,
              SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS.key -> "true"
            ) {
              val df = sql(s"""
                              |${selectWithMergeJoinHint("t1", "t2")}
                              |t1.store_id, t1.dept_id, t1.data, t2.data
                              |FROM testcat.ns.$table1 t1 JOIN testcat.ns.$table2 t2
                              |ON t1.store_id = t2.store_id AND t1.dept_id = t2.dept_id
                              |""".stripMargin)

              val shuffles = collectColumnarShuffleExchangeExec(df.queryExecution.executedPlan)
              assert(shuffles.nonEmpty, "SPJ should not be triggered")
            }
        }
    }
  }

  testGluten("SPARK-48655: order by on partition keys should not introduce additional shuffle") {
    val items_partitions = Array(identity("price"), identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        s"(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        s"(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
        s"(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        s"(3, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(null, 'cc', 15.5, cast('2020-02-01' as timestamp)), " +
        s"(3, 'cc', null, cast('2020-02-01' as timestamp))")

    Seq(true, false).foreach {
      sortingEnabled =>
        withSQLConf(SQLConf.V2_BUCKETING_SORTING_ENABLED.key -> sortingEnabled.toString) {

          def verifyShuffle(cmd: String, answer: Seq[Row]): Unit = {
            val df = sql(cmd)
            if (sortingEnabled) {
              assert(
                collectAllShuffles(df.queryExecution.executedPlan).isEmpty,
                "should contain no shuffle when sorting by partition values")
            } else {
              assert(
                collectAllShuffles(df.queryExecution.executedPlan).size == 1,
                "should contain one shuffle when optimization is disabled")
            }
            checkAnswer(df, answer)
          }: Unit

          verifyShuffle(
            s"SELECT price, id FROM testcat.ns.$items ORDER BY price ASC, id ASC",
            Seq(
              Row(null, 3),
              Row(10.0, 2),
              Row(15.5, null),
              Row(15.5, 3),
              Row(40.0, 1),
              Row(41.0, 1))
          )

          verifyShuffle(
            s"SELECT price, id FROM testcat.ns.$items " +
              s"ORDER BY price ASC NULLS LAST, id ASC NULLS LAST",
            Seq(
              Row(10.0, 2),
              Row(15.5, 3),
              Row(15.5, null),
              Row(40.0, 1),
              Row(41.0, 1),
              Row(null, 3))
          )

          verifyShuffle(
            s"SELECT price, id FROM testcat.ns.$items ORDER BY price DESC, id ASC",
            Seq(
              Row(41.0, 1),
              Row(40.0, 1),
              Row(15.5, null),
              Row(15.5, 3),
              Row(10.0, 2),
              Row(null, 3))
          )

          verifyShuffle(
            s"SELECT price, id FROM testcat.ns.$items ORDER BY price DESC, id DESC",
            Seq(
              Row(41.0, 1),
              Row(40.0, 1),
              Row(15.5, 3),
              Row(15.5, null),
              Row(10.0, 2),
              Row(null, 3))
          )

          verifyShuffle(
            s"SELECT price, id FROM testcat.ns.$items " +
              s"ORDER BY price DESC NULLS FIRST, id DESC NULLS FIRST",
            Seq(
              Row(null, 3),
              Row(41.0, 1),
              Row(40.0, 1),
              Row(15.5, null),
              Row(15.5, 3),
              Row(10.0, 2))
          )
        }
    }
  }

  testGluten("SPARK-48012: one-side shuffle with partition transforms") {
    val items_partitions = Array(bucket(2, "id"), identity("arrive_time"))
    val items_partitions2 = Array(identity("arrive_time"), bucket(2, "id"))

    Seq(items_partitions, items_partitions2).foreach {
      partition =>
        catalog.clearTables()

        createTable(items, itemsColumns, partition)
        sql(
          s"INSERT INTO testcat.ns.$items VALUES " +
            "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
            "(1, 'bb', 30.0, cast('2020-01-01' as timestamp)), " +
            "(1, 'cc', 30.0, cast('2020-01-02' as timestamp)), " +
            "(3, 'dd', 10.0, cast('2020-01-01' as timestamp)), " +
            "(4, 'ee', 15.5, cast('2020-02-01' as timestamp)), " +
            "(5, 'ff', 32.1, cast('2020-03-01' as timestamp))")

        createTable(purchases, purchasesColumns, Array.empty)
        sql(
          s"INSERT INTO testcat.ns.$purchases VALUES " +
            "(1, 42.0, cast('2020-01-01' as timestamp)), " +
            "(2, 10.7, cast('2020-01-01' as timestamp))," +
            "(3, 19.5, cast('2020-02-01' as timestamp))," +
            "(4, 56.5, cast('2020-02-01' as timestamp))")

        withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
          val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
          val shuffles = collectShuffles(df.queryExecution.executedPlan)
          assert(shuffles.size == 1, "only shuffle side that does not report partitioning")

          checkAnswer(
            df,
            Seq(Row(1, "bb", 30.0, 42.0), Row(1, "aa", 40.0, 42.0), Row(4, "ee", 15.5, 56.5)))
        }
    }
  }

  testGluten("SPARK-48012: one-side shuffle with partition transforms and pushdown values") {
    val items_partitions = Array(bucket(2, "id"), identity("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'bb', 30.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'cc', 30.0, cast('2020-01-02' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(2, 10.7, cast('2020-01-01' as timestamp))")

    Seq(true, false).foreach {
      pushDown =>
        {
          withSQLConf(
            SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key ->
              pushDown.toString) {
            val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
            val shuffles = collectShuffles(df.queryExecution.executedPlan)
            assert(shuffles.size == 1, "only shuffle side that does not report partitioning")

            checkAnswer(df, Seq(Row(1, "bb", 30.0, 42.0), Row(1, "aa", 40.0, 42.0)))
          }
        }
    }
  }

  testGluten(
    "SPARK-48012: one-side shuffle with partition transforms " +
      "with fewer join keys than partition kes") {
    val items_partitions = Array(bucket(2, "id"), identity("name"))
    createTable(items, itemsColumns, items_partitions)

    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
        "(1, 'aa', 30.0, cast('2020-01-02' as timestamp)), " +
        "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
        "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 42.0, cast('2020-01-01' as timestamp)), " +
        "(1, 89.0, cast('2020-01-03' as timestamp)), " +
        "(3, 19.5, cast('2020-02-01' as timestamp)), " +
        "(5, 26.0, cast('2023-01-01' as timestamp)), " +
        "(6, 50.0, cast('2023-02-01' as timestamp))")

    withSQLConf(
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
      SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> "false",
      SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true"
    ) {
      val df = createJoinTestDF(Seq("id" -> "item_id"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size == 1, "SPJ should be triggered")
      checkAnswer(
        df,
        Seq(
          Row(1, "aa", 30.0, 42.0),
          Row(1, "aa", 30.0, 89.0),
          Row(1, "aa", 40.0, 42.0),
          Row(1, "aa", 40.0, 89.0),
          Row(3, "bb", 10.0, 19.5)))
    }
  }

  testGluten("SPARK-53322: checkpointed scans avoid shuffles for aggregates") {
    withTempDir {
      dir =>
        spark.sparkContext.setCheckpointDir(dir.getPath)
        val itemsPartitions = Array(identity("id"))
        createTable(items, itemsColumns, itemsPartitions)
        sql(
          s"INSERT INTO testcat.ns.$items VALUES " +
            "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
            "(1, 'aa', 41.0, cast('2020-01-02' as timestamp)), " +
            "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
            "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

        val scanDF = spark.read.table(s"testcat.ns.$items").checkpoint()
        val df = scanDF.groupBy("id").agg(max("price").as("res")).select("res")
        checkAnswer(df.sort("res"), Seq(Row(10.0), Row(15.5), Row(41.0)))

        val shuffles = collectAllShuffles(df.queryExecution.executedPlan)
        assert(
          shuffles.isEmpty,
          "should not contain shuffle when not grouping by partition values")
        val groupPartitions = collectAllGroupPartitions(df.queryExecution.executedPlan)
        assert(groupPartitions.size === 1)
        assert(groupPartitions.head.outputPartitioning.numPartitions == 3)
    }
  }

  testGluten("SPARK-53322: checkpointed scans are used for SPJ") {
    withTempDir {
      dir =>
        spark.sparkContext.setCheckpointDir(dir.getPath)
        val itemsPartitions = Array(identity("id"))
        createTable(items, itemsColumns, itemsPartitions)
        sql(
          s"INSERT INTO testcat.ns.$items VALUES " +
            "(1, 'aa', 41.0, cast('2020-01-01' as timestamp)), " +
            "(2, 'bb', 10.0, cast('2020-01-02' as timestamp)), " +
            "(3, 'cc', 15.5, cast('2020-01-03' as timestamp))")

        val purchasePartitions = Array(identity("item_id"))
        createTable(purchases, purchasesColumns, purchasePartitions)
        sql(
          s"INSERT INTO testcat.ns.$purchases VALUES " +
            "(1, 40.0, cast('2020-01-01' as timestamp)), " +
            "(3, 25.5, cast('2020-01-03' as timestamp)), " +
            "(4, 20.0, cast('2020-01-04' as timestamp))")

        for {
          pushdownValues <- Seq(true, false)
          checkpointBothScans <- Seq(true, false)
        } {
          withSQLConf(
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushdownValues.toString) {
            val scanDF1 = spark.read.table(s"testcat.ns.$items").checkpoint().as("i")
            val scanDF2 = if (checkpointBothScans) {
              spark.read.table(s"testcat.ns.$purchases").checkpoint().as("p")
            } else {
              spark.read.table(s"testcat.ns.$purchases").as("p")
            }

            val df = scanDF1
              .join(scanDF2, col("id") === col("item_id"))
              .selectExpr("id", "name", "i.price AS purchase_price", "p.price AS sale_price")
              .orderBy("id", "purchase_price", "sale_price")
            checkAnswer(df, Seq(Row(1, "aa", 41.0, 40.0), Row(3, "cc", 15.5, 25.5)))

            val plan = df.queryExecution.executedPlan
            if (pushdownValues) {
              // 1 shuffle for SORT and 2 group partitions for JOIN are expected.
              assert(collectAllShuffles(plan).length === 1)
              assert(collectAllGroupPartitions(plan).length === 2)
            } else {
              // 1 shuffle for SORT and 2 shuffles for JOIN are expected.
              assert(collectAllShuffles(plan).length === 3)
              assert(collectAllGroupPartitions(plan).isEmpty)
            }
          }
        }
    }
  }

  testGluten("SPARK-53322: checkpointed scans can shuffle other children on SPJ") {
    withTempDir {
      dir =>
        spark.sparkContext.setCheckpointDir(dir.getPath)
        val itemsPartitions = Array(identity("id"))
        createTable(items, itemsColumns, itemsPartitions)
        sql(
          s"INSERT INTO testcat.ns.$items VALUES " +
            "(1, 'aa', 41.0, cast('2020-01-01' as timestamp)), " +
            "(2, 'bb', 10.0, cast('2020-01-02' as timestamp)), " +
            "(3, 'cc', 15.5, cast('2020-01-03' as timestamp))")

        createTable(purchases, purchasesColumns, Array.empty)
        sql(
          s"INSERT INTO testcat.ns.$purchases VALUES " +
            "(1, 40.0, cast('2020-01-01' as timestamp)), " +
            "(3, 25.5, cast('2020-01-03' as timestamp)), " +
            "(4, 20.0, cast('2020-01-04' as timestamp))")

        Seq(true, false).foreach {
          pushdownValues =>
            withSQLConf(
              SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
              SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true",
              SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key -> pushdownValues.toString
            ) {
              val scanDF1 = spark.read.table(s"testcat.ns.$items").checkpoint().as("i")
              val scanDF2 = spark.read.table(s"testcat.ns.$purchases").as("p")

              val df = scanDF1
                .join(scanDF2, col("id") === col("item_id"))
                .selectExpr("id", "name", "i.price AS purchase_price", "p.price AS sale_price")
                .orderBy("id", "purchase_price", "sale_price")
              checkAnswer(df, Seq(Row(1, "aa", 41.0, 40.0), Row(3, "cc", 15.5, 25.5)))

              val plan = df.queryExecution.executedPlan
              // 1 shuffle for SORT and 1 shuffle for JOIN are expected.
              assert(collectAllShuffles(plan).length === 2)
              // The JOIN shuffle has KeyedPartitioning and falls back to vanilla Spark
              // (GLUTEN-10992). The SORT shuffle is native.
              assert(collectVanillaShuffles(plan).length === 1)
              assert(collectAllColumnarShuffles(plan).length === 1)
              // 0 group partitions are expected because both sides of the join are clustered
              assert(collectAllGroupPartitions(plan).isEmpty)
            }
        }
    }
  }

  testGluten("SPARK-55848: checkpointed partially-clustered join with dedup") {
    withTempDir {
      dir =>
        spark.sparkContext.setCheckpointDir(dir.getPath)
        val items_partitions = Array(identity("id"))
        createTable(items, itemsColumns, items_partitions)
        sql(
          s"INSERT INTO testcat.ns.$items VALUES " +
            "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
            "(1, 'aa', 41.0, cast('2020-01-15' as timestamp)), " +
            "(2, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
            "(3, 'cc', 15.5, cast('2020-02-01' as timestamp))")

        val purchases_partitions = Array(identity("item_id"))
        createTable(purchases, purchasesColumns, purchases_partitions)
        sql(
          s"INSERT INTO testcat.ns.$purchases VALUES " +
            "(1, 42.0, cast('2020-01-01' as timestamp)), " +
            "(1, 50.0, cast('2020-01-02' as timestamp)), " +
            "(2, 11.0, cast('2020-01-01' as timestamp)), " +
            "(3, 19.5, cast('2020-02-01' as timestamp))")

        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key -> true.toString
        ) {
          // Checkpoint the JOIN result (not the scan) so the checkpoint node carries the
          // partially-clustered KeyGroupedPartitioning. The dedup on top must still insert
          // the required grouping operator because partially-clustered partitioning does not
          // satisfy ClusteredDistribution.
          val joinedDf = sql(s"""${selectWithMergeJoinHint("i", "p")} i.id, i.name, i.price
                                |FROM testcat.ns.$items i
                                |JOIN testcat.ns.$purchases p ON i.id = p.item_id""".stripMargin)
          val checkpointedDf = joinedDf.checkpoint()
          val df = checkpointedDf.select("id").distinct()
          checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

          // Gluten offloads the checkpoint's RDDScanExec to an RDDScanTransformer, which keeps
          // the output partitioning of the RDDScanExec.
          val checkpointScans = collect(df.queryExecution.executedPlan) {
            case r: RDDScanExec => r
            case r: RDDScanTransformer => r
          }
          assert(
            checkpointScans.exists(_.outputPartitioning match {
              case kp: KeyedPartitioning => !kp.isGrouped
              case _ => false
            }),
            "checkpoint scan should have ungrouped KeyedPartitioning"
          )

          val allGroupPartitions = collectAllGroupPartitions(df.queryExecution.executedPlan)
          assert(
            allGroupPartitions.size === 1,
            "expected 1 GroupPartitionsExec above the checkpointed join for dedup")
        }
    }
  }

  testGluten("SPARK-54439: KeyGroupedPartitioning and join key size mismatch") {
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(3, 'bb', 10.0, cast('2020-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2020-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2020-02-01' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      // `time` and `item_id` in the required `ClusteredDistribution` for `purchases`, but `item` is
      // storage partitioned only by `id`
      val df = createJoinTestDF(Seq("arrive_time" -> "time", "id" -> "item_id"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size == 1, "only shuffle one side not report partitioning")

      checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
    }
  }

  testGluten("SPARK-54439: KeyGroupedPartitioning with transform and join key size mismatch") {
    // Do not use `bucket()` in "one side partition" tests as its implementation in
    // `InMemoryBaseTable` conflicts with `BucketFunction`
    val items_partitions = Array(years("arrive_time"))
    createTable(items, itemsColumns, items_partitions)

    sql(s"INSERT INTO testcat.ns.$items VALUES " +
      "(1, 'aa', 40.0, cast('2020-01-01' as timestamp)), " +
      "(1, 'bb', 10.0, cast('2021-01-01' as timestamp)), " +
      "(4, 'cc', 15.5, cast('2021-02-01' as timestamp))")

    createTable(purchases, purchasesColumns, Array.empty)
    sql(s"INSERT INTO testcat.ns.$purchases VALUES " +
      "(1, 42.0, cast('2020-01-01' as timestamp)), " +
      "(3, 19.5, cast('2021-02-01' as timestamp))")

    withSQLConf(SQLConf.V2_BUCKETING_SHUFFLE_ENABLED.key -> "true") {
      // `item_id` and `time` in the required `ClusteredDistribution` for `purchases`, but `item` is
      // storage partitioned only by `year(arrive_time)`
      val df = createJoinTestDF(Seq("id" -> "item_id", "arrive_time" -> "time"))
      val shuffles = collectShuffles(df.queryExecution.executedPlan)
      assert(shuffles.size == 1, "only shuffle one side not report partitioning")

      checkAnswer(df, Seq(Row(1, "aa", 40.0, 42.0)))
    }
  }

  private def assertNoSortBeforeSortMergeJoin(plan: SparkPlan): Unit = {
    val smjs = collect(plan) { case j: SortMergeJoinExecTransformer => j }
    assert(smjs.nonEmpty, "expected SortMergeJoinExecTransformer in plan")
    smjs.foreach {
      smj =>
        val sorts = smj.children.flatMap {
          child =>
            collect(child) {
              case s: SortExec => s
              case s: SortExecTransformer => s
            }
        }
        assert(sorts.isEmpty, s"should not add a sort before SMJ, but found: $sorts")
    }
  }

  testGluten(
    "SPARK-56241: GroupPartitionsExec non-coalescing passes through child ordering, " +
      "no pre-join SortExec needed before SortMergeJoin") {
    // Non-identical key sets force GroupPartitionsExec to be inserted on both sides align them,
    // but each group has exactly one partition — no coalescing.
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 10.0, cast('2021-01-01' as timestamp)), " +
        "(2, 'bb', 20.0, cast('2021-01-01' as timestamp)), " +
        "(3, 'cc', 30.0, cast('2021-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 100.0, cast('2021-01-01' as timestamp)), " +
        "(2, 200.0, cast('2021-01-01' as timestamp))")

    // GroupPartitionsExec passes through the child's key-derived outputOrdering.
    // EnsureRequirements checks outputOrdering directly so no sort should be inserted before
    // the SMJ.
    withSQLConf(SQLConf.V2_BUCKETING_PARTITION_KEY_ORDERING_ENABLED.key -> "true") {
      val df = sql(s"""
                      |${selectWithMergeJoinHint("i", "p")}
                      |i.id, i.name
                      |FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id
                      |""".stripMargin)

      checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb")))

      val plan = df.queryExecution.executedPlan
      val groupPartitions = collectGroupPartitions(plan)
      assert(groupPartitions.nonEmpty, "expected GroupPartitionsExec in plan")
      assert(
        groupPartitions.forall(_.groupedPartitions.forall(_._2.size <= 1)),
        "expected non-coalescing GroupPartitionsExec")
      assertNoSortBeforeSortMergeJoin(plan)
    }
  }

  testGluten(
    "SPARK-56241: GroupPartitionsExec coalescing derives ordering from key expressions, " +
      "no pre-join SortExec needed before SortMergeJoin") {
    // Duplicate key 1 on both sides causes coalescing.
    val items_partitions = Array(identity("id"))
    createTable(items, itemsColumns, items_partitions)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(1, 'aa', 10.0, cast('2021-01-01' as timestamp)), " +
        "(1, 'ab', 11.0, cast('2021-06-01' as timestamp)), " +
        "(2, 'bb', 20.0, cast('2021-01-01' as timestamp))")

    val purchases_partitions = Array(identity("item_id"))
    createTable(purchases, purchasesColumns, purchases_partitions)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(1, 100.0, cast('2021-01-01' as timestamp)), " +
        "(1, 110.0, cast('2021-06-01' as timestamp)), " +
        "(2, 200.0, cast('2021-01-01' as timestamp))")

    // GroupPartitionsExec derives outputOrdering from the key expressions after coalescing.
    // EnsureRequirements checks outputOrdering directly so no sort should be inserted before
    // the SMJ.
    withSQLConf(
      SQLConf.V2_BUCKETING_PARTITION_KEY_ORDERING_ENABLED.key -> "true",
      SQLConf.V2_BUCKETING_PRESERVE_KEY_ORDERING_ON_COALESCE_ENABLED.key -> "true") {
      val df = sql(s"""
                      |${selectWithMergeJoinHint("i", "p")}
                      |i.id, i.name
                      |FROM testcat.ns.$items i JOIN testcat.ns.$purchases p ON p.item_id = i.id
                      |""".stripMargin)

      checkAnswer(df, Seq(Row(1, "aa"), Row(1, "aa"), Row(1, "ab"), Row(1, "ab"), Row(2, "bb")))

      val plan = df.queryExecution.executedPlan
      val groupPartitions = collectGroupPartitions(plan)
      assert(groupPartitions.nonEmpty, "expected GroupPartitionsExec in plan")
      assert(
        groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
        "expected coalescing GroupPartitionsExec")
      assertNoSortBeforeSortMergeJoin(plan)
    }
  }

  // Gluten offloads the Filter/Project between the scan and a GroupPartitionsExec. Gluten
  // operators are not SafeForKWayMerge, so the GroupPartitionsExec then can't use the sorted merge
  // (SortedMergeCoalescedRDD): it concatenates the coalesced splits and reports only the ordering
  // that survives that, and Gluten's EnsureLocalSortRequirements adds a sort before the sort-merge
  // join. Turning the offload off keeps that subtree vanilla, so that the sorted merge is used.
  private def groupPartitionsChildConfs(offload: Boolean): Seq[(String, String)] = Seq(
    GlutenConfig.COLUMNAR_FILTER_ENABLED.key -> offload.toString,
    GlutenConfig.COLUMNAR_PROJECT_ENABLED.key -> offload.toString)

  testGluten("SPARK-55715: Custom metrics of sorted-merge coalesced partitions") {
    // items has id=1 on three splits with interleaved arrive_times -- out of order across splits.
    // purchases has item_id=1 on two splits, also out of order. Both sides coalesce under SMJ,
    // using SortedMergeCoalescedRDD with multiple concurrent readers per task. This test verifies
    // that all rows from both tables (5 + 4 = 9) are accounted for in the per-scan metrics.
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    createTable(items, itemsColumns, Array(identity("id")), itemOrdering)
    // Rows inserted out of order: id=1 lands on partitions 1, 3, 4 with arrive_times
    // [2022-03-10, 2021-05-20, 2025-09-01] -- out of order.
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
        "(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
        "(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
        "(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
        "(1, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    createTable(purchases, purchasesColumns, Array(identity("item_id")), purchaseOrdering)
    // item_id=1 lands on partitions 1 and 3 with times [2022-03-10, 2021-05-20] -- out of order.
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(2, 30.0, cast('2023-06-15' as timestamp)), " +
        "(1, 20.0, cast('2022-03-10' as timestamp)), " +
        "(3, 40.0, cast('2024-01-01' as timestamp)), " +
        "(1, 10.0, cast('2021-05-20' as timestamp))")

    withSQLConf(
      (groupPartitionsChildConfs(offload = false) ++ Seq(
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
        SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true"
      )): _*) {
      val metrics = runAndFetchMetrics {
        val df = sql(s"""${selectWithMergeJoinHint("i", "p")}
                        |i.id, i.name
                        |FROM testcat.ns.$items i
                        |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
                        |""".stripMargin)
        checkAnswer(df, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(3, "dd")))
        val plan = df.queryExecution.executedPlan
        assert(
          collect(plan) { case j: SortMergeJoinExecTransformer => j }.nonEmpty,
          "expected SortMergeJoinExecTransformer")
        val groupPartitions = collectAllGroupPartitions(plan)
        val coalescingGP = groupPartitions.filter(_.groupedPartitions.exists(_._2.size > 1))
        assert(coalescingGP.nonEmpty, "expected a coalescing GroupPartitionsExec")
        coalescingGP.foreach {
          gp =>
            assert(
              gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
              "should use SortedMergeCoalescedRDD when preserve-ordering config is enabled")
        }
      }
      assert(metrics.collect {
        case ((_, "BatchScan testcat.ns.items", "number of rows read"), v) => v
      } === Seq("5"))
      assert(metrics.collect {
        case ((_, "BatchScan testcat.ns.purchases", "number of rows read"), v) => v
      } === Seq("4"))
    }
  }

  testGluten("SPARK-55715: preserve outputOrdering when coalescing partitions with sorted merge") {
    // Both tables are partitioned by their id column and report ordering [id ASC, arrive_time ASC]
    // via SupportsReportOrdering. Each has two rows with id=1 (two splits), so GroupPartitionsExec
    // must coalesce them. We join on (id, arrive_time) = (item_id, time) using SMJ.
    //
    // With config enabled:  SortedMergeCoalescedRDD performs a k-way merge preserving the full
    //   [id ASC, arrive_time ASC] ordering -> EnsureRequirements is satisfied -> no SortExec added.
    // With config disabled: simple CoalescedRDD concatenates the splits and only the key-derived
    //   [id ASC] ordering survives -> arrive_time ordering is lost -> SortExec is added.
    // With the GroupPartitionsExec child offloaded, Gluten always falls back to the latter.
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    createTable(items, itemsColumns, Array(identity("id")), itemOrdering)
    // Rows inserted out of order: id values are interleaved and arrive_time is not monotone
    // within each id group, so ordering by [id ASC, arrive_time ASC] is non-trivial.
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
        "(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
        "(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
        "(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
        "(2, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    createTable(purchases, purchasesColumns, Array(identity("item_id")), purchaseOrdering)
    // Also inserted out of order
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(2, 50.0, cast('2025-09-01' as timestamp)), " +
        "(1, 10.0, cast('2021-05-20' as timestamp)), " +
        "(3, 40.0, cast('2024-01-01' as timestamp)), " +
        "(2, 30.0, cast('2023-06-15' as timestamp)), " +
        "(1, 20.0, cast('2022-03-10' as timestamp))")

    for (offloadChild <- Seq(false, true); preserveOrdering <- Seq(true, false)) {
      withSQLConf(
        (groupPartitionsChildConfs(offloadChild) ++ Seq(
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS.key -> "true",
          SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key ->
            preserveOrdering.toString
        )): _*) {
        val df = sql(s"""
                        |${selectWithMergeJoinHint("i", "p")}
                        |i.id, i.name
                        |FROM testcat.ns.$items i
                        |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
                        |""".stripMargin)
        checkAnswer(df, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))

        val plan = df.queryExecution.executedPlan
        assert(
          collect(plan) { case s: ShuffleExchangeLike => s }.isEmpty,
          "should not contain any shuffle")

        val groupPartitions = collectAllGroupPartitions(plan)
        assert(groupPartitions.nonEmpty, "should contain GroupPartitionsExec for coalescing")
        assert(
          groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
          "expected coalescing GroupPartitionsExec")

        val smjs = collect(plan) { case j: SortMergeJoinExecTransformer => j }
        assert(smjs.nonEmpty, "expected SortMergeJoinExecTransformer in plan")
        smjs.foreach {
          smj =>
            val sorts = smj.children.flatMap(
              child =>
                collect(child) {
                  case s: SortExec => s
                  case s: SortExecTransformer => s
                })
            if (preserveOrdering && !offloadChild) {
              assert(
                sorts.isEmpty,
                "config enabled: SortedMergeCoalescedRDD preserves [id ASC, arrive_time ASC], " +
                  "no sort should be added before SMJ")

              // Also verify the k-way merge RDD is actually used
              val coalescingGP = groupPartitions.filter(_.groupedPartitions.exists(_._2.size > 1))
              coalescingGP.foreach {
                gp =>
                  assert(
                    gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
                    "config enabled: should use SortedMergeCoalescedRDD")
              }
            } else {
              assert(
                sorts.nonEmpty,
                "config disabled or child offloaded: simple coalescing loses arrive_time " +
                  "ordering, a sort should be added before SMJ")
            }
        }
      }
    }
  }

  testGluten("SPARK-55715: preserve outputOrdering when coalescing transform-partitioned splits") {
    // Both tables are partitioned by years("arrive_time") / years("time") and report ordering
    // [arrive_time ASC] / [time ASC]. Two rows share the same year bucket (2022 and 2023), so
    // GroupPartitionsExec coalesces two splits per year. We join solely on
    // p.time = i.arrive_time (the partition key expression) using SMJ.
    //
    // With config enabled:  SortedMergeCoalescedRDD k-way merge preserves [arrive_time ASC]
    //   ordering -> EnsureRequirements is satisfied -> no SortExec added.
    // With config disabled: simple CoalescedRDD only preserves the key-derived year ordering ->
    //   arrive_time ordering within a year is lost -> SortExec is added.
    // With the GroupPartitionsExec child offloaded, Gluten always falls back to the latter.
    val itemOrdering = Array(
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(items, itemsColumns, Array(years("arrive_time")), itemOrdering)
    // Inserted out of order: within year 2022, September is before March in insertion order
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(2, 'bb', 20.0, cast('2022-09-20' as timestamp)), " +
        "(4, 'dd', 40.0, cast('2023-11-05' as timestamp)), " +
        "(1, 'aa', 10.0, cast('2022-03-15' as timestamp)), " +
        "(3, 'cc', 30.0, cast('2023-01-10' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    createTable(purchases, purchasesColumns, Array(years("time")), purchaseOrdering)
    // Also inserted out of order
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(2, 20.0, cast('2022-09-20' as timestamp)), " +
        "(4, 40.0, cast('2023-11-05' as timestamp)), " +
        "(1, 10.0, cast('2022-03-15' as timestamp)), " +
        "(3, 30.0, cast('2023-01-10' as timestamp))")

    for (offloadChild <- Seq(false, true); preserveOrdering <- Seq(true, false)) {
      withSQLConf(
        (groupPartitionsChildConfs(offloadChild) :+
          (SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key ->
            preserveOrdering.toString)): _*) {
        val df = sql(s"""
                        |${selectWithMergeJoinHint("i", "p")}
                        |i.id, i.name
                        |FROM testcat.ns.$items i
                        |JOIN testcat.ns.$purchases p ON p.time = i.arrive_time
                        |""".stripMargin)
        checkAnswer(df, Seq(Row(1, "aa"), Row(2, "bb"), Row(3, "cc"), Row(4, "dd")))

        val plan = df.queryExecution.executedPlan
        assert(
          collect(plan) { case s: ShuffleExchangeLike => s }.isEmpty,
          "should not contain any shuffle")

        val groupPartitions = collectAllGroupPartitions(plan)
        assert(groupPartitions.nonEmpty, "should contain GroupPartitionsExec for coalescing")
        assert(
          groupPartitions.exists(_.groupedPartitions.exists(_._2.size > 1)),
          "expected coalescing GroupPartitionsExec")

        val smjs = collect(plan) { case j: SortMergeJoinExecTransformer => j }
        assert(smjs.nonEmpty, "expected SortMergeJoinExecTransformer in plan")
        smjs.foreach {
          smj =>
            val sorts = smj.children.flatMap(
              child =>
                collect(child) {
                  case s: SortExec => s
                  case s: SortExecTransformer => s
                })
            if (preserveOrdering && !offloadChild) {
              assert(
                sorts.isEmpty,
                "config enabled: SortedMergeCoalescedRDD preserves [arrive_time ASC], " +
                  "no sort should be added before SMJ")

              val coalescingGP = groupPartitions.filter(_.groupedPartitions.exists(_._2.size > 1))
              coalescingGP.foreach {
                gp =>
                  assert(
                    gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
                    "config enabled: should use SortedMergeCoalescedRDD")
              }
            } else {
              assert(
                sorts.nonEmpty,
                "config disabled or child offloaded: simple coalescing loses arrive_time " +
                  "ordering within a year, a sort should be added before SMJ"
              )
            }
        }
      }
    }
  }

  testGluten("SPARK-56549: k-way merge enabled only when parent requires ordering") {
    // Both tables are partitioned by id/item_id and report a two-column ordering.
    // Key 1 appears on two splits on each side, so GroupPartitionsExec must coalesce.
    //
    // Dynamic gate: with the config enabled, k-way merge must be activated only when the parent
    // actually requires ordering (SMJ), and must stay off when the parent does not (hash join).
    val itemOrdering = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("arrive_time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    createTable(items, itemsColumns, Array(identity("id")), itemOrdering)
    sql(
      s"INSERT INTO testcat.ns.$items VALUES " +
        "(2, 'cc', 30.0, cast('2023-06-15' as timestamp)), " +
        "(1, 'bb', 20.0, cast('2022-03-10' as timestamp)), " +
        "(3, 'dd', 40.0, cast('2024-01-01' as timestamp)), " +
        "(1, 'aa', 10.0, cast('2021-05-20' as timestamp)), " +
        "(2, 'ee', 50.0, cast('2025-09-01' as timestamp))")

    val purchaseOrdering = Array(
      sort(FieldReference("item_id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST),
      sort(FieldReference("time"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    )
    createTable(purchases, purchasesColumns, Array(identity("item_id")), purchaseOrdering)
    sql(
      s"INSERT INTO testcat.ns.$purchases VALUES " +
        "(2, 50.0, cast('2025-09-01' as timestamp)), " +
        "(1, 10.0, cast('2021-05-20' as timestamp)), " +
        "(3, 40.0, cast('2024-01-01' as timestamp)), " +
        "(2, 30.0, cast('2023-06-15' as timestamp)), " +
        "(1, 20.0, cast('2022-03-10' as timestamp))")

    withSQLConf(
      (groupPartitionsChildConfs(offload = false) ++ Seq(
        SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION.key -> "false",
        SQLConf.V2_BUCKETING_PRESERVE_ORDERING_ON_COALESCE_ENABLED.key -> "true")): _*) {
      val hashDf =
        sql(s"""
               |SELECT /*+ SHUFFLE_HASH(i, p) */ i.id, i.name
               |FROM testcat.ns.$items i
               |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
               |""".stripMargin)
      checkAnswer(hashDf, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))
      val hashPlan = hashDf.queryExecution.executedPlan
      assert(
        collect(hashPlan) { case j: ShuffledHashJoinExecTransformerBase => j }.nonEmpty,
        "expected ShuffledHashJoinExecTransformer")
      assert(
        collect(hashPlan) { case s: ShuffleExchangeLike => s }.isEmpty,
        "should not shuffle for compatible partitioning")
      val hashCoalescing =
        collectAllGroupPartitions(hashPlan).filter(_.groupedPartitions.exists(_._2.size > 1))
      assert(hashCoalescing.nonEmpty, "expected coalescing GroupPartitionsExec")
      hashCoalescing.foreach {
        gp =>
          assert(
            !gp.enableSortedMerge,
            "hash join does not require ordering: enableSortedMerge must stay false")
          assert(
            !gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
            "hash join does not require ordering: must use simple CoalescedRDD")
      }

      val smjDf =
        sql(s"""
               |${selectWithMergeJoinHint("i", "p")}
               |i.id, i.name
               |FROM testcat.ns.$items i
               |JOIN testcat.ns.$purchases p ON p.item_id = i.id AND p.time = i.arrive_time
               |""".stripMargin)
      checkAnswer(smjDf, Seq(Row(1, "aa"), Row(1, "bb"), Row(2, "cc"), Row(2, "ee"), Row(3, "dd")))
      val smjPlan = smjDf.queryExecution.executedPlan
      assert(
        collect(smjPlan) { case j: SortMergeJoinExecTransformer => j }.nonEmpty,
        "expected SortMergeJoinExecTransformer")
      assert(
        collect(smjPlan) { case s: ShuffleExchangeLike => s }.isEmpty,
        "should not shuffle for compatible partitioning")
      val smjCoalescing =
        collectAllGroupPartitions(smjPlan).filter(_.groupedPartitions.exists(_._2.size > 1))
      assert(smjCoalescing.nonEmpty, "expected coalescing GroupPartitionsExec")
      smjCoalescing.foreach {
        gp =>
          assert(
            gp.enableSortedMerge,
            "sort-merge join requires ordering: enableSortedMerge must be true")
          assert(
            gp.execute().isInstanceOf[SortedMergeCoalescedRDD[_]],
            "sort-merge join requires ordering: must use SortedMergeCoalescedRDD")
      }
    }
  }
}
