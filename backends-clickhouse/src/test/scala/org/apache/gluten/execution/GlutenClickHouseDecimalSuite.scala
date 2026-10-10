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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, rand, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.io.File
import java.util

case class DataTypesWithNonPrimitiveType(
    string_field: String,
    int_field: java.lang.Integer,
    decimal_field: java.math.BigDecimal
)

class GlutenClickHouseDecimalSuite extends ParquetSuite {

  override protected def createTestTables(): Unit = {
    decimalTPCHTables.foreach(t => createDecimalTables(t._1))
  }
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .createDataFrame(genTestData())
      .createTempView(decimalTable)
  }

  private val decimalTable: String = "decimal_table"
  private val decimalTPCHTables: Seq[(DecimalType, Seq[Int])] = Seq.apply(
    (DecimalType.apply(9, 4), Seq()),
    (DecimalType.apply(18, 8), Seq()),
    // 3/10: all value is null and compare with limit
    // 1 Spark 3.5
    (DecimalType.apply(38, 19), Seq(3, 10))
  )

  private def createDecimalTables(dataType: DecimalType): Unit = {
    spark.sql(s"DROP database IF EXISTS decimal_${dataType.precision}_${dataType.scale}")
    spark.sql(s"create database IF not EXISTS decimal_${dataType.precision}_${dataType.scale}")
    spark.sql(s"use decimal_${dataType.precision}_${dataType.scale}")

    // first process the parquet data to:
    // 1. make every column nullable in schema (optional rather than required)
    // 2. salt some null values randomly
    val saltedTablesPath = s"$dataHome/tpch-data-decimal_${dataType.precision}_${dataType.scale}"
    withSQLConf(vanillaSparkConfs(): _*) {
      Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")
        .map(
          tableName => {
            val originTablePath = s"$testParquetAbsolutePath/$tableName"
            spark.read.parquet(originTablePath).createOrReplaceTempView(tableName + "_ori")

            val sql = tableName match {
              case "customer" =>
                s"""
                   |select
                   |  c_custkey,c_name,c_address,c_nationkey,c_phone,
                   |  cast(c_acctbal as decimal(${dataType.precision},${dataType.scale})),
                   |  c_mktsegment,c_comment
                   |from ${tableName}_ori""".stripMargin
              case "lineitem" =>
                s"""
                   |select
                   |  l_orderkey,l_partkey,l_suppkey,l_linenumber,
                   |  cast(l_quantity as decimal(${dataType.precision},${dataType.scale})),
                   |  cast(l_extendedprice as decimal(${dataType.precision},${dataType.scale})),
                   |  cast(l_discount as decimal(${dataType.precision},${dataType.scale})),
                   |  cast(l_tax as decimal(${dataType.precision},${dataType.scale})),
                   |  l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,
                   |  l_shipinstruct,l_shipmode,l_comment
                   |from ${tableName}_ori """.stripMargin
              case "orders" =>
                s"""
                   |select
                   |  o_orderkey,o_custkey,o_orderstatus,
                   |  cast(o_totalprice as decimal(${dataType.precision},${dataType.scale})),
                   |  o_orderdate,
                   |  o_orderpriority,o_clerk,o_shippriority,o_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case "part" =>
                s"""
                   |select
                   |  p_partkey,p_name,p_mfgr,p_brand,p_type,p_size,p_container,
                   |  cast(p_retailprice as decimal(${dataType.precision},${dataType.scale})),
                   |  p_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case "partsupp" =>
                s"""
                   |select
                   |  ps_partkey,ps_suppkey,ps_availqty,
                   |  cast(ps_supplycost as decimal(${dataType.precision},${dataType.scale})),
                   |  ps_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case "supplier" =>
                s"""
                   |select
                   |  s_suppkey,s_name,s_address,s_nationkey,s_phone,
                   |  cast(s_acctbal as decimal(${dataType.precision},${dataType.scale})),s_comment
                   |from ${tableName}_ori
                   |""".stripMargin
              case _ => s"select * from ${tableName}_ori"
            }

            val df = spark.sql(sql).toDF()
            var salted_df: Option[DataFrame] = None
            for (c <- df.schema) {
              salted_df = Some((salted_df match {
                case Some(x) => x
                case None => df
              }).withColumn(c.name, when(rand() < 0.1, null).otherwise(col(c.name))))
            }

            val currentSaltedTablePath = saltedTablesPath + "/" + tableName
            val file = new File(currentSaltedTablePath)
            if (file.exists()) {
              file.delete()
            }

            salted_df.get.write.parquet(currentSaltedTablePath)
          })
    }

    val customerData = saltedTablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    decimal(${dataType.precision},${dataType.scale}),
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING PARQUET LOCATION '$customerData'
                 |""".stripMargin)

    val lineitemData = saltedTablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS lineitem (
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      decimal(${dataType.precision},${dataType.scale}),
                 | l_extendedprice decimal(${dataType.precision},${dataType.scale}),
                 | l_discount      decimal(${dataType.precision},${dataType.scale}),
                 | l_tax           decimal(${dataType.precision},${dataType.scale}),
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string)
                 | USING PARQUET LOCATION '$lineitemData'
                 |""".stripMargin)

    val nationData = saltedTablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS nation (
                 | n_nationkey bigint,
                 | n_name      string,
                 | n_regionkey bigint,
                 | n_comment   string)
                 | USING PARQUET LOCATION '$nationData'
                 |""".stripMargin)

    val regionData = saltedTablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS region (
                 | r_regionkey bigint,
                 | r_name      string,
                 | r_comment   string)
                 | USING PARQUET LOCATION '$regionData'
                 |""".stripMargin)

    val ordersData = saltedTablesPath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS orders (
                 | o_orderkey      bigint,
                 | o_custkey       bigint,
                 | o_orderstatus   string,
                 | o_totalprice    decimal(${dataType.precision},${dataType.scale}),
                 | o_orderdate     date,
                 | o_orderpriority string,
                 | o_clerk         string,
                 | o_shippriority  bigint,
                 | o_comment       string)
                 | USING PARQUET LOCATION '$ordersData'
                 |""".stripMargin)

    val partData = saltedTablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice decimal(${dataType.precision},${dataType.scale}),
                 | p_comment     string)
                 | USING PARQUET LOCATION '$partData'
                 |""".stripMargin)

    val partsuppData = saltedTablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS partsupp (
                 | ps_partkey    bigint,
                 | ps_suppkey    bigint,
                 | ps_availqty   bigint,
                 | ps_supplycost decimal(${dataType.precision},${dataType.scale}),
                 | ps_comment    string)
                 | USING PARQUET LOCATION '$partsuppData'
                 |""".stripMargin)

    val supplierData = saltedTablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   decimal(${dataType.precision},${dataType.scale}),
                 | s_comment   string)
                 | USING PARQUET LOCATION '$supplierData'
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 16)
    spark.sql(s"use default")
  }

  test("from decimalArithmeticOperations.sql") {
    // prepare
    val createSql =
      "create table decimals_test(id int, a decimal(38,18), b decimal(38,18)) using parquet"
    val inserts =
      "insert into decimals_test values(1, 100.0, 999.0)" +
        ", (2, 12345.123, 12345.123)" +
        ", (3, 0.1234567891011, 1234.1)" +
        ", (4, 123456789123456789.0, 1.123456789123456789)" +
        ", (5, 0, 0)" +
        ", (6, 0, 1.23)" +
        ", (7, 1.23, 0)"
    spark.sql(createSql)

    try {
      spark.sql(inserts)

      val q1 = "select id, a+b, a-b, a*b, a/b ,a%b from decimals_test order by id"

      // test operations between decimals and constants
      val q2 = "select id, a*10, b/10 from decimals_test order by id"
      // FIXME val q2 = "select id, a*10, b/10, a%20, b%30 from decimals_test order by id"

      Seq("true", "false").foreach {
        allowPrecisionLoss =>
          withSQLConf((SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key, allowPrecisionLoss)) {
            compareResultsAgainstVanillaSpark(q1, compareResult = true, _ => {})
            compareResultsAgainstVanillaSpark(q2, compareResult = true, _ => {})
          }
      }
    } finally {
      spark.sql("drop table if exists decimals_test")
    }
  }

  test("test castornull") {
    // prepare
    val createSql =
      "create table decimals_cast_test(a decimal(18,8)) using parquet"
    val inserts =
      "insert into decimals_cast_test values(123456789.12345678)"
    spark.sql(createSql)

    try {
      spark.sql(inserts)
      val q1 = "select cast(a as decimal(9,2)) from decimals_cast_test"
      compareResultsAgainstVanillaSpark(q1, compareResult = true, _ => {})
    } finally {
      spark.sql("drop table if exists decimals_cast_test")
    }
  }

  // FIXME: Support AVG for Decimal Type
  Seq("true", "false").foreach {
    allowPrecisionLoss =>
      Range
        .inclusive(1, 22)
        .foreach {
          sql_num =>
            {
              decimalTPCHTables.foreach {
                dt =>
                  {
                    val compareResult = !dt._2.contains(sql_num)
                    val compareStr = if (compareResult) "compare" else "noCompare"
                    val PrecisionLoss = s"allowPrecisionLoss=$allowPrecisionLoss"
                    val decimalType = dt._1
                    test(s"""TPCH Decimal(${decimalType.precision},${decimalType.scale})
                            | Q$sql_num[$PrecisionLoss,native,$compareStr]""".stripMargin) {
                      spark.sql(s"use decimal_${decimalType.precision}_${decimalType.scale}")
                      withSQLConf(
                        (SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key, allowPrecisionLoss)) {
                        check(sql_num, compareResult)
                      }
                      spark.sql(s"use default")
                    }
                  }
              }
            }
        }
  }

  test("fix decimal precision overflow") {
    val sql =
      s"""
         | select
         |     cast(int_field  as decimal(20, 2))
         |         * cast(decimal_field as decimal(30, 2)) * decimal_field
         | from $decimalTable
         | limit 1
         |""".stripMargin
    withSQLConf(vanillaSparkConfs(): _*) {
      val df2 = spark.sql(sql)
      print(df2.queryExecution.executedPlan)
    }
    testFromRandomBase(
      sql,
      _ => {}
    )
  }

  test("GLUTEN-8074 Fix adjust output constant column") {
    val sql =
      s"""
         |select bround(1002.5786, 3),
         |       bround(-10.8, 0),
         |       bround(13.888888888889, 5)
         |from $decimalTable
         |WHERE bround(cast(decimal_field as decimal(30, 2)), 1) > 0 LIMIT 2;
         |""".stripMargin
    runQueryAndCompare(sql)(_ => {})
  }

  test("fix decimal32 with negative value") {
    val schema = StructType.apply(
      Seq(
        StructField.apply("decimal32_field", DecimalType.apply(8, 4), nullable = true)
      ))
    val dataCorrect = new util.ArrayList[Row]()
    dataCorrect.add(Row(new java.math.BigDecimal(1.123)))
    dataCorrect.add(Row(new java.math.BigDecimal(-2.123)))
    dataCorrect.add(Row(new java.math.BigDecimal(-3.123)))
    dataCorrect.add(Row(null))
    spark.createDataFrame(dataCorrect, schema).createTempView("decimal32_table")

    val sql_nullable =
      s"""
         | select
         |     *
         | from decimal32_table
         |""".stripMargin

    val sql_not_null =
      s"""
         | select
         |     *
         | from decimal32_table
         | where decimal32_field < 0
         |""".stripMargin

    compareResultsAgainstVanillaSpark(sql_nullable, compareResult = true, _ => {})
    compareResultsAgainstVanillaSpark(sql_not_null, compareResult = true, _ => {})
  }

  test("bigint % 6.1") {
    val sql =
      s"""
         | select
         |     s_suppkey,
         |     s_suppkey % 6.1
         | from supplier
         |""".stripMargin
    spark.sql(s"use decimal_${9}_${4}")
    withSQLConf(vanillaSparkConfs(): _*) {
      val df2 = spark.sql(sql)
      print(df2.queryExecution.executedPlan)
    }
    testFromRandomBase(
      sql,
      _ => {}
    )
  }

  test("Fix issue(6015) allow overflow when converting decimal to integer") {
    val sql = "select int(cast(id * 9999999999 as decimal(29, 2))) from range(10)"
    runQueryAndCompare(sql)(checkGlutenPlan[ProjectExecTransformer])
  }

  def testFromRandomBase(
      sql: String,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true
  ): Unit = {
    compareResultsAgainstVanillaSpark(
      sql,
      compareResult = true,
      customCheck,
      noFallBack = noFallBack)
  }

  def genTestData(): Seq[DataTypesWithNonPrimitiveType] = {
    (0 to 300).map {
      i =>
        if (i % 100 == 1) {
          // scalastyle:off nonascii
          DataTypesWithNonPrimitiveType(
            "测试中文",
            Integer.MAX_VALUE,
            new java.math.BigDecimal(Integer.MAX_VALUE + ".56"))
          // scalastyle:on nonascii
        } else if (i % 10 == 0) {
          DataTypesWithNonPrimitiveType(
            s"${i / 1000}",
            Integer.MAX_VALUE,
            new java.math.BigDecimal(i + ".56"))
        } else if (i % 50 == 0) {
          DataTypesWithNonPrimitiveType(null, null, null)
        } else {
          DataTypesWithNonPrimitiveType(s"${i / 1000}", i, new java.math.BigDecimal(i + ".56"))
        }
    }
  }
}
