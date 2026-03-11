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
package org.apache.gluten.integration.h

import org.apache.gluten.integration.{DataGen, TypeModifier}
import org.apache.gluten.integration.shim.Shim

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import io.trino.tpch._

import java.io.File
import java.sql.Date

import scala.collection.JavaConverters._

class TpchDataGen(
    scale: Double,
    partitions: Int,
    source: String,
    dir: String,
    featureNames: Seq[String],
    typeModifiers: Seq[TypeModifier])
  extends Serializable
  with DataGen {

  private val featureRegistry = new DataGen.FeatureRegistry
  private val features = featureNames.map(featureRegistry.getFeature)

  override def gen(spark: SparkSession): Unit = {
    generate(spark, dir, "lineitem", lineItemSchema, partitions, lineItemGenerator, lineItemParser)
    generate(spark, dir, "customer", customerSchema, partitions, customerGenerator, customerParser)
    generate(spark, dir, "orders", orderSchema, partitions, orderGenerator, orderParser)
    generate(
      spark,
      dir,
      "partsupp",
      partSupplierSchema,
      partitions,
      partSupplierGenerator,
      partSupplierParser)
    generate(spark, dir, "supplier", supplierSchema, partitions, supplierGenerator, supplierParser)
    generate(spark, dir, "nation", nationSchema, nationGenerator, nationParser)
    generate(spark, dir, "part", partSchema, partitions, partGenerator, partParser)
    generate(spark, dir, "region", regionSchema, regionGenerator, regionParser)

    features.foreach(feature => DataGen.Feature.run(spark, source, feature))
  }

  // lineitem
  private def lineItemGenerator = {
    (part: Int, partCount: Int) => new LineItemGenerator(scale, part, partCount)
  }

  private def lineItemSchema = {
    StructType(
      Seq(
        StructField("l_orderkey", LongType),
        StructField("l_partkey", LongType),
        StructField("l_suppkey", LongType),
        StructField("l_linenumber", IntegerType),
        StructField("l_quantity", DecimalType(12, 2)),
        StructField("l_extendedprice", DecimalType(12, 2)),
        StructField("l_discount", DecimalType(12, 2)),
        StructField("l_tax", DecimalType(12, 2)),
        StructField("l_returnflag", StringType),
        StructField("l_linestatus", StringType),
        StructField("l_commitdate", DateType),
        StructField("l_receiptdate", DateType),
        StructField("l_shipinstruct", StringType),
        StructField("l_shipmode", StringType),
        StructField("l_comment", StringType),
        StructField("l_shipdate", DateType)
      ))
  }

  private def lineItemParser: LineItem => Row =
    lineItem =>
      Row(
        lineItem.getOrderKey,
        lineItem.getPartKey,
        lineItem.getSupplierKey,
        lineItem.getLineNumber,
        BigDecimal.valueOf(lineItem.getQuantity),
        BigDecimal.valueOf(lineItem.getExtendedPrice),
        BigDecimal.valueOf(lineItem.getDiscount),
        BigDecimal.valueOf(lineItem.getTax),
        lineItem.getReturnFlag,
        lineItem.getStatus,
        Date.valueOf(GenerateUtils.formatDate(lineItem.getCommitDate)),
        Date.valueOf(GenerateUtils.formatDate(lineItem.getReceiptDate)),
        lineItem.getShipInstructions,
        lineItem.getShipMode,
        lineItem.getComment,
        Date.valueOf(GenerateUtils.formatDate(lineItem.getShipDate))
      )

  // customer
  private def customerGenerator = {
    (part: Int, partCount: Int) => new CustomerGenerator(scale, part, partCount)
  }

  private def customerSchema = {
    StructType(
      Seq(
        StructField("c_custkey", LongType),
        StructField("c_name", StringType),
        StructField("c_address", StringType),
        StructField("c_nationkey", LongType),
        StructField("c_phone", StringType),
        StructField("c_acctbal", DecimalType(12, 2)),
        StructField("c_comment", StringType),
        StructField("c_mktsegment", StringType)
      ))
  }

  private def customerParser: Customer => Row =
    customer =>
      Row(
        customer.getCustomerKey,
        customer.getName,
        customer.getAddress,
        customer.getNationKey,
        customer.getPhone,
        BigDecimal.valueOf(customer.getAccountBalance),
        customer.getComment,
        customer.getMarketSegment
      )

  // orders
  private def orderGenerator = {
    (part: Int, partCount: Int) => new OrderGenerator(scale, part, partCount)
  }

  private def orderSchema = {
    StructType(
      Seq(
        StructField("o_orderkey", LongType),
        StructField("o_custkey", LongType),
        StructField("o_orderstatus", StringType),
        StructField("o_totalprice", DecimalType(12, 2)),
        StructField("o_orderpriority", StringType),
        StructField("o_clerk", StringType),
        StructField("o_shippriority", IntegerType),
        StructField("o_comment", StringType),
        StructField("o_orderdate", DateType)
      ))
  }

  private def orderParser: Order => Row =
    order =>
      Row(
        order.getOrderKey,
        order.getCustomerKey,
        String.valueOf(order.getOrderStatus),
        BigDecimal.valueOf(order.getTotalPrice),
        order.getOrderPriority,
        order.getClerk,
        order.getShipPriority,
        order.getComment,
        Date.valueOf(GenerateUtils.formatDate(order.getOrderDate))
      )

  // partsupp
  private def partSupplierGenerator = {
    (part: Int, partCount: Int) => new PartSupplierGenerator(scale, part, partCount)
  }

  private def partSupplierSchema = {
    StructType(
      Seq(
        StructField("ps_partkey", LongType),
        StructField("ps_suppkey", LongType),
        StructField("ps_availqty", IntegerType),
        StructField("ps_supplycost", DecimalType(12, 2)),
        StructField("ps_comment", StringType)
      ))
  }

  private def partSupplierParser: PartSupplier => Row =
    ps =>
      Row(
        ps.getPartKey,
        ps.getSupplierKey,
        ps.getAvailableQuantity,
        BigDecimal.valueOf(ps.getSupplyCost),
        ps.getComment)

  // supplier
  private def supplierGenerator = {
    (part: Int, partCount: Int) => new SupplierGenerator(scale, part, partCount)
  }

  private def supplierSchema = {
    StructType(
      Seq(
        StructField("s_suppkey", LongType),
        StructField("s_name", StringType),
        StructField("s_address", StringType),
        StructField("s_nationkey", LongType),
        StructField("s_phone", StringType),
        StructField("s_acctbal", DecimalType(12, 2)),
        StructField("s_comment", StringType)
      ))
  }

  private def supplierParser: Supplier => Row =
    s =>
      Row(
        s.getSupplierKey,
        s.getName,
        s.getAddress,
        s.getNationKey,
        s.getPhone,
        BigDecimal.valueOf(s.getAccountBalance),
        s.getComment)

  // nation
  private def nationGenerator = { () => new NationGenerator() }

  private def nationSchema = {
    StructType(
      Seq(
        StructField("n_nationkey", LongType),
        StructField("n_name", StringType),
        StructField("n_regionkey", LongType),
        StructField("n_comment", StringType)))
  }

  private def nationParser: Nation => Row =
    nation => Row(nation.getNationKey, nation.getName, nation.getRegionKey, nation.getComment)

  // part
  private def partGenerator = {
    (part: Int, partCount: Int) => new PartGenerator(scale, part, partCount)
  }

  private def partSchema = {
    StructType(
      Seq(
        StructField("p_partkey", LongType),
        StructField("p_name", StringType),
        StructField("p_mfgr", StringType),
        StructField("p_type", StringType),
        StructField("p_size", IntegerType),
        StructField("p_container", StringType),
        StructField("p_retailprice", DecimalType(12, 2)),
        StructField("p_comment", StringType),
        StructField("p_brand", StringType)
      ))
  }

  private def partParser: Part => Row =
    part =>
      Row(
        part.getPartKey,
        part.getName,
        part.getManufacturer,
        part.getType,
        part.getSize,
        part.getContainer,
        BigDecimal.valueOf(part.getRetailPrice),
        part.getComment,
        part.getBrand
      )

  // region
  private def regionGenerator = { () => new RegionGenerator() }

  private def regionSchema = {
    StructType(
      Seq(
        StructField("r_regionkey", LongType),
        StructField("r_name", StringType),
        StructField("r_comment", StringType)))
  }

  private def regionParser: Region => Row =
    region => Row(region.getRegionKey, region.getName, region.getComment)

  // gen tpc-h data
  private def generate[U](
      spark: SparkSession,
      dir: String,
      tableName: String,
      schema: StructType,
      gen: () => java.lang.Iterable[U],
      parser: U => Row): Unit = {
    generate(
      spark,
      dir,
      tableName,
      schema,
      1,
      (_: Int, _: Int) => {
        gen.apply()
      },
      parser)
  }

  private def generate[U](
      spark: SparkSession,
      dir: String,
      tableName: String,
      schema: StructType,
      partitions: Int,
      gen: (Int, Int) => java.lang.Iterable[U],
      parser: U => Row): Unit = {
    println(s"Generating table $tableName...")
    val rowModifier = DataGen.getRowModifier(schema, typeModifiers)
    val modifiedSchema = DataGen.modifySchema(schema, rowModifier)
    spark
      .range(0, partitions, 1L, partitions)
      .mapPartitions {
        itr =>
          val id = itr.toArray
          if (id.length != 1) {
            throw new IllegalStateException()
          }
          val data = gen.apply(id(0).toInt + 1, partitions)
          val dataItr = data.iterator()
          val rows = dataItr.asScala.map {
            item =>
              val row = parser(item)
              val modifiedRow = Row(row.toSeq.zipWithIndex.map {
                case (v, i) =>
                  val modifier = rowModifier.apply(i)
                  modifier.modValue(v)
              }.toArray: _*)
              modifiedRow
          }
          rows
      }(Shim.getExpressionEncoder(modifiedSchema))
      .write
      .format(source)
      .mode(SaveMode.Overwrite)
      .option("path", dir + File.separator + tableName) // storage location
      .saveAsTable(tableName)
  }
}
