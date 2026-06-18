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
package org.apache.gluten.integration.ds

import org.apache.gluten.integration.{DataGen, TypeModifier}
import org.apache.gluten.integration.shim.Shim

import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import io.trino.tpcds._

import java.io.File

import scala.collection.JavaConverters._

class TpcdsDataGen(
    source: String,
    tableLayout: TpcdsTableLayout,
    scale: Double,
    partitions: Int,
    dir: String,
    featureNames: Seq[String],
    typeModifiers: Seq[TypeModifier]
) extends Serializable
  with DataGen {

  private val featureRegistry = new DataGen.FeatureRegistry

  featureRegistry.register(TpcdsDataGenFeatures.EnableDeltaDeletionVector)
  featureRegistry.register(TpcdsDataGenFeatures.DeleteTenPercentData)

  private val features = featureNames.map(featureRegistry.getFeature)

  def writeParquetTable(spark: SparkSession, t: Table): Unit = {
    val name = t.getName
    if (name.equals("dbgen_version")) {
      return
    }
    val schema = tableLayout.getSchema(name)
    val partitionBy = tableLayout.getPartitionColumns(name)

    writeParquetTable(spark, name, t, schema, partitionBy)
  }

  private def writeParquetTable(
      spark: SparkSession,
      tableName: String,
      t: Table,
      schema: StructType,
      partitionBy: Seq[String]): Unit = {
    println(s"Generating table $tableName...")
    val rowModifier = DataGen.getRowModifier(schema, typeModifiers)
    val modifiedSchema = DataGen.modifySchema(schema, rowModifier)

    val stringSchema = StructType(modifiedSchema.fields.map(f => StructField(f.name, StringType)))

    val columns = modifiedSchema.fields.map(f => new Column(f.name).cast(f.dataType).as(f.name))
    spark
      .range(0, partitions, 1L, partitions)
      .mapPartitions {
        itr =>
          val id = itr.toArray
          if (id.length != 1) {
            throw new IllegalStateException()
          }
          val options = new Options()
          options.scale = scale
          options.parallelism = partitions
          val session = options.toSession
          val chunkSession = session.withChunkNumber(id(0).toInt + 1)
          val results = Results.constructResults(t, chunkSession).asScala.toIterator
          results.map {
            parentAndChildRow =>
              // Skip child table when generating parent table,
              // we generate every table individually no matter it is parent or child.
              val array: Array[String] = parentAndChildRow.get(0).asScala.toArray
              Row(array: _*)
          }
      }(Shim.getExpressionEncoder(stringSchema))
      .select(columns: _*)
      .write
      .format(source)
      .partitionBy(partitionBy.toArray: _*)
      .mode(SaveMode.Overwrite)
      .option("path", dir + File.separator + tableName) // storage location
      .saveAsTable(tableName)
  }

  override def gen(spark: SparkSession): Unit = {
    Table.getBaseTables.forEach(t => writeParquetTable(spark, t))

    features.foreach(feature => DataGen.Feature.run(spark, source, feature))
  }
}

object TpcdsDataGen {}
