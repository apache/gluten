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
package org.apache.gluten.integration.table

import org.apache.gluten.integration.{DataGen, TypeModifier}

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

import scala.collection.mutable.ArrayBuffer

class LayoutTableCreator(layout: TableLayout, typeModifiers: Seq[TypeModifier])
  extends TableCreator {
  override def create(spark: SparkSession, source: String, dataPath: String): Unit = {
    val uri = URI.create(dataPath)
    val fs = FileSystem.get(uri, spark.sessionState.newHadoopConf())

    val basePath = new Path(dataPath)
    val statuses = fs.listStatus(basePath)
    val tableDirs = statuses.filter(_.isDirectory).map(_.getPath)

    val tableNames = ArrayBuffer[String]()
    val existedTableNames = ArrayBuffer[String]()
    val createdTableNames = ArrayBuffer[String]()
    val recoveredPartitionTableNames = ArrayBuffer[String]()

    tableDirs.foreach(tablePath => tableNames += tablePath.getName)

    println("Creating catalog tables: " + tableNames.mkString(", ") + "...")

    tableDirs.foreach {
      tablePath =>
        val tableName = tablePath.getName

        if (spark.catalog.tableExists(tableName)) {
          existedTableNames += tableName
        } else {
          val schema = layout.getSchema(tableName)
          val partitionColumns = layout.getPartitionColumns(tableName)

          val rowModifier = DataGen.getRowModifier(schema, typeModifiers)
          val modifiedSchema = DataGen.modifySchema(schema, rowModifier)

          val nonPartitionColumns = modifiedSchema.fields
            .filterNot(f => partitionColumns.contains(f.name))
            .map(f => s"${f.name} ${f.dataType.sql}")
            .mkString(", ")

          val partitionDDL =
            if (partitionColumns.nonEmpty)
              s"PARTITIONED BY (${partitionColumns.map(c => s"$c ${modifiedSchema(c).dataType.sql}").mkString(", ")})"
            else
              ""

          spark.sql(
            s"CREATE TABLE $tableName ($nonPartitionColumns) USING $source $partitionDDL LOCATION '${tablePath.toString}'")

          createdTableNames += tableName

          if (partitionColumns.nonEmpty) {
            try {
              spark.catalog.recoverPartitions(tableName)
              recoveredPartitionTableNames += tableName
            } catch {
              case ae: AnalysisException =>
                Console.err.println(
                  s"Failed to recover partitions for table $tableName: ${ae.message}")
            }
          }
        }
    }

    if (tableNames.isEmpty) {
      return
    }

    if (existedTableNames.nonEmpty) {
      println("Tables already exists: " + existedTableNames.mkString(", ") + ".")
    }

    if (createdTableNames.nonEmpty) {
      println("Tables created: " + createdTableNames.mkString(", ") + ".")
    }

    if (recoveredPartitionTableNames.nonEmpty) {
      println("Recovered partition tables: " + recoveredPartitionTableNames.mkString(", ") + ".")
    }
  }
}
