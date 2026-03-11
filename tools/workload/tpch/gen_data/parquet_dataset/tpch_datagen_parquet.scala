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
import com.databricks.spark.sql.perf.tpch._

import java.io.File


val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = 200  // how many dsdgen partitions to run - number of input tasks.

val format = "parquet" // valid spark format like parquet "parquet".
val rootDir = "/PATH/TO/TPCH_PARQUET_PATH" // root directory of location to create data in.

/**
 * Location of dbgen.
 * Please compile with https://github.com/databricks/tpch-dbgen.git.
 */
val dbgenDir = "/PATH/TO/TPCH_DBGEN"

val tables = new TPCHTables(spark.sqlContext,
    dbgenDir = dbgenDir,
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType

object FileUtils {
  def deleteFilesWithKeyword(path: String, keyword: String): Unit = {
    val dir = new File(path)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles()
        .filter(f => f.isFile && f.getName.contains(keyword))
        .foreach { f =>
          f.delete()
        }
    }
  }
}

FileUtils.deleteFilesWithKeyword(dbgenDir, "tbl")

tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = false, // do not create the partitioned fact tables
    clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = numPartitions) // how many dsdgen partitions to run - number of input tasks.

