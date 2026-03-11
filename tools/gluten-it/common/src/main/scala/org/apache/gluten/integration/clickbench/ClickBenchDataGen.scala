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
package org.apache.gluten.integration.clickbench

import org.apache.gluten.integration.DataGen

import org.apache.spark.sql.{functions, SparkSession}

import org.apache.hadoop.fs.{FileSystem, Path}

class ClickBenchDataGen(dir: String) extends DataGen {

  import ClickBenchDataGen._

  override def gen(spark: SparkSession): Unit = {
    println(s"Start to download ClickBench Parquet dataset from URL: $DATA_URL...")

    val conf = spark.sessionState.newHadoopConf()
    val fs = FileSystem.get(conf)

    val tmpPath = new Path(dir, TMP_FILE_NAME)
    val outputPath = new Path(dir, FILE_NAME)

    if (!fs.exists(tmpPath.getParent)) {
      fs.mkdirs(tmpPath.getParent)
    }

    downloadWithProgress(DATA_URL, tmpPath, fs)

    println(s"ClickBench Parquet dataset successfully downloaded to $tmpPath.")
    println(s"Starting to write a data file $outputPath that is compatible with Spark...")

    spark.read
      .parquet(tmpPath.toString)
      .withColumn("eventtime", functions.col("eventtime").cast("timestamp"))
      .withColumn("clienteventtime", functions.col("clienteventtime").cast("timestamp"))
      .withColumn("localeventtime", functions.col("localeventtime").cast("timestamp"))
      .write
      .mode("overwrite")
      .parquet(outputPath.toString)

    println(s"ClickBench Parquet dataset (Spark compatible) successfully created at $outputPath.")
  }

  private def downloadWithProgress(urlStr: String, target: Path, fs: FileSystem): Unit = {

    import java.net.URL

    val connection = new URL(urlStr).openConnection()
    val contentLength = connection.getContentLengthLong

    val in = connection.getInputStream
    val out = fs.create(target, true)

    val buffer = new Array[Byte](1024 * 1024) // 1MB
    var bytesRead = 0
    var totalRead = 0L
    var nextPrint = 1

    try {
      while ({
        bytesRead = in.read(buffer)
        bytesRead != -1
      }) {
        out.write(buffer, 0, bytesRead)
        totalRead += bytesRead

        if (contentLength > 0) {
          val percent = ((totalRead * 100) / contentLength).toInt
          if (percent >= nextPrint) {
            println(s"Download progress: $percent%")
            nextPrint += 1
          }
        }
      }
    } finally {
      in.close()
      out.close()
    }
  }
}

object ClickBenchDataGen {
  private val DATA_URL =
    "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
  private val TMP_FILE_NAME = "hits.parquet.tmp"
  private[clickbench] val FILE_NAME = "hits.parquet"
}
