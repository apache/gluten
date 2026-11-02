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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.internal.LegacyBehaviorPolicy
import org.apache.spark.sql.internal.LegacyBehaviorPolicy.{CORRECTED, EXCEPTION, LEGACY}
import org.apache.spark.sql.internal.SQLConf

import java.sql.Date

class GlutenParquetRebaseDatetimeV1Suite
  extends ParquetRebaseDatetimeV1Suite
  with GlutenSQLTestsBaseTrait {

  import testImplicits._

  override protected def getResourceParquetFilePath(name: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + name
  }

  private def inReadConfToOptions(
      conf: String,
      mode: LegacyBehaviorPolicy.Value): Map[String, String] = conf match {
    case SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key =>
      Map(ParquetOptions.INT96_REBASE_MODE -> mode.toString)
    case _ => Map(ParquetOptions.DATETIME_REBASE_MODE -> mode.toString)
  }

  private def runInMode(conf: String, modes: Seq[LegacyBehaviorPolicy.Value])(
      f: Map[String, String] => Unit): Unit = {
    modes.foreach(mode => withSQLConf(conf -> mode.toString)(f(Map.empty)))
    withSQLConf(conf -> EXCEPTION.toString) {
      modes.foreach(mode => f(inReadConfToOptions(conf, mode)))
    }
  }

  // gluten does not consider file metadata which indicates needs rebase or not
  // it only supports write the parquet file as CORRECTED
  testGluten("SPARK-31159: rebasing dates in write") {
    val N = 8
    Seq(false, true).foreach {
      dictionaryEncoding =>
        withTempPath {
          dir =>
            val path = dir.getAbsolutePath
            withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> CORRECTED.toString) {
              Seq
                .tabulate(N)(_ => "1001-01-01")
                .toDF("dateS")
                .select($"dateS".cast("date").as("date"))
                .repartition(1)
                .write
                .option("parquet.enable.dictionary", dictionaryEncoding)
                .parquet(path)
            }

            withAllParquetReaders {
              // The file metadata indicates if it needs rebase or not, so we can always get the
              // correct result regardless of the "rebase mode" config.
              runInMode(
                SQLConf.PARQUET_REBASE_MODE_IN_READ.key,
                Seq(LEGACY, CORRECTED, EXCEPTION)) {
                options =>
                  checkAnswer(
                    spark.read.options(options).parquet(path),
                    Seq.tabulate(N)(_ => Row(Date.valueOf("1001-01-01"))))
              }

              // Force to not rebase to prove the written datetime values are rebased
              // and we will get wrong result if we don't rebase while reading.
              // gluten not support this mode
//          withSQLConf("spark.test.forceNoRebase" -> "true") {
//            checkAnswer(
//              spark.read.parquet(path),
//              Seq.tabulate(N)(_ => Row(Date.valueOf("1001-01-07"))))
//          }
            }
        }
    }
  }
}

class GlutenParquetRebaseDatetimeV2Suite
  extends ParquetRebaseDatetimeV2Suite
  with GlutenSQLTestsBaseTrait {

  override protected def getResourceParquetFilePath(name: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + name
  }
}
