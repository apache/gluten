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
package org.apache.spark.sql

import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf

import org.apache.hadoop.fs.Path

/**
 * Spark's "Enabling/disabling ignoreCorruptFiles" test of ParquetQuerySuite and OrcQuerySuite.
 *
 * When ignoreCorruptFiles is set, Gluten falls back the scan, so Spark skips the corrupt files as
 * the original test expects. Otherwise the native scan reads them and fails with its own error,
 * where the original test checks the message of Spark's reader, so this test only checks that the
 * query fails.
 */
trait GlutenIgnoreCorruptFilesTest extends GlutenSQLTestsBaseTrait { self: QueryTest =>

  protected def ignoreCorruptFilesTest(format: String): Unit = {
    testGluten("Enabling/disabling ignoreCorruptFiles") {
      // Writes two directories of the format and one of JSON, which isn't a file of the format,
      // or, if allCorrupt, two of JSON. The check gets the read, which infers the schema when it
      // runs, so that the check can expect it to fail.
      def withRead(options: Map[String, String], inferSchema: Boolean, allCorrupt: Boolean)(
          check: (() => DataFrame) => Unit): Unit = {
        withTempDir {
          dir =>
            val paths = (0 until (if (allCorrupt) 2 else 3)).map {
              i =>
                val path = new Path(dir.getCanonicalPath, s"dir$i").toString
                val df = spark.range(i, i + 1).toDF("a")
                if (allCorrupt || i == 2) df.write.json(path)
                else df.write.format(format).save(path)
                path
            }
            val reader = spark.read.options(options).format(format)
            check(() => (if (inferSchema) reader else reader.schema("a long")).load(paths: _*))
        }
      }

      def checkSkipped(options: Map[String, String]): Unit = {
        Seq(true, false).foreach {
          inferSchema =>
            withRead(options, inferSchema, allCorrupt = false)(
              read => checkAnswer(read(), Seq(Row(0), Row(1))))
        }
        withRead(options, inferSchema = false, allCorrupt = true)(
          read => assert(read().count() == 0))
        withRead(options, inferSchema = true, allCorrupt = true) {
          read =>
            val e = intercept[AnalysisException](read())
            assert(e.getMessage.contains("UNABLE_TO_INFER_SCHEMA"))
        }
      }

      // Spark skips the corrupt files, by the conf or by the read option, which wins over it
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
        checkSkipped(Map.empty)
      }
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
        checkSkipped(Map("ignoreCorruptFiles" -> "true"))
      }

      // Otherwise the query fails, also when the read option disables what the conf enables
      Seq(
        "false" -> Map.empty[String, String],
        "true" -> Map("ignoreCorruptFiles" -> "false")).foreach {
        case (conf, options) =>
          withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> conf) {
            Seq(true, false).foreach {
              inferSchema =>
                withRead(options, inferSchema, allCorrupt = false)(
                  read => intercept[SparkException](read().collect()))
            }
          }
      }
    }
  }
}
