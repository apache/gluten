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
package org.apache.gluten.integration

import org.apache.spark.sql.SparkSession

trait TableAnalyzer {
  def analyze(spark: SparkSession): Unit
}

object TableAnalyzer {
  def noop(): TableAnalyzer = {
    Noop
  }

  def analyzeAll(): TableAnalyzer = {
    AnalyzeAll
  }

  private object Noop extends TableAnalyzer {
    override def analyze(spark: SparkSession): Unit = {
      // Do nothing.
    }
  }

  private object AnalyzeAll extends TableAnalyzer {
    override def analyze(spark: SparkSession): Unit = {
      val tables = spark.catalog.listTables().collect()
      tables.foreach {
        tab =>
          val tableName = tab.name
          val tableColumnNames = spark.catalog.listColumns(tableName).collect().map(c => c.name)
          println(s"Analyzing catalog table: $tableName [${tableColumnNames.mkString(", ")}]...")
          spark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
          spark.sql(
            s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS ${tableColumnNames.mkString(", ")}")
          println(s"Catalog table analyzed: $tableName.")
      }
    }
  }
}
