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
package org.apache.spark.sql.execution.python

import org.apache.gluten.execution.FilterExecTransformerBase

import org.apache.spark.sql.{GlutenSQLTestsTrait, IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.python.PythonScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class GlutenPythonDataSourceSuite extends PythonDataSourceSuite with GlutenSQLTestsTrait {

  import IntegratedUDFTestUtils._

  // Gluten replaces FilterExec with FilterExecTransformer and
  // BatchScanExec with BatchScanExecTransformer
  testGluten("data source reader with filter pushdown") {
    assume(shouldTestPandasUDFs)
    val dataSourceScript =
      s"""
         |from pyspark.sql.datasource import (
         |    DataSource,
         |    DataSourceReader,
         |    EqualTo,
         |    InputPartition,
         |)
         |
         |class SimpleDataSourceReader(DataSourceReader):
         |    def partitions(self):
         |        return [InputPartition(i) for i in range(2)]
         |
         |    def pushFilters(self, filters):
         |        for filter in filters:
         |            if filter != EqualTo(("partition",), 0):
         |                yield filter
         |
         |    def read(self, partition):
         |        yield (0, partition.value)
         |        yield (1, partition.value)
         |        yield (2, partition.value)
         |
         |class SimpleDataSource(DataSource):
         |    def schema(self):
         |        return "id int, partition int"
         |
         |    def reader(self, schema):
         |        return SimpleDataSourceReader()
         |""".stripMargin
    val schema = StructType.fromDDL("id INT, partition INT")
    val dataSource =
      createUserDefinedPythonDataSource(name = dataSourceName, pythonScript = dataSourceScript)
    withSQLConf(SQLConf.PYTHON_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      spark.dataSource.registerPython(dataSourceName, dataSource)
      val df =
        spark.read.format(dataSourceName).schema(schema).load().filter("id = 1 and partition = 0")
      val plan = df.queryExecution.executedPlan

      val filter = collectFirst(plan) {
        case s: FilterExecTransformerBase =>
          val condition = s.cond.toString
          assert(!condition.contains("= 0"))
          assert(condition.contains("= 1"))
          s
      }.getOrElse(
        fail(s"FilterExecTransformerBase not found in the plan. Actual plan:\n$plan")
      )

      // Gluten does not replace PythonScan's BatchScanExec - it stays as vanilla
      // BatchScanExec with RowToVeloxColumnar transition
      collectFirst(filter) {
        case s: BatchScanExec if s.scan.isInstanceOf[PythonScan] =>
          val p = s.scan.asInstanceOf[PythonScan]
          assert(p.getMetaData().get("PushedFilters").contains("[EqualTo(partition,0)]"))
      }.getOrElse(
        fail(s"BatchScanExec with PythonScan not found. Actual plan:\n$plan")
      )

      checkAnswer(df, Seq(Row(1, 0), Row(1, 1)))
    }
  }
}
