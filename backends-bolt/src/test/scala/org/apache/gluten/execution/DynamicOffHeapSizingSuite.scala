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

import org.apache.gluten.config.GlutenCoreConfig
import org.apache.gluten.memory.memtarget.DynamicOffHeapSizingMemoryTarget

import org.apache.spark.SparkConf

class DynamicOffHeapSizingSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.executor.memory", "2GB")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.memory.offHeap.size", "0")
      .set(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION.key, "0.95")
      .set(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, "true")
  }

  test("Dynamic off-heap sizing") {
    if (DynamicOffHeapSizingMemoryTarget.isJava9OrLater()) {
      val query =
        """
          | select l_quantity, c_acctbal, o_orderdate, p_type, n_name, s_suppkey
          | from customer, orders, lineitem, part, supplier, nation
          | where c_custkey = o_custkey and o_orderkey = l_orderkey and l_partkey = p_partkey
          | and l_suppkey = s_suppkey and s_nationkey = n_nationkey
          | order by c_acctbal desc, o_orderdate, s_suppkey, n_name, p_type, l_quantity
          | limit 1
      """.stripMargin
      var totalMemory = Runtime.getRuntime().totalMemory()
      var freeMemory = Runtime.getRuntime().freeMemory()
      // Ensure that the JVM memory is not too small to trigger dynamic off-heap sizing.
      while (!DynamicOffHeapSizingMemoryTarget.canShrinkJVMMemory(totalMemory, freeMemory)) {
        withSQLConf(("spark.gluten.enabled", "false")) {
          spark.sql(query).collect()
        }
        totalMemory = Runtime.getRuntime().totalMemory()
        freeMemory = Runtime.getRuntime().freeMemory()
      }
      val newTotalMemory =
        DynamicOffHeapSizingMemoryTarget.shrinkOnHeapMemory(totalMemory, freeMemory, false)
      assert(DynamicOffHeapSizingMemoryTarget.getTotalExplicitGCCount() > 0)
      // Verify that the total memory is reduced after shrink.
      assert(newTotalMemory < totalMemory)
      // Verify that the query can run with dynamic off-heap sizing enabled.
      runAndCompare(query)
    }
  }
}
