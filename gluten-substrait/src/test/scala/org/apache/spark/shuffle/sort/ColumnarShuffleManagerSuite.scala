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
package org.apache.spark.shuffle.sort

import org.apache.spark.SparkConf

import org.scalatest.funsuite.AnyFunSuiteLike

class ColumnarShuffleManagerSuite extends AnyFunSuiteLike {

  // ShuffleExchangeExec.needToCopyObjectsBeforeShuffle checks
  // `SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]` and, when that is false, falls
  // through to a catch-all `true` that makes every map task run UnsafeRow.copy() per row. Since
  // spark.shuffle.manager is process-wide, breaking this subtype relationship silently regresses
  // every row-based exchange: no exception, no test failure, only lost throughput.
  test("is a SortShuffleManager so row-based exchanges keep Spark's zero-copy write path") {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ColumnarShuffleManagerSuite")
    val shuffleManager = new ColumnarShuffleManager(conf)
    try {
      assert(shuffleManager.isInstanceOf[SortShuffleManager])
    } finally {
      shuffleManager.stop()
    }
  }
}
