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

import org.apache.spark.sql.internal.SQLConf

class GlutenMapStatusEndToEndSuite extends MapStatusEndToEndSuite with GlutenTestsTrait {

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark.sparkContext.conf.set(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key, "5")
    _spark.conf.set(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key, "5")

    _spark.sparkContext.conf
      .set(SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key, "false")
    _spark.conf.set(SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key, "false")
  }
}
