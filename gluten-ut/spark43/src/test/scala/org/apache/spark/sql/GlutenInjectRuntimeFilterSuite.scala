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

class GlutenInjectRuntimeFilterSuite extends InjectRuntimeFilterSuite with GlutenSQLTestsBaseTrait {

  testGluten("GLUTEN-9849: bloom filter applied to partition filter") {
    withSQLConf(
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000"
    ) {
      assertRewroteWithBloomFilter(
        "select * from bf5part join bf2 on " +
          "bf5part.f5 = bf2.c2 where bf2.a2 = 67")
    }
  }

}
