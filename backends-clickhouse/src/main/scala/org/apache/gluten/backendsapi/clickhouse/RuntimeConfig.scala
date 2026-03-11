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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.spark.sql.internal.SQLConf

object RuntimeConfig {

  import CHConfig.runtimeConfig
  import SQLConf._

  /** Clickhouse Configuration */
  val PATH =
    buildConf(runtimeConfig("path"))
      .doc(
        "https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#path")
      .stringConf
      .createWithDefault("/")

  // scalastyle:off line.size.limit
  val TMP_PATH =
    buildConf(runtimeConfig("tmp_path"))
      .doc("https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#tmp-path")
      .stringConf
      .createWithDefault("/tmp/libch")
  // scalastyle:on line.size.limit

  // scalastyle:off line.size.limit
  val LOGGER_LEVEL =
    buildConf(runtimeConfig("logger.level"))
      .doc(
        "https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings#logger")
      .stringConf
      .createWithDefault("warning")
  // scalastyle:on line.size.limit

  /** Gluten Configuration */
  val USE_CURRENT_DIRECTORY_AS_TMP =
    buildConf(runtimeConfig("use_current_directory_as_tmp"))
      .doc("Use the current directory as the temporary directory.")
      .booleanConf
      .createWithDefault(false)

  val DUMP_PIPELINE =
    buildConf(runtimeConfig("dump_pipeline"))
      .doc("Dump pipeline to file after execution")
      .booleanConf
      .createWithDefault(false)
}
