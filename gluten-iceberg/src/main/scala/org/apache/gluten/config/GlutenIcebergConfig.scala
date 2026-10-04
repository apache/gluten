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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

class GlutenIcebergConfig(conf: SQLConf) extends GlutenCoreConfig(conf) {
  import GlutenIcebergConfig._

  def enableNativeRead: Boolean = getConf(ENABLE_NATIVE_READ)

  def enableNativeWrite: Boolean = getConf(ENABLE_NATIVE_WRITE)

  def enableNativeEqualityDelete: Boolean = getConf(ENABLE_NATIVE_EQUALITY_DELETE)
}

object GlutenIcebergConfig extends ConfigRegistry {

  def get: GlutenIcebergConfig = {
    new GlutenIcebergConfig(SQLConf.get)
  }

  val ENABLE_NATIVE_READ: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.iceberg.enableNativeRead")
      .doc("Enable offloading Iceberg scans to the native backend. When disabled, Iceberg scans" +
        " fall back to vanilla Spark while scans of other formats stay offloaded.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_NATIVE_WRITE: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.iceberg.enableNativeWrite")
      .doc("Enable offloading Iceberg writes to the native backend. When disabled, Iceberg" +
        " writes fall back to vanilla Spark. Native data writes in the Velox backend additionally" +
        " require spark.gluten.sql.enable.enhancedFeatures to be enabled.")
      .booleanConf
      .createWithDefault(true)

  val ENABLE_NATIVE_EQUALITY_DELETE: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.iceberg.enableNativeEqualityDelete")
      .doc("Write equality-delete files for supported merge-on-read SQL DELETE, UPDATE and MERGE." +
        " Requires native Iceberg writes to be enabled. Equality-delete files use the native" +
        " Parquet writer. Unsupported encodings retain Iceberg's normal row-level write plan.")
      .booleanConf
      .createWithDefault(false)
}
