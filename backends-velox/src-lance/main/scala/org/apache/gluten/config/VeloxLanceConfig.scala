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

class VeloxLanceConfig(conf: SQLConf) extends GlutenCoreConfig(conf) {
  import VeloxLanceConfig._

  def enableNativeWrite: Boolean = getConf(ENABLE_NATIVE_WRITE)
}

object VeloxLanceConfig extends ConfigRegistry {
  def get: VeloxLanceConfig = new VeloxLanceConfig(SQLConf.get)

  val ENABLE_NATIVE_WRITE: ConfigEntry[Boolean] =
    buildConf("spark.gluten.sql.columnar.backend.velox.lance.enableNativeWrite")
      .doc(
        "Experimental. Enable offloading writes to Lance tables to Velox. Writes Velox cannot " +
          "serve fall back to lance-spark's row writer.")
      .booleanConf
      .createWithDefault(false)
}
