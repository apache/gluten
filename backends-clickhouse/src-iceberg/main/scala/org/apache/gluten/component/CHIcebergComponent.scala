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
package org.apache.gluten.component

import org.apache.gluten.backendsapi.clickhouse.CHBackend
import org.apache.gluten.extension.OffloadIcebergScan
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.util.SparkReflectionUtil

class CHIcebergComponent extends Component {
  override def name(): String = "clickhouse-iceberg"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[CHBackend] :: Nil

  override def isRuntimeCompatible: Boolean = {
    SparkReflectionUtil.isClassPresent(
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }

  override def injectRules(injector: Injector): Unit = {
    OffloadIcebergScan.inject(injector)
  }
}
