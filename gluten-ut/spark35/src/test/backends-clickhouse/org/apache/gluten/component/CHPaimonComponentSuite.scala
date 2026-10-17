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

import org.apache.gluten.backendsapi.clickhouse.CHConfig

import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuite

class CHPaimonComponentSuite extends AnyFunSuite {
  test("native Paimon primary-key read is disabled by default and configurable") {
    val conf = new SQLConf
    assert(!new CHConfig(conf).paimonNativeReadEnabled)
    assert(!new CHConfig(conf).paimonNativePkReadEnabled)
    conf.setConfString(CHConfig.PAIMON_NATIVE_READ_ENABLED.key, "true")
    conf.setConfString(CHConfig.PAIMON_NATIVE_PK_READ_ENABLED.key, "true")
    assert(new CHConfig(conf).paimonNativeReadEnabled)
    assert(new CHConfig(conf).paimonNativePkReadEnabled)
  }

  test("Paimon component is registered when the Paimon profile is enabled") {
    val loader = Thread.currentThread().getContextClassLoader
    val paimonExtension = "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
    assume(scala.util.Try(loader.loadClass(paimonExtension)).isSuccess)

    val component = "org.apache.gluten.component.CHPaimonComponent"
    assert(loader.getResource(s"META-INF/gluten-components/$component") != null)
    assert(loader.loadClass(component).getDeclaredConstructor().newInstance()
      .asInstanceOf[Component].dependencies().contains(
        classOf[org.apache.gluten.backendsapi.clickhouse.CHBackend]))
  }
}
