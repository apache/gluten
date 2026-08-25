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
package org.apache.gluten

import org.apache.gluten.component.Component
import org.apache.gluten.extension.GlutenSessionExtensions
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS

import org.scalatest.funsuite.AnyFunSuite

class GlutenSessionExtensionsSuite extends AnyFunSuite {
  test("component session extensions are appended once") {
    val configuredExtension = "example.ConfiguredExtension"
    val componentExtension = "example.ComponentExtension"
    val conf = new SparkConf(false).set(
      SPARK_SESSION_EXTENSIONS.key,
      Seq(configuredExtension, componentExtension).mkString(","))
    val component = new TestComponent(Seq(componentExtension, componentExtension))

    GlutenDriverPlugin.configureSessionExtensions(conf, component :: Nil)

    assert(
      conf.get(SPARK_SESSION_EXTENSIONS.key).split(",").toSeq == Seq(
        configuredExtension,
        componentExtension,
        GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME))
  }

  private class TestComponent(extensions: Seq[String]) extends Component {
    override def name(): String = "test"
    override def dependencies(): Seq[Class[_ <: Component]] = Nil
    override def sparkSessionExtensions(): Seq[String] = extensions
    override def injectRules(injector: Injector): Unit = {}
  }
}
