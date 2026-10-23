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

import org.apache.gluten.backendsapi.bolt.BoltSparkPlanExecApi
import org.apache.gluten.config.BoltConfig

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf

import java.net.URI

class GlutenBoltSpillPathSuite extends SparkFunSuite {
  private val api = new BoltSparkPlanExecApi

  private def withFileSystem(system: String)(body: => Unit): Unit = {
    val conf = new SQLConf
    conf.setConfString(BoltConfig.COLUMNAR_BOLT_SPILL_FILE_SYSTEM.key, system)
    SQLConf.withExistingConf(conf)(body)
  }

  test("Bolt local spill paths retain their exact value") {
    withFileSystem("local") {
      Seq("/tmp/gluten/spill", "/tmp/gluten/a b?#%{spill}", "/tmp/gluten/溢写").foreach {
        path => assert(api.rewriteSpillPath(path) == path)
      }
    }
  }

  test("Bolt heap spill URIs preserve literal path characters") {
    withFileSystem("heap-over-local") {
      Seq("/tmp/gluten/spill", "/tmp/gluten/a b?#%{spill}", "/tmp/gluten/溢写").foreach {
        path =>
          val rewritten = new URI(api.rewriteSpillPath(path))
          assert(rewritten.getScheme == "jol")
          assert(rewritten.getAuthority == null)
          assert(rewritten.getQuery == null)
          assert(rewritten.getFragment == null)
          assert(rewritten.getPath == path)
      }
    }
  }
}
