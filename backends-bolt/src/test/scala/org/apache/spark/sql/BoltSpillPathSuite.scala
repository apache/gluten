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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

class BoltSpillPathSuite extends AnyFunSuite {
  private val api = new BoltSparkPlanExecApi

  private def withFileSystem(system: String)(body: => Unit): Unit = {
    val conf = new SQLConf
    conf.setConfString(BoltConfig.COLUMNAR_BOLT_SPILL_FILE_SYSTEM.key, system)
    SQLConf.withExistingConf(conf)(body)
  }

  test("local spill paths retain their exact value") {
    withFileSystem("local") {
      Seq("/tmp/spill", "/tmp/spark%20disk", "/tmp/a b?#%{spill}", "/tmp/溢写").foreach {
        path => assert(api.rewriteSpillPath(path) == path)
      }
    }
  }

  test("heap-over-local routing preserves the actual spill directory") {
    val root = Files.createTempDirectory("bolt-spill-path")
    try {
      withFileSystem("heap-over-local") {
        Seq("spark%20disk", "spark%2520disk", "a b?#%{spill}", "溢写").foreach {
          name =>
            val directory = Files.createDirectory(root.resolve(name))
            val marker = Files.write(directory.resolve("spill.data"), Array[Byte](1, 2, 3))
            val rewritten = api.rewriteSpillPath(directory.toFile.getAbsolutePath)
            assert(rewritten.startsWith("jol:"))
            // Native FileSystemWrapper strips only the routing prefix, without URI decoding.
            val routed = Paths.get(rewritten.stripPrefix("jol:"))
            assert(Files.isDirectory(routed), s"Spill directory was changed: $rewritten")
            assert(Files.isSameFile(routed.resolve("spill.data"), marker))
        }
      }
    } finally {
      Utils.deleteRecursively(root.toFile)
    }
  }
}
