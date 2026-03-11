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
package org.apache.gluten.integration.action

import org.apache.gluten.integration.Suite

import org.apache.hadoop.fs.{FileSystem, Path}

case class DataGenOnly(strategy: DataGenOnly.Strategy) extends Action {

  override def execute(suite: Suite): Boolean = {
    suite.sessionSwitcher.useSession("baseline", "Data Gen")
    val fs = this.fs(suite)
    val markerPath = this.markerPath(suite)

    strategy match {
      case DataGenOnly.Skip =>
        ()

      case DataGenOnly.Once =>
        val dataPath = this.dataPath(suite)
        if (fs.exists(dataPath) && fs.exists(markerPath)) {
          println(s"Test data already generated at $dataPath. Skipping.")
        } else {
          if (fs.exists(dataPath)) {
            println(
              s"Test data exists at $dataPath but no completion marker found. Regenerating."
            )
            fs.delete(dataPath, true)
          }
          if (fs.exists(markerPath)) {
            fs.delete(markerPath, true)
          }
          gen(suite)
          // Create marker after successful generation.
          fs.create(markerPath, false).close()
        }

      case DataGenOnly.Always =>
        gen(suite)
        // Create marker after successful generation.
        fs.create(markerPath, false).close()
    }
    true
  }

  private def fs(suite: Suite): FileSystem = {
    val configuration = suite.sessionSwitcher.spark().sessionState.newHadoopConf()
    dataPath(suite).getFileSystem(configuration)
  }

  private def markerPath(suite: Suite): Path =
    new Path(suite.dataWritePath() + ".completed")

  private def dataPath(suite: Suite): Path =
    new Path(suite.dataWritePath())

  private def gen(suite: Suite): Unit = {
    val dataPath = suite.dataWritePath()

    println(s"Generating test data to $dataPath...")

    val dataGen = suite.createDataGen()
    dataGen.gen(suite.sessionSwitcher.spark())

    println(s"All test data successfully generated at $dataPath.")
  }
}

object DataGenOnly {
  sealed trait Strategy
  case object Skip extends Strategy
  case object Once extends Strategy
  case object Always extends Strategy
}
