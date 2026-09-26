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
package org.apache.spark.util

import org.apache.gluten.exception.GlutenException

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import org.apache.commons.io.FileUtils

import java.io.{File, IOException}
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

/**
 * Manages Gluten's local directories, for storing jars, libs, spill files, or other temporary
 * stuffs.
 */
class SparkDirectoryUtil private (val roots: Array[String]) extends Logging {
  private val ROOTS: Array[File] = roots.flatMap {
    rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "gluten")
        SparkShutdownManagerUtil.addHookForTempDirRemoval(
          () => {
            try FileUtils.forceDelete(localDir)
            catch {
              case e: Exception =>
                throw new GlutenException(e)
            }
          })
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create Gluten local dir in $rootDir. Ignoring this directory.", e)
          None
      }
  }

  private val NAMESPACE_MAPPING: java.util.Map[String, Namespace] =
    new ConcurrentHashMap[String, Namespace]

  def namespace(name: String): Namespace = {
    if (ROOTS.isEmpty) {
      val configured =
        if (roots.isEmpty) "none were configured"
        else roots.map(r => s"'$r'").mkString("[", ", ", "]")
      throw new IllegalStateException(
        s"No available Gluten local directory for namespace '$name': none of the " +
          s"configured local directories could be created ($configured)")
    }
    NAMESPACE_MAPPING.computeIfAbsent(name, (name: String) => new Namespace(ROOTS, name))
  }
}

object SparkDirectoryUtil extends Logging {
  @volatile private var roots: Array[String] = _
  private lazy val INSTANCE: SparkDirectoryUtil = {
    if (this.roots == null) {
      throw new IllegalStateException("SparkDirectoryUtil not initialized")
    }
    new SparkDirectoryUtil(this.roots)
  }

  def init(conf: SparkConf): Unit = {
    val roots = Utils.getConfiguredLocalDirs(conf)
    init(roots)
  }

  private def init(roots: Array[String]): Unit = synchronized {
    if (this.roots == null) {
      this.roots = roots
    } else if (this.roots.toSet != roots.toSet) {
      throw new IllegalArgumentException(
        s"Reinitialize SparkDirectoryUtil with different root dirs: old: ${this.roots
            .mkString("Array(", ", ", ")")}, new: ${roots.mkString("Array(", ", ", ")")}"
      )
    }
  }

  def get(): SparkDirectoryUtil = INSTANCE

  // Visible for testing: build an isolated instance without touching the
  // process-wide singleton, so tests do not depend on init() ordering.
  private[util] def createForTesting(roots: Array[String]): SparkDirectoryUtil =
    new SparkDirectoryUtil(roots)
}

class Namespace(private val parents: Array[File], private val name: String) {
  val all = parents.map {
    root =>
      val path = Paths
        .get(root.getAbsolutePath)
        .resolve(name)
      path.toFile
  }

  if (all.isEmpty) {
    // all.isEmpty iff parents.isEmpty (map preserves length), so the actionable
    // detail here is that no parent directories were provided.
    throw new IllegalStateException(
      s"No available Gluten local directory in namespace '$name': " +
        s"no parent directories were provided")
  }

  private var nextRootIndex = 0

  def mkChildDirRoundRobin(childDirName: String): File = synchronized {
    // Round-robin across the parent roots by index. The constructor rejects an
    // empty parent list, so `all` is always non-empty here.
    val subDir = all(nextRootIndex)
    nextRootIndex = (nextRootIndex + 1) % all.length
    val path = Paths
      .get(subDir.getAbsolutePath)
      .resolve(childDirName)
    val file = path.toFile
    FileUtils.forceMkdir(file)
    file
  }

  def mkChildDirRandomly(childDirName: String): File = {
    val selected = all(scala.util.Random.nextInt(all.length))
    val path = Paths
      .get(selected.getAbsolutePath)
      .resolve(childDirName)
    val file = path.toFile
    FileUtils.forceMkdir(file)
    file
  }

  def mkChildDirs(childDirName: String): Array[File] = {
    all.map {
      subDir =>
        val path = Paths
          .get(subDir.getAbsolutePath)
          .resolve(childDirName)
        val file = path.toFile
        FileUtils.forceMkdir(file)
        file
    }
  }
}
