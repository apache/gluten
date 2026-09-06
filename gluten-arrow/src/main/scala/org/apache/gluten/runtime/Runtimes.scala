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
package org.apache.gluten.runtime

import org.apache.spark.task.TaskResources

import java.security.MessageDigest
import java.util

object Runtimes {

  /**
   * Produce a stable, value-free cache key for a (backendName, name, extraConf) triple.
   *
   * Two problems with the old `s"$backendName:$name:$extraConf"` key:
   *
   *   1. **Credential leakage** – `Map.toString` embeds secret values (e.g. `fs.s3a.secret.key`,
   *      `fs.azure.account.oauth2.client.secret`) in a plain heap string that can appear in logs,
   *      thread dumps, and heap snapshots.
   *   2. **Nondeterminism** – Scala `Map.toString` does not guarantee insertion order, so two maps
   *      with identical entries can produce different strings, causing spurious duplicate
   *      `VeloxRuntime` registrations within a task.
   *
   * Fix: sort keys, hash them with SHA-256, and use only the hex digest as the key. Values are
   * intentionally excluded from the digest – distinct configs (different credentials for the same
   * key set) that need separate runtimes are already separated at the task level through
   * `GlutenPartition.fsConf`. Within a single task the key set is stable, so the digest is stable.
   */
  private def stableKey(
      backendName: String,
      name: String,
      extraConf: util.Map[String, String]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(backendName.getBytes("UTF-8"))
    digest.update(0.toByte) // field separator
    digest.update(name.getBytes("UTF-8"))
    digest.update(0.toByte)
    // Sort keys for determinism; hash only keys, not values, to avoid leaking secrets.
    val sortedKeys = new java.util.ArrayList(extraConf.keySet)
    java.util.Collections.sort(sortedKeys)
    sortedKeys.forEach {
      k =>
        digest.update(k.getBytes("UTF-8"))
        digest.update(0.toByte)
    }
    digest.digest().map("%02x".format(_)).mkString
  }

  def contextInstance(
      backendName: String,
      name: String,
      extraConf: util.Map[String, String]): Runtime = {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.")
    }
    TaskResources.addResourceIfNotRegistered(
      stableKey(backendName, name, extraConf),
      () => Runtime(backendName, name, extraConf))
  }

  /** Get or create the runtime which bound with Spark TaskContext. */
  def contextInstance(backendName: String, name: String): Runtime = {
    contextInstance(backendName, name, new util.HashMap[String, String]())
  }

}
