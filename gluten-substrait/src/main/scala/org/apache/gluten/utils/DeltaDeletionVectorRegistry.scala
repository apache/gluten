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
package org.apache.gluten.utils

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

object DeltaDeletionVectorRegistry {
  val RegistryIdProperty = "gluten.delta.dv.registry.id"

  final case class Entry(cardinality: Long, filterType: String, payload: Array[Byte])
    extends Serializable

  private val registry = new ConcurrentHashMap[String, Map[String, Entry]]()

  def register(entries: Map[String, Entry]): String = {
    val id = UUID.randomUUID().toString
    registry.put(id, entries)
    id
  }

  def get(id: String): Option[Map[String, Entry]] = Option(registry.get(id))

  def normalizePathKey(path: String): String = {
    path.replace('\\', '/').stripSuffix("/")
  }
}
