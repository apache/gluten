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
package org.apache.gluten.execution

/** One step of a [[SubfieldPath]] below its column. */
sealed trait SubfieldElement

object SubfieldElement {

  /** A struct field, by the schema's name. */
  case class Field(name: String) extends SubfieldElement

  /** A map lookup by a string key. */
  case class StringKey(key: String) extends SubfieldElement

  /** A map lookup by an integral key. */
  case class LongKey(key: Long) extends SubfieldElement
}

/**
 * A path from a scan output column into its value, e.g. column `m` with elements [StringKey("a"),
 * Field("t")] for `m['a'].t`. Declared on a scan as a required subfield so the native reader keeps
 * only the map entries such paths name. Transported structurally (see
 * RequiredSubfieldsExtension.proto); toString renders the Velox Subfield syntax for logs and tests.
 */
case class SubfieldPath(column: String, elements: Seq[SubfieldElement]) {
  override def toString: String = {
    val sb = new StringBuilder(column)
    elements.foreach {
      case SubfieldElement.Field(name) => sb.append('.').append(name)
      case SubfieldElement.StringKey(key) =>
        sb.append("[\"").append(key.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"]")
      case SubfieldElement.LongKey(key) => sb.append('[').append(key).append(']')
    }
    sb.toString
  }
}
