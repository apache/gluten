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

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.util.SparkReflectionUtil

/** Spark's VariantType only exists in Spark 4.0+, so it is matched by class name. */
object VariantTypeUtil {
  private val VariantTypeClassName = "org.apache.spark.sql.types.VariantType$"

  def isVariantType(dt: DataType): Boolean = dt.getClass.getName == VariantTypeClassName

  /** Spark's VariantType. Throws on Spark versions before 4.0. */
  lazy val variantType: DataType =
    SparkReflectionUtil
      .classForName(VariantTypeClassName)
      .getField("MODULE$")
      .get(null)
      .asInstanceOf[DataType]

  def containsVariantType(dt: DataType): Boolean = dt match {
    case _ if isVariantType(dt) => true
    case s: StructType => s.fields.exists(f => containsVariantType(f.dataType))
    case a: ArrayType => containsVariantType(a.elementType)
    case m: MapType => containsVariantType(m.keyType) || containsVariantType(m.valueType)
    case _ => false
  }
}
