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
package org.apache.gluten.integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable

trait DataGen {
  def gen(spark: SparkSession): Unit
}

abstract class TypeModifier(val predicate: DataType => Boolean, val to: DataType)
  extends Serializable {
  def modValue(value: Any): Any
}

class NoopModifier(t: DataType) extends TypeModifier(_ => true, t) {
  override def modValue(value: Any): Any = value
}

object DataGen {
  def getRowModifier(schema: StructType, typeModifiers: Seq[TypeModifier]): Int => TypeModifier = {
    val modifiers = schema.fields.map {
      f =>
        val matchedModifiers = typeModifiers.flatMap {
          m =>
            if (m.predicate.apply(f.dataType)) {
              Some(m)
            } else {
              None
            }
        }
        if (matchedModifiers.isEmpty) {
          new NoopModifier(f.dataType)
        } else {
          if (matchedModifiers.size > 1) {
            println(
              s"More than one type modifiers specified for type ${f.dataType}, " +
                s"use first one in the list")
          }
          matchedModifiers.head // use the first one that matches
        }
    }
    i => modifiers(i)
  }

  def modifySchema(schema: StructType, rowModifier: Int => TypeModifier): StructType = {
    val modifiedSchema = new StructType(schema.fields.zipWithIndex.map {
      case (f, i) =>
        val modifier = rowModifier.apply(i)
        StructField(f.name, modifier.to, f.nullable, f.metadata)
    })
    modifiedSchema
  }

  trait Feature extends Serializable {
    def name(): String
    def run(spark: SparkSession, source: String): Unit
  }

  object Feature {
    def run(spark: SparkSession, source: String, feature: Feature): Unit = {
      println(s"Executing feature: ${feature.name()}")
      val start = System.nanoTime()
      feature.run(spark, source)
      val end = System.nanoTime()
      println(
        s"Finished executing feature: ${feature.name()}, elapsed time: ${(end - start) / 1e6} ms.")
    }
  }

  class FeatureRegistry extends Serializable {
    private val lookup: mutable.LinkedHashMap[String, Feature] = mutable.LinkedHashMap()

    def register(feature: Feature): Unit = {
      require(feature.name().matches("^\\w+$"))
      require(!lookup.contains(feature.name()))
      lookup(feature.name()) = feature
    }

    def getFeature(name: String): Feature = {
      require(
        lookup.contains(name),
        s"No feature found by name: $name, available features: ${lookup.keys.mkString(", ")}")
      lookup(name)
    }
  }
}
