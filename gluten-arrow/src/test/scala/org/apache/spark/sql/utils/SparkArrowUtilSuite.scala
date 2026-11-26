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
package org.apache.spark.sql.utils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

import org.apache.arrow.vector.types.pojo.{Field, Schema}

import scala.collection.JavaConverters._

class SparkArrowUtilSuite extends SparkFunSuite with SharedSparkSession {
  private val schema = new StructType()
    .add(
      "Info",
      new StructType()
        .add("userId", LongType)
        .add("homeAddr", new StructType().add("zipCode", IntegerType)))
    .add("itemList", ArrayType(new StructType().add("itemId", LongType)))
    .add("itemsByKey", MapType(StringType, new StructType().add("itemCount", LongType)))

  private def names(field: Field): Seq[String] =
    field.getName +: field.getChildren.asScala.toSeq.flatMap(names)

  private def names(schema: Schema): Seq[String] = schema.getFields.asScala.toSeq.flatMap(names)

  private val originalNames = Seq(
    "Info",
    "userId",
    "homeAddr",
    "zipCode",
    "itemList",
    "element",
    "itemId",
    "itemsByKey",
    "entries",
    "key",
    "value",
    "itemCount")

  test("toArrowSchema normalizes nested field names unless asked to keep them") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      // Top-level names are kept. Nested struct field names are lower-cased, as in the Substrait
      // plan.
      assert(
        names(SparkArrowUtil.toArrowSchema(schema, "UTC")) == Seq(
          "Info",
          "userid",
          "homeaddr",
          "zipcode",
          "itemList",
          "element",
          "itemid",
          "itemsByKey",
          "entries",
          "key",
          "value",
          "itemcount"))
      assert(
        names(SparkArrowUtil.toArrowSchema(schema, "UTC", normalizeFieldNames = false)) ==
          originalNames)
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      assert(names(SparkArrowUtil.toArrowSchema(schema, "UTC")) == originalNames)
      assert(
        names(SparkArrowUtil.toArrowSchema(schema, "UTC", normalizeFieldNames = false)) ==
          originalNames)
    }
  }
}
