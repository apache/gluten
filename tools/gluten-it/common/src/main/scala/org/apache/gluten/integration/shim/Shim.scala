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
package org.apache.gluten.integration.shim

import org.apache.spark.VersionUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

object Shim {
  def getExpressionEncoder(schema: StructType): ExpressionEncoder[Row] = {
    val sparkVersion = VersionUtils.majorMinorVersion()
    if (VersionUtils.compareMajorMinorVersion(sparkVersion, (3, 5)) < 0) {
      RowEncoder.getClass
        .getMethod("apply", classOf[StructType])
        .invoke(RowEncoder, schema)
        .asInstanceOf[ExpressionEncoder[Row]]
    } else {
      // to be compatible with Spark 3.5 and later
      ExpressionEncoder.getClass
        .getMethod("apply", classOf[StructType])
        .invoke(ExpressionEncoder, schema)
        .asInstanceOf[ExpressionEncoder[Row]]
    }
  }
}
