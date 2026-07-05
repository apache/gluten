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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.arrow.vector.types.pojo.Schema

import java.util.{Objects, TimeZone}

object SparkSchemaUtil {

  def fromArrowSchema(schema: Schema): StructType = {
    SparkArrowUtil.fromArrowSchema(schema)
  }

  def toArrowSchema(schema: StructType): Schema = {
    SparkArrowUtil.toArrowSchema(schema, getLocalTimezoneID, enableLargeVarTypes)
  }

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    SparkArrowUtil.toArrowSchema(schema, timeZoneId, enableLargeVarTypes)
  }

  def isTimeZoneIDEquivalentToUTC(zoneId: String): Boolean = {
    getTimeZoneIDOffset(zoneId) == 0
  }

  def getLocalTimezoneID: String = {
    SQLConf.get.sessionLocalTimeZone
  }

  def enableLargeVarTypes: Boolean = {
    SQLConf.get.getConfString("spark.sql.execution.arrow.useLargeVarTypes", "false").toBoolean
  }

  def timeZoneIDEquals(one: String, other: String): Boolean = {
    getTimeZoneIDOffset(one) == getTimeZoneIDOffset(other)
  }

  def getTimeZoneIDOffset(zoneId: String): Int = {
    Objects.requireNonNull(zoneId)
    TimeZone
      .getTimeZone(zoneId)
      .toZoneId
      .getRules
      .getOffset(java.time.Instant.now())
      .getTotalSeconds
  }
}
