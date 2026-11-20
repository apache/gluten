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
package org.apache.spark.sql.classic

import org.apache.spark.sql

/**
 * Enables access to the methods in the companion object classic.SparkSession via sql.SparkSession.
 * Since Spark 4.0, these methods have been moved from sql.SparkSession to sql.classic.SparkSession.
 */
object ExtendedClassicConversions {

  implicit class RichSqlSparkSession(sqlSparkSession: sql.SparkSession.type) {
    def cleanupAnyExistingSession(): Unit = {
      // Redirect to the classic.SparkSession companion method.
      sql.classic.SparkSession.cleanupAnyExistingSession()
    }
  }
}
