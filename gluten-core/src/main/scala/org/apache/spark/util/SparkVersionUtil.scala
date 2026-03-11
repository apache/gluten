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

object SparkVersionUtil {
  val lteSpark32: Boolean = compareMajorMinorVersion((3, 2)) <= 0
  private val comparedWithSpark33 = compareMajorMinorVersion((3, 3))
  private val comparedWithSpark35 = compareMajorMinorVersion((3, 5))
  val eqSpark33: Boolean = comparedWithSpark33 == 0
  val lteSpark33: Boolean = lteSpark32 || eqSpark33
  val gteSpark33: Boolean = comparedWithSpark33 >= 0
  val gteSpark35: Boolean = comparedWithSpark35 >= 0
  val gteSpark40: Boolean = compareMajorMinorVersion((4, 0)) >= 0
  val gteSpark41: Boolean = compareMajorMinorVersion((4, 1)) >= 0

  // Returns X. X < 0 if one < other, x == 0 if one == other, x > 0 if one > other.
  def compareMajorMinorVersion(other: (Int, Int)): Int = {
    val (major, minor) = VersionUtils.majorMinorVersion(org.apache.spark.SPARK_VERSION)
    if (major == other._1) {
      minor - other._2
    } else {
      major - other._1
    }
  }
}
