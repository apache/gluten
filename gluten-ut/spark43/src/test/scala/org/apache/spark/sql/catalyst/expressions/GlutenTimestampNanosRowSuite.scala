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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenExpressionOffloadTracker
import org.apache.spark.sql.shim.GlutenTestsTrait
import org.apache.spark.sql.types.{TimestampLTZNanosType, TimestampNTZNanosType}
import org.apache.spark.unsafe.types.TimestampNanosVal

class GlutenTimestampNanosRowSuite
  extends TimestampNanosRowSuite
  with GlutenExpressionOffloadTracker
  with GlutenTestsTrait {

  // collect() converts through the external type, which truncates to the type's precision, so
  // the LTZ(7) value must already be at that precision. Spark's test uses 999 nanos.
  testGluten("checkEvaluation roundtrip for nanos timestamp Literal") {
    val ntzValue = TimestampNanosVal.fromParts(1234567890123L, 42.toShort)
    val ltzValue = TimestampNanosVal.fromParts(9876543210987L, 900.toShort)
    checkEvaluation(Literal.create(ntzValue, TimestampNTZNanosType(9)), ntzValue)
    checkEvaluation(Literal.create(ltzValue, TimestampLTZNanosType(7)), ltzValue)
  }
}
