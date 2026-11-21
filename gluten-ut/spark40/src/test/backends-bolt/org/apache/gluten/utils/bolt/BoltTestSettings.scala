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
package org.apache.gluten.utils.bolt

// Suites added or renamed in this Spark version, in addition to the shared regression inventory.
class BoltTestSettings extends BoltSpark4TestSettings {
  enableSuite[org.apache.spark.sql.GlutenDefaultANSIValueSuite]
  enableSuite[org.apache.spark.sql.catalyst.expressions.GlutenCollationRegexpExpressionsSuite]
  enableSuite[org.apache.spark.sql.execution.joins.GlutenHashedRelationSuite]
  enableSuite[org.apache.spark.sql.execution.python.GlutenRowQueueSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingFullOuterJoinSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingInnerJoinSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingLeftSemiJoinSuite]
  enableSuite[org.apache.spark.sql.streaming.GlutenStreamingOuterJoinSuite]
}
