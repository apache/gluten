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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Ensures compatibility with Spark 4.0. Since Spark 4.0, the Column class is private to the package
 * org.apache.spark. This class provides a way to construct a Column from an Expression in code that
 * is outside the org.apache.spark package by delegating to ExpressionUtils.column().
 */
object ClassicColumn {
  def apply(e: Expression): Column = {
    ExpressionUtils.column(e)
  }
}
