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
package org.apache.gluten.expression

/**
 * A mix-in trait mainly for internal-row's implementations to extend, to ensure the code is
 * compatible with Spark 3.x and 4.x at the same time.
 *
 * Provides stub implementations for methods that exist in Spark 4.x but not in Spark 3.x, including
 * getVariant, getGeography, getGeometry and getBinaryView.
 *
 * The return type is `Nothing` on purpose: it conforms to whichever concrete type the running Spark
 * version declares (and these types do not all exist in every version), so a single definition
 * works across the whole 3.x/4.x matrix.
 */
trait SpecializedGettersSparkCompatible {
  def getVariant(ordinal: Int): Nothing = {
    throw new UnsupportedOperationException()
  }

  def getGeography(ordinal: Int): Nothing =
    throw new UnsupportedOperationException()

  def getGeometry(ordinal: Int): Nothing =
    throw new UnsupportedOperationException()

  // Added by SpecializedGetters in Spark 4.2.
  def getBinaryView(ordinal: Int): Nothing =
    throw new UnsupportedOperationException()
}
