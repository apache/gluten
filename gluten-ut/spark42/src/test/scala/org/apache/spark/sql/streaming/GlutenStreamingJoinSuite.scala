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
package org.apache.spark.sql.streaming

import org.apache.spark.sql.GlutenStreamingVanillaFallbackTestsTrait

// Spark 4.2 made `testMode` abstract on StreamingJoinSuite and split each suite into
// virtual-column-family (VCF) and non-VCF variants; these mirror the upstream split.

class GlutenStreamingInnerWithVCFSuite
  extends StreamingInnerJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithVCF
}

class GlutenStreamingInnerWithoutVCFSuite
  extends StreamingInnerJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithoutVCF
}

class GlutenStreamingOuterWithVCFSuite
  extends StreamingOuterJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithVCF
}

class GlutenStreamingOuterWithoutVCFSuite
  extends StreamingOuterJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithoutVCF
}

class GlutenStreamingFullOuterWithVCFSuite
  extends StreamingFullOuterJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithVCF
}

class GlutenStreamingFullOuterWithoutVCFSuite
  extends StreamingFullOuterJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithoutVCF
}

class GlutenStreamingLeftSemiWithVCFSuite
  extends StreamingLeftSemiJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithVCF
}

class GlutenStreamingLeftSemiWithoutVCFSuite
  extends StreamingLeftSemiJoinSuite
  with GlutenStreamingVanillaFallbackTestsTrait {
  override protected def testMode = Mode.WithoutVCF
}
