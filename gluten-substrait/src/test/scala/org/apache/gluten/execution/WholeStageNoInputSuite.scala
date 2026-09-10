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
package org.apache.gluten.execution

import org.mockito.Mockito.mock
import org.scalatest.funsuite.AnyFunSuite

class WholeStageNoInputSuite extends AnyFunSuite {
  test("explicit no-input execution creates one dependency-free partition") {
    val wrapper = new ColumnarInputRDDsWrapper(Seq.empty, supportsNoInputExecution = true)

    assert(wrapper.getPartitionLength == 1)
    assert(wrapper.getDependencies.isEmpty)
    assert(wrapper.getPartitions(0).isEmpty)
    assert(wrapper.getIterators(Seq.empty, null).isEmpty)
  }

  test("an accidental empty input stage still fails") {
    val wrapper = new ColumnarInputRDDsWrapper(Seq.empty)

    val error = intercept[IllegalStateException] {
      wrapper.getPartitionLength
    }
    assert(error.getMessage.contains("No non-broadcast input RDD is available"))
  }

  test("an explicitly supported broadcast-only input creates one partition") {
    val broadcast = mock(classOf[BroadcastBuildSideRDD])
    val wrapper =
      new ColumnarInputRDDsWrapper(Seq(broadcast), supportsNoInputExecution = true)

    assert(wrapper.getDependencies.isEmpty)
    assert(wrapper.getPartitions(0).isEmpty)
    assert(wrapper.getPartitionLength == 1)
  }

  test("an unsupported broadcast-only input still fails") {
    val broadcast = mock(classOf[BroadcastBuildSideRDD])
    val wrapper = new ColumnarInputRDDsWrapper(Seq(broadcast))

    intercept[IllegalStateException] {
      wrapper.getPartitionLength
    }
  }

  test("concurrent partition discovery still produces one partition per request") {
    val wrapper = new ColumnarInputRDDsWrapper(Seq.empty, supportsNoInputExecution = true)
    val partitionLengths = Array.fill(32)(0)
    val threads = partitionLengths.indices.map {
      index => new Thread(() => partitionLengths(index) = wrapper.getPartitionLength)
    }
    threads.foreach(_.start())
    threads.foreach(_.join())

    assert(partitionLengths.forall(_ == 1))
  }
}
