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
package org.apache.spark.sql.execution.unsafe

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class UnsafeColumnarBuildSideRelationTest extends SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "200M")
      .set("spark.memory.offHeap.enabled", "true")
  }

  var unsafeRelWithIdentityMode: UnsafeColumnarBuildSideRelation = _
  var unsafeRelWithHashMode: UnsafeColumnarBuildSideRelation = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val a = AttributeReference("a", StringType, nullable = false, null)()
    val output = Seq(a)
    val totalArraySize = 1
    val perArraySize = new Array[Int](totalArraySize)
    perArraySize(0) = 10
    val bytesArray = UnsafeBytesBufferArray(
      1,
      perArraySize,
      10
    )
    bytesArray.putBytesBuffer(0, "1234567890".getBytes())
    unsafeRelWithIdentityMode = UnsafeColumnarBuildSideRelation(
      output,
      bytesArray,
      IdentityBroadcastMode
    )
    unsafeRelWithHashMode = UnsafeColumnarBuildSideRelation(
      output,
      bytesArray,
      HashedRelationBroadcastMode(output, isNullAware = false)
    )
  }

  test("Java default serialization") {
    val javaSerialization = new JavaSerializer(SparkEnv.get.conf)
    val serializerInstance = javaSerialization.newInstance()

    // test unsafeRelWithIdentityMode
    val buffer = serializerInstance.serialize(unsafeRelWithIdentityMode)
    val obj = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer)
    assert(obj != null)
    assert(obj.mode == IdentityBroadcastMode)

    // test unsafeRelWithHashMode
    val buffer2 = serializerInstance.serialize(unsafeRelWithHashMode)
    val obj2 = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer2)
    assert(obj2 != null)
    assert(obj2.mode.isInstanceOf[HashedRelationBroadcastMode])
  }

  test("Kryo serialization") {
    val kryo = new KryoSerializer(SparkEnv.get.conf)
    val serializerInstance = kryo.newInstance()

    // test unsafeRelWithIdentityMode
    val buffer = serializerInstance.serialize(unsafeRelWithIdentityMode)
    val obj = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer)
    assert(obj != null)
    assert(obj.mode == IdentityBroadcastMode)

    // test unsafeRelWithHashMode
    val buffer2 = serializerInstance.serialize(unsafeRelWithHashMode)
    val obj2 = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer2)
    assert(obj2 != null)
    assert(obj2.mode.isInstanceOf[HashedRelationBroadcastMode])
  }

}
