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

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.memory.memtarget.ThrowOnOomMemoryTarget.OutOfMemoryException

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.memory.GlobalOffHeapMemory
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.Platform

import java.util

import scala.collection.mutable
import scala.util.Random

class UnsafeColumnarBuildSideRelationTest extends SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "200M")
      .set("spark.memory.offHeap.enabled", "true")
  }

  var unsafeRelWithIdentityMode: UnsafeColumnarBuildSideRelation = _
  var unsafeRelWithHashMode: UnsafeColumnarBuildSideRelation = _
  var output: Seq[Attribute] = _
  var sample1KBytes: Array[Byte] = _
  var initialGlobalBytes: Long = _

  private def toByteArray(unsafeByteArray: UnsafeByteArray): Array[Byte] = {
    val byteArray = new Array[Byte](Math.toIntExact(unsafeByteArray.size()))
    Platform.copyMemory(
      null,
      unsafeByteArray.address(),
      byteArray,
      Platform.BYTE_ARRAY_OFFSET,
      byteArray.length)
    byteArray
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Trigger GC to clean up any residual memory from previous test suites,
    // ensuring initialGlobalBytes is accurate.
    System.gc()
    Thread.sleep(1000)
    initialGlobalBytes = GlobalOffHeapMemory.currentBytes()
    output = Seq(AttributeReference("a", StringType, nullable = false, null)())
    sample1KBytes = randomBytes(1024)
    unsafeRelWithIdentityMode = newUnsafeRelationWithIdentityMode(2)
    unsafeRelWithHashMode = newUnsafeRelationWithHashMode(2)
  }

  override protected def afterAll(): Unit = {
    // Makes sure all the underlying UnsafeByteArray instances become GC non-reachable and
    // be released after a full-GC.
    unsafeRelWithIdentityMode = null
    unsafeRelWithHashMode = null
    System.gc()
    Thread.sleep(1000)
    // Since we trigger GC in beforeAll() to clean up residual memory from previous test suites,
    // initialGlobalBytes should be accurate and this assertion should be stable.
    assert(GlobalOffHeapMemory.currentBytes() == initialGlobalBytes)
  }

  private def randomBytes(size: Int): Array[Byte] = {
    val array = new Array[Byte](size)
    val random = new Random()
    random.nextBytes(array)
    array
  }

  private def sampleUnsafeByteArrayInKb(sizeInKb: Int): UnsafeByteArray = {
    val sizeInBytes = sizeInKb * 1024
    val buf = ArrowBufferAllocators.globalInstance().buffer(sizeInBytes)
    for (i <- 0 until sizeInKb) {
      buf.setBytes(i * 1024, sample1KBytes, 0, 1024)
    }
    try {
      new UnsafeByteArray(buf, sizeInBytes)
    } finally {
      buf.close()
    }
  }

  private def newUnsafeRelationWithIdentityMode(sizeInKb: Int): UnsafeColumnarBuildSideRelation = {
    UnsafeColumnarBuildSideRelation(
      output,
      (0 until sizeInKb).map(_ => sampleUnsafeByteArrayInKb(1)),
      IdentityBroadcastMode
    )
  }

  private def newUnsafeRelationWithHashMode(sizeInKb: Int): UnsafeColumnarBuildSideRelation = {
    UnsafeColumnarBuildSideRelation(
      output,
      (0 until sizeInKb).map(_ => sampleUnsafeByteArrayInKb(1)),
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
    assert(
      util.Arrays.deepEquals(
        obj.getBatches().map(toByteArray).toArray[AnyRef],
        Array(sample1KBytes, sample1KBytes).asInstanceOf[Array[AnyRef]]))

    // test unsafeRelWithHashMode
    val buffer2 = serializerInstance.serialize(unsafeRelWithHashMode)
    val obj2 = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer2)
    assert(obj2 != null)
    assert(obj2.mode.isInstanceOf[HashedRelationBroadcastMode])
    assert(
      util.Arrays.deepEquals(
        obj2.getBatches().map(toByteArray).toArray[AnyRef],
        Array(sample1KBytes, sample1KBytes).asInstanceOf[Array[AnyRef]]))
  }

  test("Kryo serialization") {
    val kryo = new KryoSerializer(SparkEnv.get.conf)
    val serializerInstance = kryo.newInstance()

    // test unsafeRelWithIdentityMode
    val buffer = serializerInstance.serialize(unsafeRelWithIdentityMode)
    val obj = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer)
    assert(obj != null)
    assert(obj.mode == IdentityBroadcastMode)
    assert(
      util.Arrays.deepEquals(
        obj.getBatches().map(toByteArray).toArray[AnyRef],
        Array(sample1KBytes, sample1KBytes).asInstanceOf[Array[AnyRef]]))

    // test unsafeRelWithHashMode
    val buffer2 = serializerInstance.serialize(unsafeRelWithHashMode)
    val obj2 = serializerInstance.deserialize[UnsafeColumnarBuildSideRelation](buffer2)
    assert(obj2 != null)
    assert(obj2.mode.isInstanceOf[HashedRelationBroadcastMode])
    assert(
      util.Arrays.deepEquals(
        obj2.getBatches().map(toByteArray).toArray[AnyRef],
        Array(sample1KBytes, sample1KBytes).asInstanceOf[Array[AnyRef]]))
  }

  test("Should throw OOM when off-heap memory is running out") {
    // 500 MiB > 200 MiB so OOM should be thrown.
    val relations = mutable.ListBuffer[UnsafeColumnarBuildSideRelation]()
    assertThrows[OutOfMemoryException] {
      for (i <- 0 until 10) {
        relations += newUnsafeRelationWithHashMode(ByteUnit.MiB.toKiB(50).toInt)
      }
    }
    relations.clear()
  }

  test("Should trigger GC before OOM") {
    // 500 MiB > 200 MiB, but since we don't preserve the references to the created relations,
    // GC will be triggered and OOM should not be thrown.
    for (i <- 0 until 10) {
      newUnsafeRelationWithHashMode(ByteUnit.MiB.toKiB(50).toInt)
    }
  }

  test("Verify offload field serialization") {
    val relation = UnsafeColumnarBuildSideRelation(
      output,
      Seq(sampleUnsafeByteArrayInKb(1)),
      IdentityBroadcastMode,
      Seq.empty,
      offload = true
    )

    // Java Serialization
    val javaSerializer = new JavaSerializer(SparkEnv.get.conf).newInstance()
    val javaBuffer = javaSerializer.serialize(relation)
    val javaObj = javaSerializer.deserialize[UnsafeColumnarBuildSideRelation](javaBuffer)
    assert(javaObj.isOffload, "Java deserialization failed to restore offload=true")

    // Kryo Serialization
    val kryoSerializer = new KryoSerializer(SparkEnv.get.conf).newInstance()
    val kryoBuffer = kryoSerializer.serialize(relation)
    val kryoObj = kryoSerializer.deserialize[UnsafeColumnarBuildSideRelation](kryoBuffer)
    assert(kryoObj.isOffload, "Kryo deserialization failed to restore offload=true")

    // Create another relation with offload=false to compare byte size if possible,
    // but boolean only takes 1 byte, might be hard to distinguish from metadata noise.
    // Instead, trust the assertion above.
  }
}
