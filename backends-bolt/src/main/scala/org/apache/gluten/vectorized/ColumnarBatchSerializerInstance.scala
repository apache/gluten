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
package org.apache.gluten.vectorized

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.storage.BlockId

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

abstract class ColumnarBatchSerializerInstance extends SerializerInstance {

  /** Deserialize the streams of ColumnarBatches. */
  def deserializeStreams(streams: Iterator[(BlockId, InputStream)]): DeserializationStream

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    throw new UnsupportedOperationException
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    throw new UnsupportedOperationException
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    throw new UnsupportedOperationException
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    throw new UnsupportedOperationException
  }
}
