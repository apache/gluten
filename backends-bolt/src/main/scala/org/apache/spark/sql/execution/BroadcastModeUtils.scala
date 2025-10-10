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
package org.apache.spark.sql.execution

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, IdentityBroadcastMode}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}

/**
 * Provides serialization-safe representations of BroadcastMode to avoid issues with circular
 * references in complex expression trees during Kryo serialization.
 */
sealed trait SafeBroadcastMode extends Serializable

/** Safe representation of IdentityBroadcastMode */
case object IdentitySafeBroadcastMode extends SafeBroadcastMode

/**
 * Safe wrapper for HashedRelationBroadcastMode. Stores only column ordinals instead of full
 * BoundReference expressions.
 */
final case class HashSafeBroadcastMode(ordinals: Array[Int], isNullAware: Boolean)
  extends SafeBroadcastMode

/**
 * Safe wrapper for HashedRelationBroadcastMode when keys are not simple BoundReferences. Stores key
 * expressions as serialized Java bytes.
 */
final case class HashExprSafeBroadcastMode(exprBytes: Array[Byte], isNullAware: Boolean)
  extends SafeBroadcastMode

object BroadcastModeUtils extends Logging {

  /**
   * Converts a BroadcastMode to its SafeBroadcastMode equivalent. Uses ordinals for simple
   * BoundReferences, otherwise serializes the expressions.
   */
  private[execution] def toSafe(mode: BroadcastMode): SafeBroadcastMode = mode match {
    case IdentityBroadcastMode =>
      IdentitySafeBroadcastMode
    case HashedRelationBroadcastMode(keys, isNullAware) =>
      // Fast path: all keys are already BoundReference(i, ..,..).
      val ords = keys.collect { case BoundReference(ord, _, _) => ord }
      if (ords.size == keys.size) {
        HashSafeBroadcastMode(ords.toArray, isNullAware)
      } else {
        // Fallback: store the key expressions as Java-serialized bytes.
        HashExprSafeBroadcastMode(serializeExpressions(keys), isNullAware)
      }

    case other =>
      throw new IllegalArgumentException(s"Unsupported BroadcastMode: $other")
  }

  /** Converts a SafeBroadcastMode to its BroadcastMode equivalent. */
  private[execution] def fromSafe(safe: SafeBroadcastMode, output: Seq[Attribute]): BroadcastMode =
    safe match {
      case IdentitySafeBroadcastMode =>
        IdentityBroadcastMode

      case HashSafeBroadcastMode(ords, isNullAware) =>
        val bound = ords.map(i => BoundReference(i, output(i).dataType, output(i).nullable)).toSeq
        HashedRelationBroadcastMode(bound, isNullAware)

      case HashExprSafeBroadcastMode(bytes, isNullAware) =>
        HashedRelationBroadcastMode(deserializeExpressions(bytes), isNullAware)
    }

  // Helpers for expression serialization (used in HashExprSafeBroadcastMode)
  private[execution] def serializeExpressions(keys: Seq[Expression]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    var oos: ObjectOutputStream = null
    try {
      oos = new ObjectOutputStream(bos)
      oos.writeObject(keys)
      oos.flush()
      bos.toByteArray
    } catch {
      case e @ (_: IOException | _: ClassNotFoundException | _: ClassCastException) =>
        logError(
          s"Failed to serialize expressions for BroadcastMode. Expression count: ${keys.length}",
          e)
        throw new RuntimeException("Failed to serialize expressions for BroadcastMode", e)
      case e: Exception =>
        logError(
          s"Unexpected error during expression serialization. Expression count: ${keys.length}",
          e)
        throw e
    } finally {
      if (oos != null) oos.close()
      bos.close()
    }
  }

  private[execution] def deserializeExpressions(bytes: Array[Byte]): Seq[Expression] = {
    val bis = new ByteArrayInputStream(bytes)
    var ois: ObjectInputStream = null
    try {
      ois = new ObjectInputStream(bis)
      ois.readObject().asInstanceOf[Seq[Expression]]
    } catch {
      case e @ (_: IOException | _: ClassNotFoundException | _: ClassCastException) =>
        logError(
          s"Failed to deserialize expressions for BroadcastMode. Data size: ${bytes.length} bytes",
          e)
        throw new RuntimeException("Failed to deserialize expressions for BroadcastMode", e)
      case e: Exception =>
        logError(
          s"Unexpected error during expression deserialization. Data size: ${bytes.length} bytes",
          e)
        throw e
    } finally {
      if (ois != null) ois.close()
      bis.close()
    }
  }
}
