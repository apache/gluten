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

import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.util.Collections
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
 * JNI wrapper for Velox native row index finder.
 *
 * This provides native execution for finding row indices matching a filter condition, replacing
 * Spark's fallback implementation for Delta Lake MoR operations.
 */
object VeloxRowIndexFinderJni {
  case class SerializedFilter(expressionBytes: Array[Byte], functionMappings: Array[Array[Byte]])

  /**
   * Create a new row index finder instance.
   *
   * @param memoryPoolHandle
   *   Handle to Velox memory pool (0 for default)
   * @return
   *   Handle to the native finder instance
   */
  @native def nativeCreateFinder(memoryPoolHandle: Long): Long

  /**
   * Create a new row index finder instance with custom configuration.
   *
   * @param memoryPoolHandle
   *   Handle to Velox memory pool (0 for default)
   * @param batchSize
   *   Number of rows to process in a single batch
   * @param numThreads
   *   Number of threads for parallel execution
   * @param memoryLimitMB
   *   Memory limit in megabytes
   * @return
   *   Handle to the native finder instance
   */
  @native def nativeCreateFinderWithConfig(
      memoryPoolHandle: Long,
      batchSize: Int,
      numThreads: Int,
      memoryLimitMB: Int): Long

  /**
   * Find row indices matching the filter condition in a Parquet file.
   *
   * @param finderHandle
   *   Handle to the native finder instance
   * @param filePath
   *   Path to the Parquet file
   * @param filterExpr
   *   Serialized filter expression (Substrait format)
   * @param schema
   *   Serialized schema
   * @return
   *   Array of matching row indices (0-based)
   */
  @native def nativeFindMatchingRows(
      finderHandle: Long,
      filePath: String,
      filterExpr: Array[Byte],
      schema: Array[Byte],
      functionMappings: Array[Array[Byte]]): Array[Long]

  /**
   * Find row indices matching filter conditions in multiple files (batch mode).
   *
   * @param finderHandle
   *   Handle to the native finder instance
   * @param filePaths
   *   Array of file paths
   * @param filterExprs
   *   Array of serialized filter expressions
   * @param schemas
   *   Array of serialized schemas
   * @return
   *   Array of arrays, one per file, containing matching row indices
   */
  @native def nativeFindMatchingRowsBatch(
      finderHandle: Long,
      filePaths: Array[String],
      filterExprs: Array[Array[Byte]],
      schemas: Array[Array[Byte]]): Array[Array[Long]]

  /**
   * Get statistics from the last operation.
   *
   * @param finderHandle
   *   Handle to the native finder instance
   * @return
   *   Array of [rowsScanned, rowsMatched, bytesRead, executionTimeMs]
   */
  @native def nativeGetStats(finderHandle: Long): Array[Long]

  /**
   * Release the native finder instance.
   *
   * @param finderHandle
   *   Handle to the native finder instance
   */
  @native def nativeReleaseFinder(finderHandle: Long): Unit

  /** Configuration for row index finder. */
  case class Config(batchSize: Int = 10000, numThreads: Int = 1, memoryLimitMB: Int = 1024)

  /** Statistics from row finding operation. */
  case class Stats(rowsScanned: Long, rowsMatched: Long, bytesRead: Long, executionTimeMs: Long)

  private def withFinder[T](config: Config)(f: Long => T): T = {
    val finderHandle = nativeCreateFinderWithConfig(
      0, // Use default memory pool
      config.batchSize,
      config.numThreads,
      config.memoryLimitMB)
    try {
      f(finderHandle)
    } finally {
      nativeReleaseFinder(finderHandle)
    }
  }

  private def findMatchingRowsWithFinder(
      finderHandle: Long,
      filePath: String,
      serializedFilter: Option[SerializedFilter],
      serializedSchema: Array[Byte]): Array[Long] = {
    nativeFindMatchingRows(
      finderHandle,
      filePath,
      serializedFilter.map(_.expressionBytes).orNull,
      serializedSchema,
      serializedFilter.map(_.functionMappings).orNull)
  }

  /**
   * High-level API: Find matching rows in a single file.
   *
   * @param filePath
   *   Path to the Parquet file
   * @param filter
   *   Filter expression (Spark expression)
   * @param schema
   *   File schema
   * @param config
   *   Optional configuration
   * @return
   *   Array of matching row indices
   */
  def findMatchingRows(
      filePath: String,
      filter: Option[Expression],
      schema: StructType,
      config: Config = Config()): Array[Long] = {
    findMatchingRowsSerialized(
      filePath,
      serializeFilter(filter, schema),
      serializeSchema(schema),
      config)
  }

  def findMatchingRowsSerialized(
      filePath: String,
      serializedFilter: Option[SerializedFilter],
      serializedSchema: Array[Byte],
      config: Config = Config()): Array[Long] = {
    withFinder(config) {
      finderHandle =>
        findMatchingRowsWithFinder(finderHandle, filePath, serializedFilter, serializedSchema)
    }
  }

  def findMatchingRowsSerializedBatch(
      filePaths: Seq[String],
      serializedFilter: Option[SerializedFilter],
      serializedSchema: Array[Byte],
      config: Config = Config(),
      parallelism: Int = 1): Map[String, Array[Long]] = {
    if (filePaths.isEmpty) {
      Map.empty
    } else if (parallelism <= 1 || filePaths.lengthCompare(1) <= 0) {
      withFinder(config) {
        finderHandle =>
          filePaths.iterator.map {
            filePath =>
              filePath -> findMatchingRowsWithFinder(
                finderHandle,
                filePath,
                serializedFilter,
                serializedSchema)
          }.toMap
      }
    } else {
      val workerCount = math.min(parallelism, filePaths.size)
      val executor = Executors.newFixedThreadPool(workerCount)
      implicit val executionContext: ExecutionContext =
        ExecutionContext.fromExecutorService(executor)
      try {
        val chunkSize = math.max(1, (filePaths.size + workerCount - 1) / workerCount)
        // scalastyle:off awaitresult
        Await
          .result(
            Future.sequence(filePaths.grouped(chunkSize).map {
              chunk =>
                Future {
                  withFinder(config) {
                    finderHandle =>
                      chunk.iterator.map {
                        filePath =>
                          filePath -> findMatchingRowsWithFinder(
                            finderHandle,
                            filePath,
                            serializedFilter,
                            serializedSchema)
                      }.toMap
                  }
                }
            }),
            Duration.Inf
          )
          .foldLeft(Map.empty[String, Array[Long]])(_ ++ _)
        // scalastyle:on awaitresult
      } finally {
        executor.shutdown()
      }
    }
  }

  def serializeFilter(filter: Option[Expression], schema: StructType): Option[SerializedFilter] = {
    filter.map(ExpressionSerializer.serialize(_, schema))
  }

  def serializeSchema(schema: StructType): Array[Byte] = {
    SchemaSerializer.serialize(schema)
  }

  /**
   * High-level API: Find matching rows in multiple files (batch mode).
   *
   * @param files
   *   Sequence of (filePath, filter, schema) tuples
   * @param config
   *   Optional configuration
   * @return
   *   Map of filePath -> row indices
   */
  def findMatchingRowsBatch(
      files: Seq[(String, Option[Expression], StructType)],
      config: Config = Config()): Map[String, Array[Long]] = {
    files.map {
      case (filePath, filter, schema) =>
        filePath -> findMatchingRows(filePath, filter, schema, config)
    }.toMap
  }

  /**
   * Get statistics from a finder instance.
   *
   * @param finderHandle
   *   Handle to the native finder instance
   * @return
   *   Statistics object
   */
  def getStats(finderHandle: Long): Stats = {
    val statsArray = nativeGetStats(finderHandle)
    Stats(
      rowsScanned = statsArray(0),
      rowsMatched = statsArray(1),
      bytesRead = statsArray(2),
      executionTimeMs = statsArray(3))
  }
}

/** Helper object for serializing Spark expressions to Substrait format. */
object ExpressionSerializer {

  /**
   * Serialize a Spark expression to Substrait format.
   *
   * @param expr
   *   Spark expression
   * @return
   *   Serialized bytes
   */
  def serialize(expr: Expression, schema: StructType): VeloxRowIndexFinderJni.SerializedFilter = {
    val inputAttributes = SchemaSerializer.outputAttributes(schema)
    val reboundExpr = rebindConditionToOutput(expr, inputAttributes)
    val context = new SubstraitContext
    val expressionNode = SubstraitUtil.toSubstraitExpression(reboundExpr, inputAttributes, context)
    val functionMappings = context.registeredFunction.asScala.toSeq.map {
      case (name, functionId) =>
        ExtensionBuilder.makeFunctionMapping(name, functionId).toProtobuf.toByteArray
    }.toArray

    VeloxRowIndexFinderJni.SerializedFilter(
      expressionBytes = expressionNode.toProtobuf.toByteArray,
      functionMappings = functionMappings)
  }

  private def rebindConditionToOutput(expr: Expression, output: Seq[Attribute]): Expression = {
    val resolver = SQLConf.get.resolver
    expr.transform {
      case attr: AttributeReference =>
        val matches = output.filter(candidate => resolver(candidate.name, attr.name))
        if (matches.lengthCompare(1) == 0) {
          matches.head
        } else if (matches.isEmpty) {
          throw new IllegalStateException(
            s"Unable to rebind delete predicate attribute `${attr.name}` " +
              "to native row finder output.")
        } else {
          throw new IllegalStateException(
            s"Ambiguous delete predicate attribute `${attr.name}` " +
              "in native row finder output.")
        }
    }
  }
}

/** Helper object for serializing Spark schemas. */
object SchemaSerializer {

  /**
   * Serialize a Spark schema.
   *
   * @param schema
   *   Spark schema
   * @return
   *   Serialized bytes
   */
  def serialize(schema: StructType): Array[Byte] = {
    SubstraitUtil
      .createNameStructBuilder(
        ConverterUtils.collectAttributeTypeNodes(schema),
        schema.fields.map(_.name).toSeq.asJava,
        Collections.emptyList[ColumnTypeNode]())
      .build()
      .toByteArray
  }

  def outputAttributes(schema: StructType): Seq[AttributeReference] = {
    schema.fields.map {
      field => AttributeReference(field.name, field.dataType, field.nullable)()
    }.toSeq
  }
}

// Made with Bob
