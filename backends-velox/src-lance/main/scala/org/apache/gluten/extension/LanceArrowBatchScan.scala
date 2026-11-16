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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.lance.spark.read.LanceScan

import scala.collection.JavaConverters._

/**
 * Makes a Lance scan emit Arrow batches (lance-spark's `LanceScan.withArrowBatches`) when Velox
 * imports every column type of them, so that Gluten offloads its output to Velox through
 * [[org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType]]. The vanilla
 * [[BatchScanExec]] stays in the plan, and keeps the scan's runtime filters, partitioning and
 * metrics.
 */
case class LanceArrowBatchScan(glutenConf: GlutenConfig) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableColumnarBatchScan) {
      return plan
    }
    plan.transformUp {
      case scan: BatchScanExec =>
        scan.scan match {
          case lance: LanceScan
              if !lance.readOptions().isArrowBatchReadsEnabled &&
                LanceArrowBatchScan.importsToVelox(lance) =>
            scan.copy(scan = lance.withArrowBatches())
          case _ => scan
        }
    }
  }
}

object LanceArrowBatchScan {

  /**
   * Whether `plan` reads a Lance scan's Arrow batches that Velox imports, which Gluten then takes
   * as Java Arrow batches.
   */
  def isArrowBatchScan(plan: BatchScanExec): Boolean = plan.scan match {
    case lance: LanceScan =>
      lance.readOptions().isArrowBatchReadsEnabled && importsToVelox(lance) &&
      plan.supportsColumnar
    case _ => false
  }

  /** Whether the scan can emit Arrow batches, and Velox imports every column of them. */
  def importsToVelox(scan: LanceScan): Boolean = {
    val schema = scan.arrowBatchSchema()
    schema.isPresent && schema.get.getFields.asScala.forall(importsToVelox)
  }

  /**
   * Whether Velox's Arrow import reads `field` as the type Spark reads it as. Spark's
   * `ArrowColumnVector` already rules out unsigned integers, `Float16`, fixed-size binaries and
   * lists, and other date and time units. Interval and dictionary-encoded columns are left to
   * vanilla Spark.
   */
  private def importsToVelox(field: Field): Boolean =
    field.getDictionary == null && (field.getType match {
      case _: ArrowType.Null | _: ArrowType.Bool => true
      case int: ArrowType.Int => int.getIsSigned
      case float: ArrowType.FloatingPoint => float.getPrecision != FloatingPointPrecision.HALF
      case _: ArrowType.Utf8 | _: ArrowType.LargeUtf8 => true
      case _: ArrowType.Binary | _: ArrowType.LargeBinary => true
      case decimal: ArrowType.Decimal => decimal.getBitWidth == 128
      case date: ArrowType.Date => date.getUnit == DateUnit.DAY
      // With a time zone for TIMESTAMP, without one for TIMESTAMP_NTZ.
      case timestamp: ArrowType.Timestamp => timestamp.getUnit == TimeUnit.MICROSECOND
      case _: ArrowType.List | _: ArrowType.Struct | _: ArrowType.Map =>
        field.getChildren.asScala.forall(importsToVelox)
      case _ => false
    })
}
