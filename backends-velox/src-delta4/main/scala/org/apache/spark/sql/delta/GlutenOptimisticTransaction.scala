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
package org.apache.spark.sql.delta

import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.extension.columnar.transition.Transitions

import org.apache.spark.sql.{AnalysisException, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints, DeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.files.{DeltaFileFormatWriter, GlutenDeltaFileFormatWriter, TransactionalWrite}
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.perf.{DeltaOptimizedWriterExec, GlutenDeltaOptimizedWriterExec}
import org.apache.spark.sql.delta.schema.{EmptySchemaWriteException, InnerInvariantViolationException}
import org.apache.spark.sql.delta.shims.VariantShreddingShims
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.TableParquetVersionOption
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ScalaExtensions.OptionExt
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable.ListBuffer

/**
 * An [[OptimisticTransaction]] whose `writeFiles` writes with the Velox writer.
 *
 * `writeFiles` is a copy of `TransactionalWrite.writeFiles` from delta-io/delta v4.4.1
 * (spark/src/main/scala/org/apache/spark/sql/delta/files/TransactionalWrite.scala). The copied code
 * is kept verbatim between `format: off` and `format: on`; Gluten's changes are only the
 * `GLUTEN-SPLICE` regions. Removing those regions gives back Delta's method, so on a Delta bump
 * re-copy the upstream method and re-apply the splices.
 */
class GlutenOptimisticTransaction(delegate: OptimisticTransaction)
  extends OptimisticTransaction(
    delegate.deltaLog,
    delegate.catalogTable,
    delegate.snapshot
  ) {

  // scalastyle:off
  // format: off
  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean,
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    // GLUTEN-SPLICE-BEGIN: schemas the native write path cannot handle use Delta's writeFiles.
    GlutenDeltaNativeWriteGate.unsupportedSchema(inputData.schema).foreach { reason =>
      logInfo(s"$reason; falling back to the vanilla Delta write path.")
      return super.writeFiles(inputData, writeOptions, isOptimize, additionalConstraints)
    }
    // GLUTEN-SPLICE-END
    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, trackFromData) =
      normalizeData(deltaLog, writeOptions, data)
    // Use the track set from the transaction if set,
    // otherwise use the track set from `normalizeData()`.
    val trackIdentityHighWaterMarks = trackHighWaterMarks.getOrElse(trackFromData)

    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = getCommitter(outputPath)

    val (statsDataSchema, _) = getStatsSchema(output, partitionSchema)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val (optionalStatsTracker, _) = getOptionalStatsTrackerAndStatsCollection(output, outputPath,
      partitionSchema, data)


    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints
    Constraints.validateCheckConstraints(spark, constraints, deltaLog, metadata.schema)

    val identityTrackerOpt = IdentityColumn.createIdentityColumnStatsTracker(
      spark,
      deltaLog.newDeltaHadoopConf(),
      outputPath,
      metadata.schema,
      statsDataSchema,
      trackIdentityHighWaterMarks
    )

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(
        outputPath.toString,
        Map.empty,
        output)

      val empty2NullPlan = convertEmptyToNullIfNeeded(queryExecution.executedPlan,
        partitioningColumns, constraints)
      val checkInvariants = DeltaInvariantCheckerExec(spark, empty2NullPlan, constraints)
      // No need to plan optimized write if the write command is OPTIMIZE, which aims to produce
      // evenly-balanced data files already.
      val physicalPlan = if (!isOptimize &&
        shouldOptimizeWrite(writeOptions, spark.sessionState.conf)) {
        DeltaOptimizedWriterExec(checkInvariants, metadata.partitionColumns, deltaLog)
      } else {
        checkInvariants
      }

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      // Iceberg spec requires partition columns in data files
      val writePartitionColumns = IcebergCompat.isAnyEnabled(metadata) ||
        protocol.isFeatureSupported(MaterializePartitionColumnsTableFeature) ||
        DeltaConfigs.WRITE_PARTITION_COLUMNS_TO_PARQUET.fromMetaData(metadata).getOrElse(true)
      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val filteredOptions = (writeOptions match {
        case None => Map.empty[String, String]
        case Some(writeOptions) =>
          writeOptions.options.filterKeys { key =>
            key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
              key.equalsIgnoreCase(DeltaOptions.COMPRESSION) ||
              key.equalsIgnoreCase(DeltaOptions.PARQUET_VERSION) ||
              key.equalsIgnoreCase(DeltaOptions.PARQUET_OUTPUT_TIMESTAMP_TYPE)
          }.toMap
      }) + (DeltaOptions.WRITE_PARTITION_COLUMNS -> writePartitionColumns.toString)
      val options = filteredOptions ++
        VariantShreddingShims.getVariantInferShreddingSchemaOptions(
          DeltaConfigs.ENABLE_VARIANT_SHREDDING.fromMetaData(metadata)) ++
        TableParquetVersionOption.getWriterOptions(
          spark = spark,
          writerOptions = filteredOptions,
          tableProperties = metadata.configuration)

      try {
        // GLUTEN-SPLICE-BEGIN: use the Velox writer when it reproduces the files Delta writes.
        // The job conf Delta passes to its writer below, where parquet-mr reads its settings.
        // scalastyle:off deltahadoopconfiguration
        val jobHadoopConf =
          spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options)
        // scalastyle:on deltahadoopconfiguration
        val nativeUnsupportedReason = GlutenDeltaNativeWriteGate.unsupportedWrite(
          spark.sessionState.conf,
          jobHadoopConf,
          options,
          DataTypeUtils.fromAttributes(output),
          partitioningColumns,
          metadata)
        if (nativeUnsupportedReason.isEmpty) {
          GlutenDeltaFileFormatWriter.write(
            sparkSession = spark,
            plan = glutenPhysicalPlan(spark, empty2NullPlan, constraints, isOptimize, writeOptions),
            fileFormat = new GlutenDeltaParquetFileFormat(protocol, metadata),
            committer = committer,
            outputSpec = outputSpec,
            hadoopConf = jobHadoopConf,
            partitionColumns = partitioningColumns,
            bucketSpec = None,
            statsTrackers = optionalStatsTracker.toSeq
              ++ statsTrackers
              ++ identityTrackerOpt.toSeq,
            options = options)
        } else {
          logInfo(s"${nativeUnsupportedReason.get}; writing with the vanilla Delta writer.")
        // GLUTEN-SPLICE-END
        DeltaFileFormatWriter.write(
          sparkSession = spark,
          plan = physicalPlan,
          fileFormat = deltaLog.fileFormat(protocol, metadata), // TODO support changing formats.
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers = optionalStatsTracker.toSeq
            ++ statsTrackers
            ++ identityTrackerOpt.toSeq,
          options = options)
        // GLUTEN-SPLICE-BEGIN
        }
        // GLUTEN-SPLICE-END
      } catch {
        case InnerInvariantViolationException(violationException) =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          throw violationException
        case e if EmptySchemaWriteException.isEmptySchemaWriteException(e) =>
          EmptySchemaWriteException.throwEmptySchemaWriteException(e, metadata)
      }
      statsTrackers.foreach {
        case tracker: BasicWriteJobStatsTracker =>
          val numOutputRowsOpt = tracker.driverSideMetrics.get("numOutputRows").map(_.value)
          IdentityColumn.logTableWrite(snapshot, trackIdentityHighWaterMarks, numOutputRowsOpt)
        case _ => ()
      }
    }

    var resultFiles =
      (if (optionalStatsTracker.isDefined) {
        committer.addedStatuses.map { a =>
          a.copy(stats = optionalStatsTracker.map(
            _.recordedStats(a.toPath.getName)).getOrElse(a.stats))
        }
      }
      else {
        committer.addedStatuses
      })
      .filter {
      // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
      // may be fixed in the future) are the MERGE command when you delete with empty source, or
      // empty target, or on disjoint tables. This is hard to catch before the write without
      // collecting the DF ahead of time. Instead, we can return only the AddFiles that
      // a) actually add rows, or
      // b) don't have any stats so we don't know the number of rows at all
      case a: AddFile => a.numLogicalRecords.forall(_ > 0)
      case _ => true
    }

    // add [[AddFile.Tags.ICEBERG_COMPAT_VERSION.name]] tags to addFiles
    // starting from IcebergCompatV2
    val enabledCompat = IcebergCompat.anyEnabled(metadata)
    if (enabledCompat.exists(_.version >= 2)) {
      resultFiles = resultFiles.map { addFile =>
        addFile.copy(tags = Option(addFile.tags).getOrElse(Map.empty[String, String]) +
          (AddFile.Tags.ICEBERG_COMPAT_VERSION.name -> enabledCompat.get.version.toString)
        )
      }
    }


    if (resultFiles.nonEmpty && !isOptimize) registerPostCommitHook(AutoCompact)
    // Record the updated high water marks to be used during transaction commit.
    identityTrackerOpt.ifDefined { tracker =>
      updatedIdentityHighWaterMarks.appendAll(tracker.highWaterMarks.toSeq)
    }

    resultFiles.toSeq ++ committer.changeFiles
  }
  // format: on
  // scalastyle:on

  /**
   * The plan the Velox writer consumes: Delta's plan made columnar. The invariant checker is left
   * out when there are no constraints, to avoid the transitions around it.
   */
  private def glutenPhysicalPlan(
      spark: SparkSession,
      empty2NullPlan: SparkPlan,
      constraints: Seq[Constraint],
      isOptimize: Boolean,
      writeOptions: Option[DeltaOptions]): SparkPlan = {
    val maybeCheckInvariants = if (constraints.isEmpty) {
      empty2NullPlan
    } else {
      DeltaInvariantCheckerExec(spark, empty2NullPlan, constraints)
    }
    def toVeloxPlan(plan: SparkPlan): SparkPlan = plan match {
      case aqe: AdaptiveSparkPlanExec =>
        assert(!aqe.isFinalPlan)
        aqe.copy(supportsColumnar = true)
      case _ => Transitions.toBatchPlan(maybeCheckInvariants, VeloxBatchType)
    }
    // No need to plan optimized write if the write command is OPTIMIZE, which aims to produce
    // evenly-balanced data files already.
    if (!isOptimize && shouldOptimizeWrite(writeOptions, spark.sessionState.conf)) {
      // We uniformly convert the query plan to a columnar plan. If
      // the further write operation turns out to be non-offload-able, the
      // columnar plan will be converted back to a row-based plan.
      val veloxPlan = toVeloxPlan(maybeCheckInvariants)
      try {
        val glutenWriterExec =
          GlutenDeltaOptimizedWriterExec(veloxPlan, metadata.partitionColumns, deltaLog)
        val validationResult = glutenWriterExec.doValidate()
        if (validationResult.ok()) {
          glutenWriterExec
        } else {
          logInfo(
            s"GlutenDeltaOptimizedWriterExec: Internal shuffle validated negative," +
              s" reason: ${validationResult.reason()}. Falling back to row-based shuffle.")
          DeltaOptimizedWriterExec(maybeCheckInvariants, metadata.partitionColumns, deltaLog)
        }
      } catch {
        case e: AnalysisException =>
          logWarning(
            s"GlutenDeltaOptimizedWriterExec: Failed to create internal shuffle," +
              s" reason: ${e.getMessage()}. Falling back to row-based shuffle.")
          DeltaOptimizedWriterExec(maybeCheckInvariants, metadata.partitionColumns, deltaLog)
      }
    } else {
      toVeloxPlan(maybeCheckInvariants)
    }
  }

  private def shouldOptimizeWrite(
      writeOptions: Option[DeltaOptions],
      sessionConf: SQLConf): Boolean = {
    writeOptions
      .flatMap(_.optimizeWrite)
      .getOrElse(TransactionalWrite.shouldOptimizeWrite(metadata, sessionConf))
  }
}
