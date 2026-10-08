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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.gluten.connector.write.IcebergRowDeltaWrite
import org.apache.gluten.execution.VeloxIcebergRowDeltaExec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Instruction, Keep, ROW_ID}
import org.apache.spark.sql.classic.ClassicTypes.ClassicSparkSession
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.iceberg.{FileFormat, HasTableOperations, IsolationLevel, MetadataColumns, Schema, TableProperties, TableUtil}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.{SparkFilters, SparkReadOptions, SparkSchemaUtil, SparkWriteConf}
import org.apache.iceberg.spark.source.{GlutenEqualityDeleteWriterUtil, SparkTable}
import org.apache.iceberg.types.{Type, TypeUtil}
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.util.SnapshotUtil

import java.util.{Collections, HashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class GlutenIcebergRowDelta(
    child: LogicalPlan,
    originalTable: DataSourceV2Relation,
    write: IcebergRowDeltaWrite)
  extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

class GlutenIcebergRowDeltaRewrite(spark: SparkSession) extends Logging {
  def rewrite(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case d: DeleteFromTable if d.resolved =>
      prepare(d.table, Seq(d.condition), "delete").map {
        c => c.command(Project(c.record(Some(c.keys), None), Filter(d.condition, c.scan)))
      }
    case u: UpdateTable if u.resolved && u.aligned =>
      val condition = u.condition.getOrElse(Literal.TrueLiteral)
      prepare(u.table, Seq(condition), "update").map {
        c =>
          val data = c.newData(u.assignments, preserveRowId = true)
          c.command(Project(c.record(Some(c.keys), Some(data)), Filter(condition, c.scan)))
      }
    case m: MergeIntoTable if m.resolved && m.aligned => rewriteMerge(m)
    case _ => None
  }

  private def prepare(
      target: LogicalPlan,
      predicates: Seq[Expression],
      operation: String): Option[Context] = {
    if (predicates.exists(!_.deterministic)) return None
    val relation = EliminateSubqueryAliases(target) match {
      case r: DataSourceV2Relation => r
      case _ => return None
    }
    val sparkTable = relation.table match {
      case t: SparkTable if t.snapshotId() == null => t
      case _ => return None
    }
    val table = sparkTable.table()
    if (
      !table.isInstanceOf[HasTableOperations] || !Set(2, 3).contains(TableUtil.formatVersion(table))
    ) {
      return None
    }
    if (
      !table.properties().getOrDefault(
        s"write.$operation.mode",
        "copy-on-write").equalsIgnoreCase("merge-on-read")
    ) {
      return None
    }
    val conf = new SparkWriteConf(
      spark,
      table,
      sparkTable.branch(),
      Collections.emptyMap[String, String]()) {
      override def deleteFileFormat(): FileFormat =
        Option(table.properties().get(TableProperties.DELETE_DEFAULT_FILE_FORMAT))
          .map(FileFormat.fromString).getOrElse(dataFileFormat())
    }
    if (!Set(FileFormat.PARQUET, FileFormat.ORC, FileFormat.AVRO).contains(conf.deleteFileFormat()))
      return None
    val branch = conf.branch()
    val schema = SnapshotUtil.schemaFor(table, branch)
    val ids = equalityIds(predicates, relation, schema).getOrElse(return None)
    val deleteSchema = TypeUtil.select(schema, ids.map(Int.box).toSet.asJava)
    val codec = conf.writeProperties().getOrDefault(
      TableProperties.DELETE_PARQUET_COMPRESSION,
      TableProperties.PARQUET_COMPRESSION_DEFAULT).toLowerCase(java.util.Locale.ROOT)
    IcebergRowDeltaWrite.nativeUnsupportedReason(
      table,
      deleteSchema,
      conf.deleteFileFormat(),
      codec).foreach {
      reason =>
        logInfo(s"Retaining Iceberg's $operation plan: $reason")
        return None
    }
    val snapshot = Option(SnapshotUtil.latestSnapshot(
      table.asInstanceOf[HasTableOperations].operations().current(),
      branch))
    val options = new HashMap[String, String](relation.options.asCaseSensitiveMap())
    // SparkTable.equals ignores snapshot IDs; scan options prevent stale cache matches.
    snapshot.foreach(s => options.put(SparkReadOptions.SNAPSHOT_ID, s.snapshotId().toString))
    // Keep the current schema, including columns added after the pinned snapshot.
    val pinnedTable = GlutenEqualityDeleteWriterUtil.scanTable(table)
    val readRelation =
      relation.copy(table = pinnedTable, options = new CaseInsensitiveStringMap(options))
        .withMetadataColumns()
    val scan: LogicalPlan =
      if (snapshot.isEmpty) LocalRelation(readRelation.output) else readRelation
    val conflict = (if (operation == "merge") Nil else predicates).flatMap {
      predicate =>
        if (predicate.references.subsetOf(relation.outputSet)) {
          DataSourceStrategy.translateFilter(predicate, true).flatMap(
            f => Option(SparkFilters.convert(f)))
        } else None
    }.foldLeft(Expressions.alwaysTrue(): org.apache.iceberg.expressions.Expression)(Expressions.and)
    val isolation = IsolationLevel.fromName(
      table.properties().getOrDefault(s"write.$operation.isolation-level", "serializable"))
    val dataSchema = if (TableUtil.supportsRowLineage(table)) {
      MetadataColumns.schemaWithRowLineage(schema)
    } else schema
    val write = new IcebergRowDeltaWrite(
      table,
      snapshot.map(_.snapshotId()),
      snapshot.map(_.sequenceNumber()).getOrElse(0L),
      deleteSchema,
      dataSchema,
      ids,
      conflict,
      isolation,
      conf,
      operation)
    Some(Context(relation, scan, schema, deleteSchema, dataSchema, write))
  }

  // Identifier fields alone could also delete rows that fail the predicates.
  private def equalityIds(
      predicates: Seq[Expression],
      relation: DataSourceV2Relation,
      schema: Schema): Option[Seq[Int]] = {
    val ids = mutable.LinkedHashSet.empty[Int]
    var supported = true
    def path(expr: Expression): Option[(Attribute, Seq[Int])] = expr match {
      case a: Attribute if relation.outputSet.contains(a) => Some(a -> Nil)
      case GetStructField(child, ordinal, _) =>
        path(child).map { case (a, p) => a -> (p :+ ordinal) }
      case OuterReference(child) => path(child)
      case _ => None
    }
    def visit(expr: Expression): Unit = path(expr) match {
      case Some((attr, Nil)) =>
        schema.columns().asScala.find(_.name() == attr.name) match {
          case Some(field) =>
            if (!eligible(field.`type`())) supported = false else ids += field.fieldId()
          case None => supported = false
        }
      // Iceberg 1.10 cannot read nested equality keys omitted from the read projection.
      case Some(_) => supported = false
      case None => expr.children.foreach(visit)
    }
    predicates.foreach(visit)
    if (!supported) return None
    // Constant predicates can use any supported top-level key. Skip types whose
    // physical encoding is unavailable natively, even if Iceberg permits them as keys.
    if (ids.isEmpty) {
      schema.columns().asScala.find(
        f => IcebergRowDeltaWrite.supportsNativeEqualityType(f.`type`()))
        .foreach(f => ids += f.fieldId())
    }
    if (ids.isEmpty) None else Some(ids.toSeq)
  }

  private def eligible(t: Type): Boolean = t.isPrimitiveType &&
    !Set(TypeID.FLOAT, TypeID.DOUBLE, TypeID.UNKNOWN, TypeID.VARIANT).contains(t.typeId())

  private case class Context(
      relation: DataSourceV2Relation,
      scan: LogicalPlan,
      schema: Schema,
      deleteSchema: Schema,
      dataSchema: Schema,
      write: IcebergRowDeltaWrite) {
    private def metadata(name: String): Attribute = scan.output.find(_.name == name).get
    private def project(
        t: org.apache.iceberg.types.Types.StructType,
        input: Seq[Expression],
        source: org.apache.iceberg.types.Types.StructType): Expression = {
      CreateNamedStruct(t.fields().asScala.flatMap {
        field =>
          val ordinal = source.fields().asScala.indexWhere(_.fieldId() == field.fieldId())
          val value = input(ordinal)
          val projected = if (field.`type`().isStructType) {
            val sourceType = source.fields().get(ordinal).`type`().asStructType()
            val children = sourceType.fields().asScala.zipWithIndex.map {
              case (f, i) => GetStructField(value, i, Some(f.name())): Expression
            }
            val nested = project(field.`type`().asStructType(), children.toSeq, sourceType)
            If(IsNull(value), Literal.create(null, nested.dataType), nested)
          } else value
          Seq(Literal(field.name()), projected)
      }.toSeq)
    }
    val keys: Expression = project(deleteSchema.asStruct(), relation.output, schema.asStruct())
    private val keyType = SparkSchemaUtil.convert(deleteSchema)
    private val dataType = SparkSchemaUtil.convert(dataSchema)
    private val partition = metadata(MetadataColumns.PARTITION_COLUMN_NAME)

    def newData(assignments: Seq[Assignment], preserveRowId: Boolean): Expression = {
      val userFields = schema.columns().asScala.zip(assignments).flatMap {
        case (field, assignment) => Seq(Literal(field.name()), assignment.value)
      }
      val lineage = dataSchema.columns().asScala.drop(schema.columns().size()).flatMap {
        field =>
          val value = if (preserveRowId && field.fieldId() == MetadataColumns.ROW_ID.fieldId()) {
            metadata(field.name())
          } else Literal.create(null, SparkSchemaUtil.convert(field.`type`()))
          Seq(Literal(field.name()), value)
      }
      CreateNamedStruct((userFields ++ lineage).toSeq)
    }
    def values(keys: Option[Expression], data: Option[Expression]): Seq[Expression] = Seq(
      keys.getOrElse(Literal.create(null, keyType)),
      data.getOrElse(Literal.create(null, dataType)),
      if (keys.isDefined) metadata(MetadataColumns.SPEC_ID.name())
      else Literal.create(null, org.apache.spark.sql.types.IntegerType),
      if (keys.isDefined) partition else Literal.create(null, partition.dataType),
      if (keys.isDefined) metadata(MetadataColumns.FILE_PATH.name())
      else Literal.create(null, org.apache.spark.sql.types.StringType)
    )
    def record(keys: Option[Expression], data: Option[Expression]): Seq[NamedExpression] =
      values(
        keys,
        data).zip(Seq("__delete_keys", "__new_row", "__spec_id", "__partition", "__file"))
        .map { case (value, name) => Alias(value, name)() }
    def command(query: LogicalPlan): LogicalPlan = GlutenIcebergRowDelta(query, relation, write)
  }

  private def rewriteMerge(m: MergeIntoTable): Option[LogicalPlan] = {
    val actions = m.matchedActions ++ m.notMatchedActions ++ m.notMatchedBySourceActions
    val predicates = m.mergeCondition +: actions.flatMap(_.condition)
    if (
      predicates.exists(
        e =>
          SubqueryExpression.hasSubquery(e) ||
            e.exists(_.isInstanceOf[AggregateExpression]))
    ) return None
    prepare(m.targetTable, predicates, "merge").map {
      c =>
        val checkCardinality = m.matchedActions match {
          case Nil | Seq(DeleteAction(None)) => false
          case _ => true
        }
        val targetPresent = Alias(Literal.TrueLiteral, "__gluten_target_present")()
        val sourcePresent = Alias(Literal.TrueLiteral, "__gluten_source_present")()
        val targetColumns = c.scan.output :+ targetPresent
        val target = Project(
          if (checkCardinality) {
            targetColumns :+ Alias(MonotonicallyIncreasingID(), ROW_ID)()
          } else targetColumns,
          c.scan)
        val source = Project(m.sourceTable.output :+ sourcePresent, m.sourceTable)
        val joinType = (m.notMatchedActions.nonEmpty, m.notMatchedBySourceActions.nonEmpty) match {
          case (true, true) => FullOuter
          case (true, false) => RightOuter
          case (false, true) => LeftOuter
          case _ => Inner
        }
        val hint = if (checkCardinality) {
          JoinHint(Some(HintInfo(Some(NO_BROADCAST_AND_REPLICATION))), None)
        } else JoinHint.NONE
        val join = Join(target, source, joinType, Some(m.mergeCondition), hint)
        def instruction(action: MergeAction): Instruction = action match {
          case DeleteAction(condition) =>
            Keep(condition.getOrElse(Literal.TrueLiteral), c.values(Some(c.keys), None))
          case UpdateAction(condition, assignments) =>
            Keep(
              condition.getOrElse(Literal.TrueLiteral),
              c.values(Some(c.keys), Some(c.newData(assignments, preserveRowId = true))))
          case InsertAction(condition, assignments) =>
            Keep(
              condition.getOrElse(Literal.TrueLiteral),
              c.values(None, Some(c.newData(assignments, preserveRowId = false))))
          case other => throw new IllegalArgumentException(s"Unresolved MERGE action: $other")
        }
        val output = c.record(None, None).map(_.toAttribute.withNullability(true))
        c.command(MergeRows(
          IsNotNull(sourcePresent.toAttribute.withNullability(true)),
          IsNotNull(targetPresent.toAttribute.withNullability(true)),
          m.matchedActions.map(instruction),
          m.notMatchedActions.map(instruction),
          m.notMatchedBySourceActions.map(instruction),
          checkCardinality,
          output,
          join
        ))
    }
  }
}

case class GlutenIcebergRowDeltaStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case delta: GlutenIcebergRowDelta =>
      Seq(VeloxIcebergRowDeltaExec(
        planLater(delta.child),
        () =>
          spark.sharedState.cacheManager.recacheByPlan(
            spark.asInstanceOf[ClassicSparkSession],
            delta.originalTable),
        delta.write))
    case _ => Nil
  }
}
