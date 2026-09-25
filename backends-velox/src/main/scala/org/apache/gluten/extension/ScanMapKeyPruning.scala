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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.{BasicScanExecTransformer, FilterExecTransformerBase, GenerateExecTransformer, ProjectExecTransformerBase, SubfieldElement, SubfieldPath}
import org.apache.gluten.execution.SubfieldElement.{Field, LongKey, StringKey => StrKey}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, GenerateExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Physical plan rule that derives Velox required subfields for map columns read by a native Parquet
 * scan, so that the reader prunes map entries the query never accesses.
 *
 * Background. Parquet stores a map as a repeated `key_value` group: one key column chunk and one
 * value column chunk per row group, covering every entry of every row. Evaluating `m['k1']` on the
 * scan output therefore decodes all entries into a MapVector, although only one key is needed.
 * Velox's Hive connector lets a `HiveColumnHandle` declare required subfields, written in Velox
 * `Subfield` syntax such as `m["k1"].f1`; `makeScanSpec` turns the map keys they name into a key
 * filter on the map's key stream, so the SelectiveMapColumnReader materializes only matching
 * entries. Struct fields not covered by any required subfield are filled with nulls.
 *
 * Velox treats the declared set as complete and returns null for anything outside it, so the
 * declared paths must cover every read of the column. The rule therefore only handles one plan
 * shape, in which that is easy to show:
 * {{{
 *   Scan [id, m]                              filters: m["a"].s = 'v3'
 *     -> Filter*                              more predicates over keyed paths
 *     -> Project*                             keyed paths under aliases; m may be forwarded
 *     -> Generate*                            m forwarded only if still needed above
 *     -> a Project (or Generate) that no longer outputs m   end of the chain
 * }}}
 * Once a Project drops the attribute (and every alias that still carries map data of it), no
 * operator above can reference it: in a resolved plan an attribute can only be referenced if some
 * child outputs it. Whatever sits above the chain is therefore irrelevant and needs no analysis.
 * Aliases of keyed paths that leave the chain (`m['a'].t AS x`) are fine as well: their value
 * depends only on entries the declared paths keep.
 *
 * Supported, that is, pruned:
 *   - filters on constant-key lookups, `m['a'].s = 'v3'`, `m[1].t > 0`, `element_at(m, 'a')`,
 *     including the `isnotnull(m)` Spark infers next to them;
 *   - keyed values projected or aggregated, `SELECT m['a'].t`, `sum(m['a'].t)`, also under a
 *     `count(*)`, a `LIMIT`, an `ORDER BY ... LIMIT`, a `UNION ALL` of such projections, a write of
 *     such values, a UDF or script transformation applied to them, or a join whose sides only read
 *     keyed values below the shuffle: in all of these Spark or Gluten places the keyed access in a
 *     Project that drops the map before anything else sees it;
 *   - maps nested in structs, `c.m['a']`, including the `c.m AS _extract_m` alias Spark's
 *     NestedColumnAliasing inserts; sibling struct fields are declared alongside;
 *   - generators over a keyed value, `explode(m['a'])`: Gluten's pre-projection computes the lookup
 *     below the Generate, and the Generate forwards the map only while an operator above still
 *     needs it, so the chain may end at the Generate itself;
 *   - window partition keys, `OVER (PARTITION BY m['a'])`, which Spark's analyzer extracts into a
 *     Project below the Window's shuffle;
 *   - null checks on keyed paths, `m['a'] IS NULL`.
 *
 * Not supported, that is, the map is read whole:
 *   - the map itself, or an alias still holding it, reaching an exchange, a join, an aggregate, a
 *     union, a write, a UDTF, the fragment's output or any other operator that is not a chain node;
 *     this includes `ORDER BY m['a']`, whose pre-projection must forward the map through the
 *     shuffle because the Sort still outputs it;
 *   - whole-map uses anywhere on the chain, `size(m)`, `map_keys(m)`, `explode(m)`, `m[key_col]`;
 *   - key types without a Velox subscript form, such as date or decimal, and string keys whose
 *     bytes are not valid UTF-8.
 *
 * A chain ends at the first operator that is not a chain node, an exchange included, so a scan's
 * declaration depends only on the operators between it and that point. Structurally identical
 * exchange subtrees therefore always receive identical declarations, and exchange reuse is
 * unaffected.
 *
 * Example. For
 * {{{
 *   SELECT sum(m['a'].t) FROM t WHERE m['a'].s = 'v3'
 * }}}
 * Spark places `m['a'].t AS _extract_t` in a Project below the aggregate, so the chain is Scan ->
 * Filter -> Project and the declared paths are `m["a"].s` and `m["a"].t` (shown in Velox Subfield
 * syntax, which SubfieldPath.toString renders; the wire format is structural, one element per step,
 * see RequiredSubfieldsExtension.proto).
 */
object ScanMapKeyPruning extends Rule[SparkPlan] {

  // One element of a subfield path below the attribute, mirroring Velox Subfield::PathElement:
  // NestedField, StringSubscript, LongSubscript. `c.m["k1"].f1` is attribute `c` followed by
  // Field("m"), StrKey("k1"), Field("f1").
  private type Elem = SubfieldElement

  /** What the chain collectDeclarations collects for one scan attribute. */
  private class CollectedPaths {
    // Paths whose values are read; the map entries they name must be materialized.
    val valuePaths: mutable.LinkedHashSet[List[Elem]] =
      mutable.LinkedHashSet[List[Elem]]()
    // Paths referenced only by IsNull / IsNotNull; a null check reads the null bitmap, which the
    // reader produces for anything it materializes, so it needs no map entries of its own.
    val nullCheckPaths: mutable.LinkedHashSet[List[Elem]] =
      mutable.LinkedHashSet[List[Elem]]()
    // Set when some reference cannot be declared; the attribute is then left alone.
    var abandoned: Boolean = false
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!VeloxConfig.get.scanMapKeyPruningEnabled) {
      return plan
    }
    // Walk down from the root, remembering the chain nodes directly above the current node,
    // nearest first. Any other operator restarts the chain. Declarations are kept per scan object,
    // since two scans of one table are distinct objects.
    val declarationsByScan =
      new java.util.IdentityHashMap[SparkPlan, Map[String, Seq[SubfieldPath]]]()
    def collectDeclarations(node: SparkPlan, chain: List[SparkPlan]): Unit = node match {
      case scan: BasicScanExecTransformer if isPrunableParquetScan(scan) =>
        val mapAttrs = scan.output.filter(a => containsMapType(a.dataType) && a.name.nonEmpty)
        if (mapAttrs.nonEmpty) {
          val filters = scan.filterExprs()
          val subfields = mapAttrs.flatMap {
            attr =>
              pathsToDeclare(filters, chain, attr).map(paths => renderSubfieldPaths(attr, paths))
          }.toMap
          if (subfields.nonEmpty) {
            declarationsByScan.put(scan, subfields)
          }
        }
      case _ =>
        val next = if (isChainOperator(node)) node :: chain else Nil
        node.children.foreach(collectDeclarations(_, next))
    }
    collectDeclarations(plan, Nil)
    if (declarationsByScan.isEmpty) {
      return plan
    }

    plan.transformUp {
      case scan: BasicScanExecTransformer if declarationsByScan.containsKey(scan) =>
        val subfields = declarationsByScan.get(scan)
        logInfo(s"Applying scan map-key pruning: $subfields")
        val newScan = scan.withRequiredMapSubfields(subfields)
        newScan.copyTagsFrom(scan)
        newScan
    }
  }

  /** A native Parquet scan that supports the declaration and has none yet. */
  private def isPrunableParquetScan(scan: BasicScanExecTransformer): Boolean =
    scan.supportsMapKeyPruning && scan.requiredMapSubfields.isEmpty &&
      scan.fileFormat == ReadFileFormat.ParquetReadFormat

  /**
   * The operators a chain may consist of: Filters and Projects, which use the input rows through
   * their predicate or project list, and Generate, which forwards its requiredChildOutput and uses
   * the input only through the generator. ColumnarPartialProjectExec and
   * ColumnarPartialGenerateExec are deliberately not among them: part of their work is in a field
   * the rule cannot see.
   */
  private def isChainOperator(node: SparkPlan): Boolean = node match {
    case _: FilterExecTransformerBase | _: FilterExec => true
    case _: ProjectExecTransformerBase | _: ProjectExec => true
    case _: GenerateExecTransformer | _: GenerateExec => true
    case _ => false
  }

  /**
   * Follows 'attr' from its scan (whose pushed-down 'filters' are visited first) up the chain and
   * returns the paths to declare, or None when the attribute must stay whole.
   *
   * 'liveIds' holds the ExprIds that still carry map data of the attribute, with the path from the
   * attribute to what the id stands for: the attribute itself with an empty path, a bare alias (`m
   * AS mm`) with an empty path, or a struct-field alias (`c.m AS _extract_m`, which Spark's
   * NestedColumnAliasing inserts) with path [m]. A reference to a live id resolves by prepending
   * that path: `_extract_m['a']` is `c.m["a"]`. The chain is complete once no id is live.
   */
  private def pathsToDeclare(
      filters: Seq[Expression],
      chain: List[SparkPlan],
      attr: Attribute): Option[Seq[List[Elem]]] = {
    val paths = new CollectedPaths
    var liveIds: Map[ExprId, List[Elem]] = Map(attr.exprId -> Nil)

    def recordUsesIn(e: Expression, nullCheck: Boolean): Unit =
      recordUses(liveIds, paths, e, nullCheck)

    // A project list decides what stays live. An entry that resolves to a path under a live id
    // stays live under its own id when the path is key-free and still holds a map: a forwarded
    // `m#3`, a bare alias `m#3 AS mm#12`, or Spark's `c#3.m AS _extract_m#20` (recording the path
    // instead would declare all of it and defeat pruning below). A keyed path, or a path with no
    // map under it (`c#3.s.v AS _extract_v#8`), is recorded and needs no further tracking: the
    // entry then only carries data the declared path keeps. Anything else is a computation, a use.
    def applyProjectList(list: Seq[NamedExpression]): Unit = {
      val next = mutable.LinkedHashMap[ExprId, List[Elem]]()
      list.foreach {
        entry =>
          val expr = entry match {
            case Alias(child, _) => child
            case other => other
          }
          resolveSubfieldPath(expr) match {
            case Some((root, path)) if liveIds.contains(root.exprId) =>
              val full = liveIds(root.exprId) ++ path
              if (!hasKeyLookup(full) && containsMapType(entry.dataType)) {
                next(entry.exprId) = full
              } else {
                recordPath(paths, full, nullCheck = false)
              }
            case _ => recordUsesIn(expr, nullCheck = false)
          }
      }
      liveIds = next.toMap
    }

    // A Generate uses its input only through the generator (requiredChildOutput is a forward),
    // and outputs requiredChildOutput plus the generated columns. Whatever it does not output
    // cannot be referenced above it, so it may also end the chain.
    def applyGenerate(node: SparkPlan, generator: Expression): Unit = {
      recordUsesIn(generator, nullCheck = false)
      val forwarded = node.output.map(_.exprId).toSet
      liveIds = liveIds.filter { case (id, _) => forwarded.contains(id) }
    }

    // Filters pushed into the scan (PushDownFilterToScan) are evaluated natively on the scan
    // output; they read the map like the Filter above them does.
    filters.foreach(recordUsesIn(_, nullCheck = false))
    chain.iterator.takeWhile(_ => liveIds.nonEmpty && !paths.abandoned).foreach {
      case f: FilterExecTransformerBase => recordUsesIn(f.cond, nullCheck = false)
      case f: FilterExec => recordUsesIn(f.condition, nullCheck = false)
      case p: ProjectExecTransformerBase => applyProjectList(p.list)
      case p: ProjectExec => applyProjectList(p.projectList)
      case g: GenerateExecTransformer => applyGenerate(g, g.generator)
      case g: GenerateExec => applyGenerate(g, g.generator)
    }
    // Still live when the chain ends: the attribute (or an alias holding map data of it) reaches
    // an operator the rule does not pathsToDeclare, or the fragment's output. Stay whole.
    if (paths.abandoned || liveIds.nonEmpty) {
      return None
    }

    val value = paths.valuePaths.toSeq
    // A null check needs no map entries, so it is dropped when a value path has it as a prefix
    // (`startsWith` compares element lists: `m` is a prefix of `m["a"].t`):
    //   WHERE m IS NOT NULL AND m['a'].t > 0   -> declare m["a"].t only; declaring `m` would keep
    //                                            the whole column.
    //   WHERE m['b'] IS NULL, SELECT m['a'].t   -> declare both; without m["b"] the reader would
    //                                            drop key b and the check would see null.
    val nullOnly = paths.nullCheckPaths.toSeq.filterNot(nc => value.exists(_.startsWith(nc)))
    val all = value ++ nullOnly
    // Declare only if the reader can then skip something. A key-free path keeps everything under
    // it (Velox lets a shorter path dominate longer ones), so `size(c.m)` next to `c.m['a']`, or
    // `m IS NOT NULL` next to `m['a'] IS NULL`, reads all of the map either way; declaring would
    // only rewrite the scan for nothing.
    val keyFree = all.filterNot(hasKeyLookup)
    val prunes = all.exists(p => hasKeyLookup(p) && !keyFree.exists(prefix => p.startsWith(prefix)))
    if (prunes) Some(all) else None
  }

  /**
   * Classifies one expression: a subfield path under a live id is recorded, anything else is
   * decomposed into its children. The largest sub-expression that is a path is the informative one,
   * so the whole expression is tried first. `cardinality(m)` is not a path; the descent reaches
   * `m`, whose empty path means the map is used whole, and recordPath() abandons. For
   * `cardinality(c.m)` the descent records the key-free path `c.m`, which at emission dominates
   * every keyed path below it and so suppresses the declaration. 'nullCheck' is set for the operand
   * of IsNull / IsNotNull.
   */
  private def recordUses(
      liveIds: Map[ExprId, List[Elem]],
      paths: CollectedPaths,
      e: Expression,
      nullCheck: Boolean): Unit = e match {
    case n: IsNotNull => recordUses(liveIds, paths, n.child, nullCheck = true)
    case n: IsNull => recordUses(liveIds, paths, n.child, nullCheck = true)
    case _ =>
      resolveSubfieldPath(e) match {
        case Some((root, path)) =>
          liveIds.get(root.exprId).foreach(prefix => recordPath(paths, prefix ++ path, nullCheck))
        case None => e.children.foreach(recordUses(liveIds, paths, _, nullCheck = false))
      }
  }

  /**
   * Records a reference to 'path', or abandons the attribute when the path cannot be declared: the
   * empty path, i.e. the attribute itself used whole (`size(m)`, `first(m)`), unless it is a null
   * check; or a path naming an empty field, which the native side cannot represent. Paths come from
   * resolved expressions, so they fit the schema; Velox keeps everything under them.
   */
  private def recordPath(paths: CollectedPaths, path: List[Elem], nullCheck: Boolean): Unit = {
    val emptyName = path.exists {
      case Field(name) => name.isEmpty
      case _ => false
    }
    if (emptyName || (path.isEmpty && !nullCheck)) {
      paths.abandoned = true
    } else if (nullCheck) {
      paths.nullCheckPaths += path
    } else {
      paths.valuePaths += path
    }
  }

  /** True if 'dt' is or contains a MapType, so an attribute of this type is a candidate. */
  private def containsMapType(dt: DataType): Boolean = dt match {
    case _: MapType => true
    case s: StructType => s.exists(f => containsMapType(f.dataType))
    case a: ArrayType => containsMapType(a.elementType)
    case _ => false
  }

  /**
   * Resolves 'e' to a subfield path: an AttributeReference at the root, followed by GetStructField
   * and constant-key GetMapValue / ElementAt steps. Catalyst nests these extractors outside-in, so
   * the match recurses to the attribute and appends one element per level on the way back, leaving
   * the elements in root-to-leaf order. None for anything else, for example `cardinality(m)` or a
   * lookup with a non-constant key.
   */
  private def resolveSubfieldPath(e: Expression): Option[(AttributeReference, List[Elem])] =
    e match {
      case a: AttributeReference => Some((a, Nil))
      case g: GetStructField =>
        // The schema's spelling, not the user's: with caseSensitive=false, `c.M` resolves to field
        // `m`, and the path must name the field as the file stores it.
        val fieldName = g.child.dataType.asInstanceOf[StructType](g.ordinal).name
        resolveSubfieldPath(g.child).map { case (a, p) => (a, p :+ (Field(fieldName): Elem)) }
      case g: GetMapValue => resolveMapLookup(g.child, g.key)
      case ea: ElementAt if ea.left.dataType.isInstanceOf[MapType] =>
        resolveMapLookup(ea.left, ea.right)
      case _ => None
    }

  /** One map lookup step of resolveSubfieldPath: the path to 'map' plus the constant 'key'. */
  private def resolveMapLookup(
      map: Expression,
      key: Expression): Option[(AttributeReference, List[Elem])] = {
    val keyType = map.dataType.asInstanceOf[MapType].keyType
    constantKeyElement(key, keyType).flatMap(
      k => resolveSubfieldPath(map).map { case (a, p) => (a, p :+ k) })
  }

  /**
   * The key of a lookup into a map with keys of 'keyType', as a path element, if the key expression
   * is foldable (a constant under Catalyst's definition): `m['a']`, `m[concat('a', '')]` and
   * `m[upper('a')]` qualify, a key that depends on the input row does not. The element kind follows
   * the map's key type, which is what Velox checks the subfield against: string keys become StrKey,
   * integral keys LongKey, any other key type (date, decimal, ...) has no Velox subscript and
   * yields None.
   */
  private def constantKeyElement(key: Expression, keyType: DataType): Option[Elem] = {
    if (!key.foldable) {
      return None
    }
    val value =
      try key.eval(EmptyRow)
      catch { case NonFatal(_) => return None }
    if (value == null) {
      return None
    }
    keyType match {
      case StringType =>
        // The key is compared as bytes by Spark and by the reader, but travels as a string.
        // Invalid UTF-8 (a key built from a binary cast) does not survive the round trip, so
        // such a key cannot be declared.
        val bytes = value.asInstanceOf[UTF8String].getBytes
        val rendered = value.toString
        if (java.util.Arrays.equals(rendered.getBytes(StandardCharsets.UTF_8), bytes)) {
          Some(StrKey(rendered))
        } else {
          None
        }
      case ByteType | ShortType | IntegerType | LongType =>
        Some(LongKey(value.asInstanceOf[Number].longValue()))
      case _ => None
    }
  }

  private def hasKeyLookup(path: Seq[Elem]): Boolean =
    path.exists(e => e.isInstanceOf[StrKey] || e.isInstanceOf[LongKey])

  /**
   * Renders one attribute's paths as [[SubfieldPath]]s. Column and field names go through
   * ConverterUtils.normalizeColName, exactly like the names of the scan's Substrait schema do when
   * the plan is built, so the declaration and the schema always agree, including for non-ASCII
   * names that only Java's full lowercasing changes. Transport is structural, so no character of a
   * name or key needs quoting; toString on the result gives the Velox Subfield syntax, for example
   * `c.m["k1"].f1`.
   */
  private def renderSubfieldPaths(
      attr: Attribute,
      paths: Seq[List[Elem]]): (String, Seq[SubfieldPath]) = {
    val colName = ConverterUtils.normalizeColName(attr.name)
    val rendered = paths.map {
      p =>
        SubfieldPath(
          colName,
          p.map {
            case Field(n) => Field(ConverterUtils.normalizeColName(n))
            case other => other
          })
    }
    (colName, rendered)
  }
}
