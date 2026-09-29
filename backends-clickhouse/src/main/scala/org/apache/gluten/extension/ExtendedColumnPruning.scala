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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.GeneratorNestedColumnAliasing.canPruneGenerator
import org.apache.spark.sql.catalyst.optimizer.NestedColumnAliasing
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object ExtendedGeneratorNestedColumnAliasing {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case pj @ Project(projectList, f @ Filter(condition, g: Generate))
        if canPruneGenerator(g.generator) &&
          GlutenConfig.get.enableExtendedColumnPruning &&
          (SQLConf.get.nestedPruningOnExpressions || SQLConf.get.nestedSchemaPruningEnabled) =>
      val attrToExtractValues = NestedColumnAliasing
        .getAttributeToExtractValues(projectList ++ g.generator.children :+ condition, Seq.empty)
      if (attrToExtractValues.isEmpty) {
        return None
      }

      val generatorOutputSet = AttributeSet(g.qualifiedGeneratorOutput)
      var (attrToExtractValuesOnGenerator, attrToExtractValuesNotOnGenerator) =
        attrToExtractValues.partition {
          case (attr, _) =>
            attr.references.subsetOf(generatorOutputSet)
        }

      val pushedThrough = rewritePlanWithAliases(pj, attrToExtractValuesNotOnGenerator)

      // We cannot push through if the child of generator is `MapType`.
      g.generator.children.head.dataType match {
        case _: MapType => return Some(pushedThrough)
        case _ =>
      }

      if (!g.generator.isInstanceOf[ExplodeBase]) {
        return Some(pushedThrough)
      }

      // Use a Gluten-specific extractor to collect nested `GetStructField`s over generator
      // outputs; the vanilla extractor only surfaces top-level [[ExtractValue]]s.
      attrToExtractValuesOnGenerator = NestedColumnAliasing.getAttributeToExtractValues(
        attrToExtractValuesOnGenerator.flatMap(_._2).toSeq,
        Seq.empty,
        collectNestedGetStructFields)

      val nestedFieldsOnGenerator = attrToExtractValuesOnGenerator.values.flatten.toSet
      if (nestedFieldsOnGenerator.isEmpty) {
        return Some(pushedThrough)
      }

      // Multiple or single nested column accessors.
      // E.g. df.select(explode($"items").as("item")).select($"item.a", $"item.b")
      pushedThrough match {
        case p2 @ Project(_, f2 @ Filter(_, g2: Generate)) =>
          val nestedFieldsOnGeneratorSeq = nestedFieldsOnGenerator.toSeq
          val nestedFieldToOrdinal = nestedFieldsOnGeneratorSeq.zipWithIndex.toMap
          val rewrittenG = g2.transformExpressions {
            case e: ExplodeBase =>
              val extractors = nestedFieldsOnGeneratorSeq.map(replaceGenerator(e, _))
              val names = extractors.map {
                case g: GetStructField => Literal(g.extractFieldName)
                case ga: GetArrayStructFields => Literal(ga.field.name)
                case other =>
                  throw new IllegalStateException(
                    s"Unreasonable extractor " +
                      "after replaceGenerator: $other")
              }
              val zippedArray = ArraysZip(extractors, names)
              e.withNewChildren(Seq(zippedArray))
          }
          // As we change the child of the generator, its output data type must be updated.
          val updatedGeneratorOutput = rewrittenG.generatorOutput
            .zip(
              rewrittenG.generator.elementSchema.map(
                f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
            .map {
              case (oldAttr, newAttr) =>
                newAttr.withExprId(oldAttr.exprId).withName(oldAttr.name)
            }
          assert(
            updatedGeneratorOutput.length == rewrittenG.generatorOutput.length,
            "Updated generator output must have the same length " +
              "with original generator output."
          )
          val updatedGenerate = rewrittenG.copy(generatorOutput = updatedGeneratorOutput)

          // Replace nested column accessor with generator output.
          val attrExprIdsOnGenerator = attrToExtractValuesOnGenerator.keys.map(_.exprId).toSet
          val updatedFilter = f2.withNewChildren(Seq(updatedGenerate)).transformExpressions {
            case f: GetStructField if nestedFieldsOnGenerator.contains(f) =>
              replaceGetStructField(
                f,
                updatedGenerate.output,
                attrExprIdsOnGenerator,
                nestedFieldToOrdinal)
          }

          val updatedProject = p2.withNewChildren(Seq(updatedFilter)).transformExpressions {
            case f: GetStructField if nestedFieldsOnGenerator.contains(f) =>
              replaceGetStructField(
                f,
                updatedFilter.output,
                attrExprIdsOnGenerator,
                nestedFieldToOrdinal)
          }

          Some(updatedProject)
        case other =>
          throw new IllegalStateException(s"Unreasonable plan after optimization: $other")
      }
    case _ =>
      None
  }

  private def replaceGetStructField(
      g: GetStructField,
      input: Seq[Attribute],
      attrExprIdsOnGenerator: Set[ExprId],
      nestedFieldToOrdinal: Map[ExtractValue, Int]): Expression = {
    val attr = input.find(a => attrExprIdsOnGenerator.contains(a.exprId))
    attr match {
      case Some(a) =>
        val ordinal = nestedFieldToOrdinal(g)
        GetStructField(a, ordinal, g.name)
      case None => g
    }
  }

  /** Replace the reference attribute of extractor expression with generator input. */
  private def replaceGenerator(generator: ExplodeBase, expr: Expression): Expression = {
    expr match {
      case a: Attribute if expr.references.contains(a) =>
        generator.child
      case g: GetStructField =>
        // We cannot simply do a transformUp instead because if we replace the attribute
        // `extractFieldName` could cause `ClassCastException` error. We need to get the
        // field name before replacing down the attribute/other extractor.
        val fieldName = g.extractFieldName
        val newChild = replaceGenerator(generator, g.child)
        ExtractValue(newChild, Literal(fieldName), SQLConf.get.resolver)
      case other =>
        other.mapChildren(replaceGenerator(generator, _))
    }
  }

  // This function collects all GetStructField*(attribute) from the passed in expression.
  // GetStructField* means arbitrary levels of nesting.
  private def collectNestedGetStructFields(e: Expression): Seq[Expression] = {
    // The helper function returns a tuple of
    // (nested GetStructField including the current level, all other nested GetStructField)
    def helper(e: Expression): (Seq[Expression], Seq[Expression]) = e match {
      case _: AttributeReference => (Seq(e), Seq.empty)
      case gsf: GetStructField =>
        val child_res = helper(gsf.child)
        (child_res._1.map(p => gsf.withNewChildren(Seq(p))), child_res._2)
      case other =>
        val child_res = other.children.map(helper)
        val child_res_combined = (child_res.flatMap(_._1), child_res.flatMap(_._2))
        (Seq.empty, child_res_combined._1 ++ child_res_combined._2)
    }

    val res = helper(e)
    (res._1 ++ res._2).filterNot(_.isInstanceOf[Attribute])
  }

  private def rewritePlanWithAliases(
      plan: LogicalPlan,
      attributeToExtractValues: Map[Attribute, Seq[ExtractValue]]): LogicalPlan = {
    val attributeToExtractValuesAndAliases =
      attributeToExtractValues.map {
        case (attr, evSeq) =>
          val evAliasSeq = evSeq.map {
            ev =>
              val fieldName = ev match {
                case g: GetStructField => g.extractFieldName
                case g: GetArrayStructFields => g.field.name
              }
              ev -> Alias(ev, s"_extract_$fieldName")()
          }

          attr -> evAliasSeq
      }

    val nestedFieldToAlias = attributeToExtractValuesAndAliases.values.flatten.map {
      case (field, alias) => field.canonicalized -> alias
    }.toMap

    // A reference attribute can have multiple aliases for nested fields.
    val attrToAliases =
      AttributeMap(attributeToExtractValuesAndAliases.mapValues(_.map(_._2)).toSeq)

    plan match {
      // Project(Filter(Generate))
      case Project(projectList, f @ Filter(condition, g: Generate)) =>
        val newProjectList = NestedColumnAliasing.getNewProjectList(projectList, nestedFieldToAlias)
        val newCondition = getNewExpression(condition, nestedFieldToAlias)
        val newGenerator = getNewExpression(g.generator, nestedFieldToAlias).asInstanceOf[Generator]

        val tmpG = NestedColumnAliasing
          .replaceWithAliases(g, nestedFieldToAlias, attrToAliases)
          .asInstanceOf[Generate]
        val newG = Generate(
          newGenerator,
          tmpG.unrequiredChildIndex,
          tmpG.outer,
          tmpG.qualifier,
          tmpG.generatorOutput,
          tmpG.children.head)
        val newF = Filter(newCondition, newG)
        val newP = Project(newProjectList, newF)
        newP
      case _ => plan
    }
  }

  private def getNewExpression(
      expr: Expression,
      nestedFieldToAlias: Map[Expression, Alias]): Expression = {
    expr.transform {
      case f: ExtractValue if nestedFieldToAlias.contains(f.canonicalized) =>
        nestedFieldToAlias(f.canonicalized).toAttribute
    }
  }
}

// ExtendedColumnPruning process Project(Filter(Generate)),
// which is ignored by vanilla spark in optimization rule: ColumnPruning
case class ExtendedColumnPruning(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(AlwaysProcess.fn) {
      case ExtendedGeneratorNestedColumnAliasing(rewrittenPlan) => rewrittenPlan
      case p => p
    }
}
