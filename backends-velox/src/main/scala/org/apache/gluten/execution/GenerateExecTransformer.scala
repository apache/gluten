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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.GenerateExecTransformer.supportsGenerate
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}
import org.apache.gluten.utils.PullOutProjectHelper

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{GenerateExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

import com.google.protobuf.StringValue

import scala.collection.JavaConverters._
import scala.collection.mutable

case class GenerateExecTransformer(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    child: SparkPlan)
  extends GenerateExecTransformerBase(
    generator,
    requiredChildOutput,
    outer,
    generatorOutput,
    child) {

  @transient
  override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genGenerateTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genGenerateTransformerMetricsUpdater(metrics)

  override protected def withNewChildInternal(newChild: SparkPlan): GenerateExecTransformer =
    copy(generator, requiredChildOutput, outer, generatorOutput, newChild)

  override protected def doGeneratorValidate(
      generator: Generator,
      outer: Boolean): ValidationResult = {
    if (!supportsGenerate(generator)) {
      ValidationResult.failed(
        s"Velox backend does not support this generator: ${generator.getClass.getSimpleName}" +
          s", outer: $outer")
    } else {
      ValidationResult.succeeded
    }
  }

  override protected def getRelNode(
      context: SubstraitContext,
      inputRel: RelNode,
      generatorNode: ExpressionNode,
      validation: Boolean): RelNode = {
    val operatorId = context.nextOperatorId(this.nodeName)
    RelBuilder.makeGenerateRel(
      inputRel,
      generatorNode,
      requiredChildOutputNodes.asJava,
      getExtensionNode(validation),
      outer,
      context,
      operatorId)
  }

  /**
   * Is the specified expression an Attribute reference?
   * @param expr
   * @param replaceBoundReference
   * @return
   */
  private def isAttributeReference(
      expr: Expression,
      replaceBoundReference: Boolean = false): Boolean =
    expr match {
      case _: Attribute => true
      case _: BoundReference if !replaceBoundReference => true
      case _ => false
    }

  private def getExtensionNode(validation: Boolean): AdvancedExtensionNode = {
    if (!validation) {
      // Start with "GenerateParameters:"
      val parametersStr = new StringBuffer("GenerateParameters:")
      // isPosExplode: 1 for PosExplode, 0 for others.
      val isPosExplode = if (generator.isInstanceOf[PosExplode]) {
        "1"
      } else {
        "0"
      }
      parametersStr
        .append("isPosExplode=")
        .append(isPosExplode)
        .append("\n")

      // isStack: 1 for Stack, 0 for others.
      val isStack = generator.isInstanceOf[Stack]
      parametersStr
        .append("isStack=")
        .append(if (isStack) "1" else "0")
        .append("\n")

      // isOuter: 1 for outer, 0 for inner.
      parametersStr
        .append("isOuter=")
        .append(if (outer) "1" else "0")
        .append("\n")

      val injectProject = if (isStack) {
        // We always need to inject a Project for stack because we organize
        // stack's flat params into arrays, e.g. stack(2, 1, 2, 3) is
        // organized into two arrays: [1, 2] and [3, null].
        true
      } else {
        // Other generator function only have one param, so we just check whether
        // the only param(generator.children.head) is attribute reference or not.
        !isAttributeReference(generator.children.head, true);
      }

      parametersStr
        .append("injectedProject=")
        .append(if (injectProject) "1" else "0")
        .append("\n")

      val message = StringValue
        .newBuilder()
        .setValue(parametersStr.toString)
        .build()
      val optimization = BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
      ExtensionBuilder.makeAdvancedExtension(optimization, null)
    } else {
      getExtensionNodeForValidation
    }
  }
}

object GenerateExecTransformer {
  def supportsGenerate(generator: Generator): Boolean = {
    generator match {
      case _: Inline | _: ExplodeBase | _: JsonTuple | _: Stack =>
        true
      case _ =>
        false
    }
  }
}

object PullOutGenerateProjectHelper extends PullOutProjectHelper {
  def pullOutPreProject(generate: GenerateExec): SparkPlan = {
    if (GenerateExecTransformer.supportsGenerate(generate.generator)) {
      generate.generator match {
        case _: Inline | _: ExplodeBase =>
          val expressionMap = new mutable.HashMap[Expression, NamedExpression]()
          // The new child should be either the original Attribute,
          // or an Alias to other expressions.
          replaceExpressionWithAttribute(
            generate.generator.asInstanceOf[UnaryExpression].child,
            expressionMap,
            replaceBoundReference = true)

          if (!expressionMap.isEmpty) {
            // generator.child is not an Attribute reference, e.g Literal/CreateArray/CreateMap.
            // We plug in a Project to make it an Attribute reference.
            // NOTE: DO NOT use eliminateProjectList to create the project list because
            // newGeneratorChild can be a duplicated Attribute in generate.child.output. The native
            // side identifies the last field of projection as generator's input.
            val newGeneratorChildren = Seq(expressionMap.values.head)
            generate.copy(
              generator =
                generate.generator.withNewChildren(newGeneratorChildren).asInstanceOf[Generator],
              child = ProjectExec(generate.child.output ++ newGeneratorChildren, generate.child)
            )
          } else {
            // generator.child is Attribute, no need to introduce a Project.
            generate
          }
        case stack: Stack =>
          val numRows = stack.children.head.eval().asInstanceOf[Int]
          val numFields = Math.ceil((stack.children.size - 1.0) / numRows).toInt

          val newProjections = mutable.Buffer[NamedExpression]()
          val args = stack.children.tail

          // We organize stack's params as `numFields` arrays which will be feed
          // to Unnest operator on native side.
          for (field <- 0 until numFields) {
            val fieldArray = mutable.Buffer[Expression]()

            for (row <- 0 until numRows) {
              val index = row * numFields + field
              if (index < args.size) {
                fieldArray += args(index)
              } else {
                // Append nulls.
                fieldArray += Literal(null, args(field).dataType)
              }
            }

            newProjections += Alias(CreateArray(fieldArray.toSeq), generatePreAliasName)()
          }

          // Plug in a Project between Generate and its child.
          generate.copy(
            generator = generate.generator,
            child = ProjectExec(generate.child.output ++ newProjections, generate.child)
          )
        case JsonTuple(Seq(jsonObj, jsonPaths @ _*)) =>
          val getJsons: IndexedSeq[Expression] = {
            jsonPaths.map {
              case jsonPath if jsonPath.foldable =>
                Option(jsonPath.eval()) match {
                  case Some(path) =>
                    GetJsonObject(jsonObj, Literal.create("$[" + path + "]"))
                  case _ =>
                    Literal.create(null)
                }
              case jsonPath =>
                // Build bracket notation uniformly to
                // allow dot-containing field names in JSON paths, e.g., $[a.b]
                GetJsonObject(
                  jsonObj,
                  Concat(Seq(Literal.create("$["), jsonPath, Literal.create("]"))))
            }.toIndexedSeq
          }
          val preGenerateExprs =
            Alias(
              CreateArray(Seq(CreateStruct(getJsons))),
              generatePreAliasName
            )()
          // use JsonTupleExplode here instead of Explode so that we can distinguish
          // JsonTuple and Explode, because JsonTuple has an extra post-projection
          val newGenerator = JsonTupleExplode(preGenerateExprs.toAttribute)
          generate.copy(
            generator = newGenerator,
            child = ProjectExec(generate.child.output ++ Seq(preGenerateExprs), generate.child)
          )
        case _ =>
          // Unreachable.
          throw new IllegalStateException(
            s"Generator ${generate.generator.getClass.getSimpleName} is not supported.")
      }

    } else {
      generate
    }
  }

  def pullOutPostProject(generate: GenerateExec): SparkPlan = {
    if (GenerateExecTransformer.supportsGenerate(generate.generator)) {
      generate.generator match {
        case PosExplode(_) =>
          val originalOrdinal = generate.generatorOutput.head
          val ordinal = {
            val subtract = Subtract(Cast(originalOrdinal, IntegerType), Literal(1))
            Alias(subtract, generatePostAliasName)(
              originalOrdinal.exprId,
              originalOrdinal.qualifier)
          }

          if (generate.outer) {
            val isPresent =
              AttributeReference(generatePostAliasName, BooleanType, nullable = true)()

            val newOutput = (ordinal +: generate.generatorOutput.tail).map {
              attr =>
                val caseWhen = CaseWhen(
                  Seq((isPresent, attr)),
                  Literal(null, attr.dataType)
                )
                Alias(caseWhen, generatePostAliasName)(attr.exprId, attr.qualifier)
            }

            // Reorder the generatorOutput to match the output from the Unnest operator in Velox.
            val newGenerate = generate.copy(generatorOutput =
              generate.generatorOutput.tail :+ originalOrdinal :+ isPresent)

            ProjectExec(generate.requiredChildOutput ++ newOutput, newGenerate)
          } else {
            val newGenerate =
              generate.copy(generatorOutput = generate.generatorOutput.tail :+ originalOrdinal)
            ProjectExec(
              (generate.requiredChildOutput :+ ordinal) ++ generate.generatorOutput.tail,
              newGenerate)
          }
        case Inline(_) | JsonTupleExplode(_) =>
          val unnestOutput = {
            val struct = CreateStruct(generate.generatorOutput)
            val alias = Alias(struct, generatePostAliasName)()
            alias.toAttribute
          }
          if (generate.outer) {
            val isPresent =
              AttributeReference(generatePostAliasName, BooleanType, nullable = true)()
            val newGenerate = generate.copy(generatorOutput = Seq(unnestOutput) :+ isPresent)
            val newOutput = generate.generatorOutput.zipWithIndex.map {
              case (attr, i) =>
                val getStructField = GetStructField(unnestOutput, i, Some(attr.name))
                val caseWhen = CaseWhen(
                  Seq((isPresent, getStructField)),
                  Literal(null, getStructField.dataType)
                )
                Alias(caseWhen, generatePostAliasName)(attr.exprId, attr.qualifier)
            }
            ProjectExec(generate.requiredChildOutput ++ newOutput, newGenerate)
          } else {
            val newGenerate = generate.copy(generatorOutput = Seq(unnestOutput))
            val newOutput = generate.generatorOutput.zipWithIndex.map {
              case (attr, i) =>
                val getStructField = GetStructField(unnestOutput, i, Some(attr.name))
                Alias(getStructField, generatePostAliasName)(attr.exprId, attr.qualifier)
            }
            ProjectExec(generate.requiredChildOutput ++ newOutput, newGenerate)
          }
        case Explode(_) if generate.outer =>
          // Drop the last column of generatorOutput, which is the boolean representing whether
          // the null value is unnested from the input array/map (e.g. array(1, null)), or the
          // array/map itself is null or empty (e.g. array(), map(), null).
          val isPresent =
            AttributeReference(generatePostAliasName, BooleanType, nullable = true)()
          val newGenerate =
            generate.copy(generatorOutput = generate.generatorOutput :+ isPresent)
          val newOutput = generate.generatorOutput.map {
            attr =>
              val caseWhen = CaseWhen(
                Seq((isPresent, attr)),
                Literal(null, attr.dataType)
              )
              Alias(caseWhen, generatePostAliasName)(attr.exprId, attr.qualifier)
          }
          ProjectExec(generate.requiredChildOutput ++ newOutput, newGenerate)
        case _ => generate
      }
    } else {
      generate
    }
  }
}
