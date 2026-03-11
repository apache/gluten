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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.test.TestStats
import org.apache.gluten.utils.DecimalArithmeticUtil

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{StringTrimBoth, _}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke, StructsToJsonInvoke}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.hive.HiveUDFTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

trait Transformable {
  def getTransformer(childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer
}

object ExpressionConverter extends SQLConfHelper with Logging {

  def replaceWithExpressionTransformer(
      exprs: Seq[Expression],
      attributeSeq: Seq[Attribute]): Seq[ExpressionTransformer] = {
    val expressionsMap = ExpressionMappings.expressionsMap
    exprs.map(expr => replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap))
  }

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val expressionsMap = ExpressionMappings.expressionsMap
    replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap)
  }

  def canReplaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): Boolean = {
    try {
      replaceWithExpressionTransformer(expr, attributeSeq)
      true
    } catch {
      case e: Exception =>
        logInfo(e.getMessage)
        false
    }
  }

  def replaceAttributeReference(expr: Expression): Expression = expr match {
    case ar: AttributeReference if ar.dataType == BooleanType =>
      EqualNullSafe(ar, Literal.TrueLiteral)
    case e => e
  }

  private def replacePythonUDFWithExpressionTransformer(
      udf: PythonUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    val substraitExprName = UDFMappings.pythonUDFMap.get(udf.name)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported python udf: $udf.")
    }
  }

  private def replaceScalaUDFWithExpressionTransformer(
      udf: ScalaUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    if (udf.udfName.isEmpty) {
      throw new GlutenNotSupportException("UDF name is not found!")
    }
    val substraitExprName = UDFMappings.scalaUDFMap.get(udf.udfName.get)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported scala udf: $udf.")
    }
  }

  private def replaceFlattenedExpressionWithExpressionTransformer(
      substraitName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    val children =
      expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
    BackendsApiManager.getSparkPlanExecApiInstance.genFlattenedExpressionTransformer(
      substraitName,
      children,
      expr)
  }

  private def genRescaleDecimalTransformer(
      substraitName: String,
      b: BinaryArithmetic,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): DecimalArithmeticExpressionTransformer = {
    val rescaleBinary = DecimalArithmeticUtil.rescaleLiteral(b)
    val (left, right) = DecimalArithmeticUtil.rescaleCastForDecimal(
      DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.left),
      DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.right))
    val resultType = DecimalArithmeticUtil.getResultType(
      b,
      left.dataType.asInstanceOf[DecimalType],
      right.dataType.asInstanceOf[DecimalType]
    )

    val leftChild =
      replaceWithExpressionTransformer0(left, attributeSeq, expressionsMap)
    val rightChild =
      replaceWithExpressionTransformer0(right, attributeSeq, expressionsMap)
    DecimalArithmeticExpressionTransformer(substraitName, leftChild, rightChild, resultType, b)
  }

  // Mapping for Iceberg static invoke functions
  private val icebergStaticInvokeMap = Map(
    "BucketFunction" -> ExpressionNames.BUCKET,
    "TruncateFunction" -> ExpressionNames.TRUNCATE,
    "YearsFunction" -> ExpressionNames.YEARS,
    "MonthsFunction" -> ExpressionNames.MONTHS,
    "DaysFunction" -> ExpressionNames.DAYS,
    "HoursFunction" -> ExpressionNames.HOURS
  )

  // Mapping for other static invoke functions
  private val staticInvokeMap = Map(
    "varcharTypeWriteSideCheck" -> ExpressionNames.VARCHAR_TYPE_WRITE_SIDE_CHECK,
    "charTypeWriteSideCheck" -> ExpressionNames.CHAR_TYPE_WRITE_SIDE_CHECK,
    "readSidePadding" -> ExpressionNames.READ_SIDE_PADDING,
    "lengthOfJsonArray" -> ExpressionNames.JSON_ARRAY_LENGTH,
    "jsonObjectKeys" -> ExpressionNames.JSON_OBJECT_KEYS
  )

  private def replaceStaticInvokeWithExpressionTransformer(
      i: StaticInvoke,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {

    val objName = i.objectName
    val funcName = i.functionName

    def doTransform(child: Expression): ExpressionTransformer =
      replaceWithExpressionTransformer0(child, attributeSeq, expressionsMap)

    def validateAndTransform(
        exprName: String,
        childTransformers: => Seq[ExpressionTransformer]): ExpressionTransformer = {
      if (!BackendsApiManager.getValidatorApiInstance.doExprValidate(exprName, i)) {
        throw new GlutenNotSupportException(
          s"Not supported to map current ${i.getClass} call on function: $funcName.")
      }
      GenericExpressionTransformer(exprName, childTransformers, i)
    }

    // Try to match Iceberg static invoke first
    val icebergTransformer: Option[ExpressionTransformer] =
      if (funcName == "invoke") {
        icebergStaticInvokeMap.collectFirst {
          case (func, name) if objName.startsWith("org.apache.iceberg.spark.functions." + func) =>
            GenericExpressionTransformer(name, i.arguments.map(doTransform), i)
        }
      } else {
        None
      }

    icebergTransformer.getOrElse {
      funcName match {
        case "isLuhnNumber" =>
          validateAndTransform(ExpressionNames.LUHN_CHECK, Seq(doTransform(i.arguments.head)))

        case fn @ ("encode" | "decode") if objName.endsWith("UrlCodec") =>
          validateAndTransform("url_" + fn, Seq(doTransform(i.arguments.head)))

        case fn @ ("encode" | "decode")
            if objName.endsWith("Base64") && BackendsApiManager.getValidatorApiInstance
              .doExprValidate(ExpressionNames.BASE64, i) =>
          BackendsApiManager.getSparkPlanExecApiInstance.genBase64StaticInvokeTransformer(
            ExpressionNames.BASE64,
            doTransform(i.arguments.head),
            i
          )

        case fn if staticInvokeMap.contains(fn) =>
          validateAndTransform(staticInvokeMap(fn), i.arguments.map(doTransform))

        case _ =>
          throw new GlutenNotSupportException(
            s"Not supported to transform StaticInvoke with object: $objName, function: $funcName")
      }
    }
  }

  private def replaceInvokeWithExpressionTransformer(
      invoke: Invoke,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {

    // Pattern matching for different Invoke types
    invoke match {
      // StructsToJson evaluator
      case StructsToJsonInvoke(options, child, timeZoneId) =>
        val toJsonExpr = StructsToJson(options, child, timeZoneId)
        val substraitExprName = getAndCheckSubstraitName(toJsonExpr, expressionsMap)
        BackendsApiManager.getSparkPlanExecApiInstance.genToJsonTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(child, attributeSeq, expressionsMap),
          toJsonExpr
        )

      // Unsupported invoke
      case _ =>
        throw new GlutenNotSupportException(
          s"Not supported to transform Invoke with function: $invoke")
    }
  }

  private def replaceWithExpressionTransformer0(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    logDebug(
      s"replaceWithExpressionTransformer expr: $expr class: ${expr.getClass} " +
        s"name: ${expr.prettyName}")

    tryTransformWithoutExpressionMapping(expr, attributeSeq, expressionsMap).getOrElse {
      val substraitExprName: String = getAndCheckSubstraitName(expr, expressionsMap)
      val backendConverted = BackendsApiManager.getSparkPlanExecApiInstance
        .extraExpressionConverter(substraitExprName, expr, attributeSeq)

      backendConverted.getOrElse(
        transformExpression(expr, attributeSeq, expressionsMap, substraitExprName))
    }
  }

  /**
   * Transform expressions that don't have direct expression class mapping in expressionsMap. This
   * handles special cases like UDFs (Python, Scala, Hive), StaticInvoke, and Invoke expressions,
   * where the transformation logic is based on runtime information rather than expression class
   * type.
   *
   * @param expr
   *   The expression to transform
   * @param attributeSeq
   *   The sequence of attributes for binding
   * @param expressionsMap
   *   The expression class to substrait name mapping (not used for these cases)
   * @return
   *   Some(ExpressionTransformer) if the expression matches one of these special cases, None
   *   otherwise
   */
  private def tryTransformWithoutExpressionMapping(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): Option[ExpressionTransformer] = {
    Option {
      expr match {
        case pythonUDF: PythonUDF =>
          replacePythonUDFWithExpressionTransformer(pythonUDF, attributeSeq, expressionsMap)
        case scalaUDF: ScalaUDF =>
          replaceScalaUDFWithExpressionTransformer(scalaUDF, attributeSeq, expressionsMap)
        case _ if HiveUDFTransformer.isHiveUDF(expr) =>
          BackendsApiManager.getSparkPlanExecApiInstance.genHiveUDFTransformer(expr, attributeSeq)
        case staticInvoke: StaticInvoke =>
          replaceStaticInvokeWithExpressionTransformer(staticInvoke, attributeSeq, expressionsMap)
        case invoke: Invoke =>
          replaceInvokeWithExpressionTransformer(invoke, attributeSeq, expressionsMap)
        case _ =>
          null
      }
    }
  }

  private def transformExpression(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String],
      substraitExprName: String): ExpressionTransformer = {
    expr match {
      case c: CreateArray =>
        val children =
          c.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        CreateArrayTransformer(substraitExprName, children, c)
      case g: GetArrayItem =>
        BackendsApiManager.getSparkPlanExecApiInstance.genGetArrayItemTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(g.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(g.right, attributeSeq, expressionsMap),
          g
        )
      case c: CreateMap =>
        val children =
          c.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        CreateMapTransformer(substraitExprName, children, c)
      case g: GetMapValue =>
        BackendsApiManager.getSparkPlanExecApiInstance.genGetMapValueTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(g.child, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(g.key, attributeSeq, expressionsMap),
          g
        )
      case m: MapEntries =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMapEntriesTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(m.child, attributeSeq, expressionsMap),
          m)
      case e: Explode =>
        ExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(e.child, attributeSeq, expressionsMap),
          e)
      case p: PosExplode =>
        BackendsApiManager.getSparkPlanExecApiInstance.genPosExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(p.child, attributeSeq, expressionsMap),
          p,
          attributeSeq)
      case i: Inline =>
        BackendsApiManager.getSparkPlanExecApiInstance.genInlineTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.child, attributeSeq, expressionsMap),
          i)
      case a: Alias =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAliasTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.child, attributeSeq, expressionsMap),
          a)
      case a: AttributeReference =>
        if (attributeSeq == null) {
          throw new UnsupportedOperationException("attributeSeq should not be null.")
        }
        val input = AttributeSeq(attributeSeq)
        if (input.indexOf(a.exprId) == -1) {
          // This situation may need developers to fix, although we just throw the below
          // exception to let the corresponding operator fall back.
          throw new UnsupportedOperationException(
            "Failed to bind reference: " +
              s"couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        }
        val b = BindReferences
          .bindReference(expr, input, allowFailures = false)
          .asInstanceOf[BoundReference]
        AttributeReferenceTransformer(substraitExprName, a, b)
      case b: BoundReference =>
        BoundReferenceTransformer(substraitExprName, b)
      case l: Literal =>
        LiteralTransformer(l)
      case d: DateDiff =>
        BackendsApiManager.getSparkPlanExecApiInstance.genDateDiffTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(d.endDate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(d.startDate, attributeSeq, expressionsMap),
          d
        )
      case r: Round if r.child.dataType.isInstanceOf[DecimalType] =>
        DecimalRoundTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(r.child, attributeSeq, expressionsMap),
          r)
      case t: ToUnixTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genToUnixTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(t.timeExp, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(t.format, attributeSeq, expressionsMap),
          t
        )
      case u: UnixTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genToUnixTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(u.timeExp, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(u.format, attributeSeq, expressionsMap),
          u
        )
      case t: TruncTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTruncTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(t.format, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(t.timestamp, attributeSeq, expressionsMap),
          t.timeZoneId,
          t
        )
      case m: MonthsBetween =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMonthsBetweenTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(m.date1, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(m.date2, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(m.roundOff, attributeSeq, expressionsMap),
          m
        )
      case i: If =>
        IfTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.predicate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(i.trueValue, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(i.falseValue, attributeSeq, expressionsMap),
          i
        )
      case cw: CaseWhen =>
        CaseWhenTransformer(
          substraitExprName,
          cw.branches.map {
            expr =>
              {
                (
                  replaceWithExpressionTransformer0(expr._1, attributeSeq, expressionsMap),
                  replaceWithExpressionTransformer0(expr._2, attributeSeq, expressionsMap))
              }
          },
          cw.elseValue.map {
            expr =>
              {
                replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap)
              }
          },
          cw
        )
      case i: In =>
        if (i.list.exists(!_.foldable)) {
          throw new GlutenNotSupportException(
            s"In list option does not support non-foldable expression, ${i.list.map(_.sql)}")
        }
        InTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.value, attributeSeq, expressionsMap),
          i)
      case i: InSet =>
        InSetTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.child, attributeSeq, expressionsMap),
          i)
      case s: ScalarSubquery =>
        ScalarSubqueryTransformer(substraitExprName, s)
      case c: Cast =>
        // Add trim node, as necessary.
        val newCast =
          BackendsApiManager.getSparkPlanExecApiInstance.genCastWithNewChild(c)
        CastTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(newCast.child, attributeSeq, expressionsMap),
          newCast)
      case s: String2TrimExpression =>
        val (srcStr, trimStr) = s match {
          case StringTrim(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimLeft(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimRight(srcStr, trimStr) => (srcStr, trimStr)
        }
        val children = trimStr
          .map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
          .toSeq ++
          Seq(replaceWithExpressionTransformer0(srcStr, attributeSeq, expressionsMap))
        GenericExpressionTransformer(
          substraitExprName,
          children,
          s
        )
      case s: StringTrimBoth =>
        val children = s.trimStr
          .map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
          .toSeq ++
          Seq(replaceWithExpressionTransformer0(s.srcStr, attributeSeq, expressionsMap))
        GenericExpressionTransformer(
          substraitExprName,
          children,
          s
        )
      case m: HashExpression[_] =>
        BackendsApiManager.getSparkPlanExecApiInstance.genHashExpressionTransformer(
          substraitExprName,
          m.children.map(
            expr => replaceWithExpressionTransformer0(expr, attributeSeq, expressionsMap)),
          m)
      case getStructField: GetStructField =>
        try {
          val bindRef =
            bindGetStructField(getStructField, attributeSeq)
          // Different backends may have different result.
          BackendsApiManager.getSparkPlanExecApiInstance.genGetStructFieldTransformer(
            substraitExprName,
            replaceWithExpressionTransformer0(getStructField.child, attributeSeq, expressionsMap),
            bindRef.ordinal,
            getStructField)
        } catch {
          case e: IllegalStateException =>
            // This situation may need developers to fix, although we just throw the below
            // exception to let the corresponding operator fall back.
            throw new UnsupportedOperationException(
              s"Failed to bind reference for $getStructField: ${e.getMessage}")
        }

      case getArrayStructFields: GetArrayStructFields =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(
              getArrayStructFields.child,
              attributeSeq,
              expressionsMap),
            LiteralTransformer(getArrayStructFields.ordinal)),
          getArrayStructFields
        )
      case t: StringTranslate =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringTranslateTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(t.srcExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(t.matchingExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(t.replaceExpr, attributeSeq, expressionsMap),
          t
        )
      case r: RegExpReplace =>
        BackendsApiManager.getSparkPlanExecApiInstance.genRegexpReplaceTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(r.subject, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(r.regexp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(r.rep, attributeSeq, expressionsMap),
            replaceWithExpressionTransformer0(r.pos, attributeSeq, expressionsMap)
          ),
          r
        )
      case size: Size =>
        // Covers Spark ArraySize which is replaced by Size(child, false).
        val child =
          replaceWithExpressionTransformer0(size.child, attributeSeq, expressionsMap)
        GenericExpressionTransformer(
          substraitExprName,
          Seq(child, LiteralTransformer(size.legacySizeOfNull)),
          size)
      case namedStruct: CreateNamedStruct =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNamedStructTransformer(
          substraitExprName,
          namedStruct.children.map(
            replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          namedStruct,
          attributeSeq)
      case namedLambdaVariable: NamedLambdaVariable =>
        // namedlambdavariable('acc')-> <Integer, notnull>
        GenericExpressionTransformer(
          substraitExprName,
          LiteralTransformer(namedLambdaVariable.name),
          namedLambdaVariable
        )
      case lambdaFunction: LambdaFunction =>
        LambdaFunctionTransformer(
          substraitExprName,
          function = replaceWithExpressionTransformer0(
            lambdaFunction.function,
            attributeSeq,
            expressionsMap),
          arguments = lambdaFunction.arguments.map(
            replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          original = lambdaFunction
        )
      case j: JsonTuple =>
        val children =
          j.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        JsonTupleExpressionTransformer(substraitExprName, children, j)
      case l: Like =>
        BackendsApiManager.getSparkPlanExecApiInstance.genLikeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(l.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(l.right, attributeSeq, expressionsMap),
          l
        )
      case m: MakeDecimal =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(m.child, attributeSeq, expressionsMap),
            LiteralTransformer(m.nullOnOverflow)),
          m
        )
      case PromotePrecision(_ @Cast(child, _: DecimalType, _, _))
          if child.dataType
            .isInstanceOf[DecimalType] && !BackendsApiManager.getSettings.transformCheckOverflow =>
        replaceWithExpressionTransformer0(child, attributeSeq, expressionsMap)
      case _: NormalizeNaNAndZero | _: PromotePrecision | _: TaggingExpression |
          _: DynamicPruningExpression =>
        ChildTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(expr.children.head, attributeSeq, expressionsMap),
          expr
        )
      case _: GetDateField | _: GetTimeField =>
        ExtractDateTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(expr.children.head, attributeSeq, expressionsMap),
          expr)
      case _: StringToMap =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringToMapTransformer(
          substraitExprName,
          expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          expr)
      case CheckOverflow(b: BinaryArithmetic, decimalType, _)
          if !BackendsApiManager.getSettings.transformCheckOverflow &&
            DecimalArithmeticUtil.isDecimalArithmetic(b) =>
        val arithmeticExprName =
          BackendsApiManager.getSparkPlanExecApiInstance.getDecimalArithmeticExprName(
            getAndCheckSubstraitName(b, expressionsMap))
        val left =
          replaceWithExpressionTransformer0(b.left, attributeSeq, expressionsMap)
        val right =
          replaceWithExpressionTransformer0(b.right, attributeSeq, expressionsMap)
        DecimalArithmeticExpressionTransformer(arithmeticExprName, left, right, decimalType, b)
      case c: CheckOverflow =>
        CheckOverflowTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(c.child, attributeSeq, expressionsMap),
          c)
      case c if c.getClass.getSimpleName.equals("CheckOverflowInTableInsert") =>
        throw new GlutenNotSupportException(
          "CheckOverflowInTableInsert is used in ANSI mode, but Gluten does not support ANSI mode."
        )
      case b: BinaryArithmetic if DecimalArithmeticUtil.isDecimalArithmetic(b) =>
        val exprName = BackendsApiManager.getSparkPlanExecApiInstance.getDecimalArithmeticExprName(
          substraitExprName)
        if (!BackendsApiManager.getSettings.transformCheckOverflow) {
          GenericExpressionTransformer(
            exprName,
            expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
            expr
          )
        } else {
          // Without the rescale and remove cast, result is right for high version Spark,
          // but performance regression in velox
          genRescaleDecimalTransformer(exprName, b, attributeSeq, expressionsMap)
        }
      case n: NaNvl =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNaNvlTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(n.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(n.right, attributeSeq, expressionsMap),
          n
        )
      case a: AtLeastNNonNulls =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAtLeastNNonNullsTransformer(
          substraitExprName,
          a.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          a
        )
      case m: MakeTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMakeTimestampTransformer(
          substraitExprName,
          m.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          m)
      case tsAdd: BinaryExpression if tsAdd.getClass.getSimpleName.equals("TimestampAdd") =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTimestampAddTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(tsAdd.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(tsAdd.right, attributeSeq, expressionsMap),
          tsAdd
        )
      case tsDiff: BinaryExpression if tsDiff.getClass.getSimpleName.equals("TimestampDiff") =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTimestampDiffTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(tsDiff.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(tsDiff.right, attributeSeq, expressionsMap),
          tsDiff
        )
      case e: Transformable =>
        val childrenTransformers =
          e.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap))
        e.getTransformer(childrenTransformers)
      case u: Uuid =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUuidTransformer(substraitExprName, u)
      case f: ArrayFilter =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayFilterTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(f.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(f.function, attributeSeq, expressionsMap),
          f
        )
      case arrayTransform: ArrayTransform =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayTransformTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(arrayTransform.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(arrayTransform.function, attributeSeq, expressionsMap),
          arrayTransform
        )
      case arraySort: ArraySort =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArraySortTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(arraySort.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(arraySort.function, attributeSeq, expressionsMap),
          arraySort
        )
      case tryEval @ TryEval(a: Add) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_ADD
        )
      case tryEval @ TryEval(a: Subtract) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_SUBTRACT
        )
      case tryEval @ TryEval(a: Divide) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_DIVIDE
        )
      case tryEval @ TryEval(a: Multiply) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECKED_MULTIPLY
        )
      case a: Add =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_ADD
        )
      case a: Subtract =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_SUBTRACT
        )
      case a: Multiply =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_MULTIPLY
        )
      case a: Divide =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECKED_DIVIDE
        )
      case i: IntegralDivide =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(i.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(i.right, attributeSeq, expressionsMap),
          i,
          ExpressionNames.CHECKED_DIV
        )
      case tryEval: TryEval =>
        // This is a placeholder to handle try_eval(other expressions).
        BackendsApiManager.getSparkPlanExecApiInstance.genTryEvalTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(tryEval.child, attributeSeq, expressionsMap),
          tryEval
        )
      case a: ArrayForAll =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayForAllTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.function, attributeSeq, expressionsMap),
          a
        )
      case a: ArrayExists =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayExistsTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(a.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(a.function, attributeSeq, expressionsMap),
          a
        )
      case arrayInsert if arrayInsert.getClass.getSimpleName.equals("ArrayInsert") =>
        // Since spark 3.4.0
        val children = SparkShimLoader.getSparkShims.extractExpressionArrayInsert(arrayInsert)
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayInsertTransformer(
          substraitExprName,
          children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          arrayInsert
        )
      case s: Shuffle =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformer0(s.child, attributeSeq, expressionsMap),
            LiteralTransformer(Literal(s.randomSeed.get))),
          s)
      case c: PreciseTimestampConversion =>
        BackendsApiManager.getSparkPlanExecApiInstance.genPreciseTimestampConversionTransformer(
          substraitExprName,
          Seq(replaceWithExpressionTransformer0(c.child, attributeSeq, expressionsMap)),
          c
        )
      case t: TransformKeys =>
        // default is `EXCEPTION`
        val mapKeyDedupPolicy = SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)

        // Calling `.toString` on both sides ensures compatibility across all Spark versions.
        // Starting from Spark 4.1, `SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)` returns
        // an enum instead of a String. Without `.toString`, the comparison
        // `mapKeyDedupPolicy == SQLConf.MapKeyDedupPolicy.LAST_WIN.toString` would silently fail
        // in tests, producing only a "Comparing unrelated types" warning in IntelliJ IDEA,
        // but no compile-time error.
        if (mapKeyDedupPolicy.toString == SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
          // TODO: Remove after fix ready for
          //  https://github.com/facebookincubator/velox/issues/10219
          throw new GlutenNotSupportException(
            "LAST_WIN policy is not supported yet in native to deduplicate map keys"
          )
        }
        GenericExpressionTransformer(
          substraitExprName,
          t.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          t
        )
      case e: EulerNumber =>
        LiteralTransformer(Literal(Math.E))
      case p: Pi =>
        LiteralTransformer(Literal(Math.PI))
      case v: SparkVersion =>
        LiteralTransformer(SPARK_VERSION_SHORT + " " + SPARK_REVISION)
      case dateAdd: DateAdd =>
        BackendsApiManager.getSparkPlanExecApiInstance.genDateAddTransformer(
          attributeSeq,
          substraitExprName,
          dateAdd.children,
          dateAdd
        )
      case ss: StringSplit =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringSplitTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(ss.str, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(ss.regex, attributeSeq, expressionsMap),
          replaceWithExpressionTransformer0(ss.limit, attributeSeq, expressionsMap),
          ss
        )
      case j: JsonToStructs =>
        BackendsApiManager.getSparkPlanExecApiInstance.genFromJsonTransformer(
          substraitExprName,
          expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          j)
      case s: StructsToJson =>
        BackendsApiManager.getSparkPlanExecApiInstance.genToJsonTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(s.child, attributeSeq, expressionsMap),
          s
        )
      case u: UnBase64 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUnbase64Transformer(
          substraitExprName,
          replaceWithExpressionTransformer0(u.child, attributeSeq, expressionsMap),
          u
        )
      case ce if BackendsApiManager.getSparkPlanExecApiInstance.expressionFlattenSupported(ce) =>
        replaceFlattenedExpressionWithExpressionTransformer(
          substraitExprName,
          ce,
          attributeSeq,
          expressionsMap)
      case re: RaiseError =>
        val errorMessage =
          BackendsApiManager.getSparkPlanExecApiInstance.getErrorMessage(re)
        GenericExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformer0(errorMessage, attributeSeq, expressionsMap),
          re)
      case expr =>
        GenericExpressionTransformer(
          substraitExprName,
          expr.children.map(replaceWithExpressionTransformer0(_, attributeSeq, expressionsMap)),
          expr
        )
    }
  }

  private def getAndCheckSubstraitName(
      expr: Expression,
      expressionsMap: Map[Class[_], String]): String = {
    TestStats.addExpressionClassName(expr.getClass.getName)
    // Check whether Gluten supports this expression
    expressionsMap
      .get(expr.getClass)
      .filter(BackendsApiManager.getValidatorApiInstance.doExprValidate(_, expr))
      .getOrElse {
        throw new GlutenNotSupportException(
          s"Not supported to map spark function name" +
            s" to substrait function name: $expr, class name: ${expr.getClass.getSimpleName}.")
      }
  }

  private def bindGetStructField(
      structField: GetStructField,
      input: AttributeSeq): BoundReference = {
    // get the new ordinal base input
    var newOrdinal: Int = -1
    val names = new ArrayBuffer[String]
    var root: Expression = structField
    while (root.isInstanceOf[GetStructField]) {
      val curField = root.asInstanceOf[GetStructField]
      val name = curField.childSchema.fields(curField.ordinal).name
      names += name
      root = root.asInstanceOf[GetStructField].child
    }
    // For map/array type, the reference is correct no matter NESTED_SCHEMA_PRUNING_ENABLED or not
    if (!root.isInstanceOf[AttributeReference]) {
      return BoundReference(structField.ordinal, structField.dataType, structField.nullable)
    }
    names += root.asInstanceOf[AttributeReference].name
    input.attrs.foreach(
      attribute => {
        var level = names.size - 1
        if (names(level) == attribute.name) {
          var candidateFields: Array[StructField] = null
          var dtType = attribute.dataType
          while (dtType.isInstanceOf[StructType] && level >= 1) {
            candidateFields = dtType.asInstanceOf[StructType].fields
            level -= 1
            val curName = names(level)
            for (i <- 0 until candidateFields.length) {
              if (candidateFields(i).name == curName) {
                dtType = candidateFields(i).dataType
                newOrdinal = i
              }
            }
          }
        }
      })
    if (newOrdinal == -1) {
      throw new IllegalStateException(
        s"Couldn't find $structField in ${input.attrs.mkString("[", ",", "]")}")
    } else {
      BoundReference(newOrdinal, structField.dataType, structField.nullable)
    }
  }
}
