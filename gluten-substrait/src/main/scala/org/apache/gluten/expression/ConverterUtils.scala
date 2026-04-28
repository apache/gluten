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
import org.apache.gluten.substrait.`type`._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.CodedInputStream
import io.substrait.proto.Type

import java.util.{ArrayList => JArrayList, List => JList, Locale}

import scala.collection.JavaConverters._

case class ExpressionType(dataType: DataType, nullable: Boolean) {}

object ConverterUtils extends Logging {

  /**
   * Get the source Attribute for the input Expression. It will traverse the Expression tree in a
   * pre-order manner and find the first encountered Attribute. When encountering an Alias, it will
   * not continue to traverse its child but instead directly return the Attribute output by the
   * Alias.
   */
  def getAttrFromExpr(expr: Expression): Attribute = {
    expr.transformDown { case alias: Alias => alias.toAttribute }.references.head
  }

  def normalizeColName(name: String): String = {
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
  }

  def getShortAttributeName(attr: Attribute): String = {
    val name = normalizeColName(attr.name)
    val subIndex = name.indexOf("(")
    if (subIndex != -1) {
      name.substring(0, subIndex)
    } else {
      name
    }
  }

  def genColumnNameWithoutExprId(attr: Attribute): String = {
    getShortAttributeName(attr)
  }

  def genColumnNameWithExprId(attr: Attribute): String = {
    getShortAttributeName(attr) + "#" + attr.exprId.id
  }

  def collectAttributeTypeNodes(attributes: JList[Attribute]): JList[TypeNode] = {
    collectAttributeTypeNodes(attributes.asScala.toSeq)
  }

  def collectAttributeTypeNodes(attributes: Seq[Attribute]): JList[TypeNode] = {
    attributes.map(attr => getTypeNode(attr.dataType, attr.nullable)).asJava
  }

  def collectAttributeTypeNodes(structType: StructType): JList[TypeNode] = {
    structType.fields.map(f => getTypeNode(f.dataType, f.nullable)).toList.asJava
  }

  def collectAttributeNamesWithExprId(attributes: JList[Attribute]): JList[String] = {
    collectAttributeNamesWithExprId(attributes.asScala.toSeq)
  }

  def collectAttributeNamesWithExprId(attributes: Seq[Attribute]): JList[String] = {
    collectAttributeNamesDFS(attributes)(genColumnNameWithExprId)
  }

  // TODO: This is used only by `BasicScanExecTransformer`,
  //  perhaps we can remove this in the future and use `withExprId` version consistently.
  def collectAttributeNamesWithoutExprId(attributes: Seq[Attribute]): JList[String] = {
    collectAttributeNamesDFS(attributes)(attr => normalizeColName(attr.name))
  }

  def collectAttributeNames(attributes: Seq[Attribute]): JList[String] = {
    collectAttributeNamesDFS(attributes)(_.name)
  }

  private def collectAttributeNamesDFS(attributes: Seq[Attribute])(
      f: Attribute => String): JList[String] = {
    if (BackendsApiManager.getSettings.supportStructType()) {
      val nameList = new JArrayList[String]()
      attributes.foreach {
        attr =>
          nameList.add(f(attr))
          attr.dataType match {
            case struct: StructType =>
              val nestedNames = collectStructFieldNames(struct)
              nameList.addAll(nestedNames)
            case _ =>
          }
      }
      nameList
    } else {
      attributes.map(f(_)).asJava
    }
  }

  def collectStructFieldNames(dataType: DataType): JList[String] = {
    val nameList = new JArrayList[String]()
    dataType match {
      case structType: StructType =>
        structType.fields.foreach(
          field => {
            nameList.add(normalizeColName(field.name))
            val nestedNames = collectStructFieldNames(field.dataType)
            nameList.addAll(nestedNames)
          })
      case _ =>
    }
    nameList
  }

  def isNullable(nullability: Type.Nullability): Boolean = {
    nullability == Type.Nullability.NULLABILITY_NULLABLE
  }

  def parseFromSubstraitType(substraitType: Type): (DataType, Boolean) = {
    substraitType.getKindCase match {
      case Type.KindCase.BOOL =>
        (BooleanType, isNullable(substraitType.getBool.getNullability))
      case Type.KindCase.I8 =>
        (ByteType, isNullable(substraitType.getI8.getNullability))
      case Type.KindCase.I16 =>
        (ShortType, isNullable(substraitType.getI16.getNullability))
      case Type.KindCase.I32 =>
        (IntegerType, isNullable(substraitType.getI32.getNullability))
      case Type.KindCase.I64 =>
        (LongType, isNullable(substraitType.getI64.getNullability))
      case Type.KindCase.FP32 =>
        (FloatType, isNullable(substraitType.getFp32.getNullability))
      case Type.KindCase.FP64 =>
        (DoubleType, isNullable(substraitType.getFp64.getNullability))
      case Type.KindCase.STRING =>
        (StringType, isNullable(substraitType.getString.getNullability))
      case Type.KindCase.BINARY =>
        (BinaryType, isNullable(substraitType.getBinary.getNullability))
      case Type.KindCase.TIMESTAMP_TZ =>
        (TimestampType, isNullable(substraitType.getTimestampTz.getNullability))
      case Type.KindCase.DATE =>
        (DateType, isNullable(substraitType.getDate.getNullability))
      case Type.KindCase.DECIMAL =>
        val decimal = substraitType.getDecimal
        val precision = decimal.getPrecision
        val scale = decimal.getScale
        (DecimalType(precision, scale), isNullable(decimal.getNullability))
      case Type.KindCase.STRUCT =>
        val struct_ = substraitType.getStruct
        val fields = struct_.getTypesList.asScala.map {
          typ =>
            val (field, nullable) = parseFromSubstraitType(typ)
            StructField("", field, nullable)
        }
        (StructType(fields.toSeq), isNullable(substraitType.getStruct.getNullability))
      case Type.KindCase.LIST =>
        val list = substraitType.getList
        val (elementType, containsNull) = parseFromSubstraitType(list.getType)
        (ArrayType(elementType, containsNull), isNullable(substraitType.getList.getNullability))
      case Type.KindCase.MAP =>
        val map = substraitType.getMap
        val (keyType, _) = parseFromSubstraitType(map.getKey)
        val (valueType, valueContainsNull) = parseFromSubstraitType(map.getValue)
        (
          MapType(keyType, valueType, valueContainsNull),
          isNullable(substraitType.getMap.getNullability))
      case Type.KindCase.NOTHING =>
        (NullType, true)
      case unsupported =>
        throw new GlutenNotSupportException(s"Type $unsupported not supported.")
    }
  }

  def getTypeNode(datatype: DataType, nullable: Boolean): TypeNode = {
    datatype match {
      case BooleanType =>
        TypeBuilder.makeBoolean(nullable)
      case FloatType =>
        TypeBuilder.makeFP32(nullable)
      case DoubleType =>
        TypeBuilder.makeFP64(nullable)
      case LongType =>
        TypeBuilder.makeI64(nullable)
      case IntegerType =>
        TypeBuilder.makeI32(nullable)
      case ShortType =>
        TypeBuilder.makeI16(nullable)
      case ByteType =>
        TypeBuilder.makeI8(nullable)
      case StringType =>
        TypeBuilder.makeString(nullable)
      case BinaryType =>
        TypeBuilder.makeBinary(nullable)
      case DateType =>
        TypeBuilder.makeDate(nullable)
      case YearMonthIntervalType.DEFAULT =>
        TypeBuilder.makeIntervalYear(nullable)
      case DecimalType() =>
        val decimalType = datatype.asInstanceOf[DecimalType]
        val precision = decimalType.precision
        val scale = decimalType.scale
        TypeBuilder.makeDecimal(nullable, precision, scale)
      case TimestampType =>
        TypeBuilder.makeTimestamp(nullable)
      case m: MapType =>
        TypeBuilder.makeMap(
          nullable,
          getTypeNode(m.keyType, nullable = false),
          getTypeNode(m.valueType, m.valueContainsNull))
      case a: ArrayType =>
        TypeBuilder.makeList(nullable, getTypeNode(a.elementType, a.containsNull))
      case s: StructType =>
        val normalizeStructFieldName: String => String =
          if (BackendsApiManager.getSettings.structFieldToLowerCase()) {
            (name: String) => normalizeColName(name)
          } else { (name: String) => name }
        val (fieldNodes, fieldNames) = s.map {
          f => (getTypeNode(f.dataType, f.nullable), normalizeStructFieldName(f.name))
        }.unzip
        TypeBuilder.makeStruct(nullable, fieldNodes.asJava, fieldNames.asJava)
      case _: NullType =>
        TypeBuilder.makeNothing()
      case unknown =>
        throw new GlutenNotSupportException(s"Type $unknown not supported.")
    }
  }

  def parseFromTypeNode(typeNode: TypeNode): DataType = {
    typeNode match {
      case _: BooleanTypeNode =>
        BooleanType
      case _: FP32TypeNode =>
        FloatType
      case _: FP64TypeNode =>
        DoubleType
      case _: I64TypeNode =>
        LongType
      case _: I32TypeNode =>
        IntegerType
      case _: I16TypeNode =>
        ShortType
      case _: I8TypeNode =>
        ByteType
      case _: StringTypeNode =>
        StringType
      case _: BinaryTypeNode =>
        BinaryType
      case _: DateTypeNode =>
        DateType
      case _: IntervalYearTypeNode =>
        YearMonthIntervalType.DEFAULT
      case d: DecimalTypeNode =>
        DecimalType(d.precision, d.scale)
      case _: TimestampTypeNode =>
        TimestampType
      case m: MapNode =>
        val keyType = parseFromTypeNode(m.getKeyType)
        val valueType = parseFromTypeNode(m.getValueType)
        val valueContainsNull = m.getValueType.nullable()
        MapType(keyType, valueType, valueContainsNull)
      case a: ListNode =>
        val elementType = parseFromTypeNode(a.getNestedType)
        ArrayType(elementType, a.getNestedType.nullable())
      case s: StructNode =>
        val fields = s.getFieldTypes.asScala.map {
          typ =>
            val field = parseFromTypeNode(typ)
            StructField("", field, typ.nullable())
        }
        StructType(fields.toSeq)
      case _: NothingNode =>
        NullType
      case unknown =>
        throw new GlutenNotSupportException(s"Type $unknown not supported.")
    }
  }

  def parseFromBytes(bytes: Array[Byte]): ExpressionType = {
    val input = CodedInputStream.newInstance(bytes)
    val parsed = io.substrait.proto.Type.parseFrom(input)
    val (dataType, nullable) = parseFromSubstraitType(parsed)
    ExpressionType(dataType, nullable)
  }

  def printBatch(cb: ColumnarBatch): Unit = {
    var batch = ""
    for (rowId <- 0 until cb.numRows()) {
      var row = ""
      for (colId <- 0 until cb.numCols()) {
        row += (cb.column(colId).getUTF8String(rowId) + " ")
      }
      batch += (row + "\n")
    }
    logWarning(s"batch:\n$batch")
  }

  def ifEquals(left: Seq[AttributeReference], right: Seq[NamedExpression]): Boolean = {
    if (left.size != right.size) return false
    for (i <- left.indices) {
      if (left(i).exprId != right(i).exprId) return false
    }
    true
  }

  override def toString: String = {
    s"ConverterUtils"
  }

  def powerOfTen(pow: Int): (String, Int, Int) = {
    val POWERS_OF_10: Array[(String, Int, Int)] = Array(
      ("1", 1, 0),
      ("10", 2, 0),
      ("100", 3, 0),
      ("1000", 4, 0),
      ("10000", 5, 0),
      ("100000", 6, 0),
      ("1000000", 7, 0),
      ("10000000", 8, 0),
      ("100000000", 9, 0),
      ("1000000000", 10, 0),
      ("10000000000", 11, 0),
      ("100000000000", 12, 0),
      ("1000000000000", 13, 0),
      ("10000000000000", 14, 0),
      ("100000000000000", 15, 0),
      ("1000000000000000", 16, 0),
      ("10000000000000000", 17, 0),
      ("100000000000000000", 18, 0),
      ("1000000000000000000", 19, 0),
      ("10000000000000000000", 20, 0),
      ("100000000000000000000", 21, 0),
      ("1000000000000000000000", 22, 0),
      ("10000000000000000000000", 23, 0),
      ("100000000000000000000000", 24, 0),
      ("1000000000000000000000000", 25, 0),
      ("10000000000000000000000000", 26, 0),
      ("100000000000000000000000000", 27, 0),
      ("1000000000000000000000000000", 28, 0),
      ("10000000000000000000000000000", 29, 0),
      ("100000000000000000000000000000", 30, 0),
      ("1000000000000000000000000000000", 31, 0),
      ("10000000000000000000000000000000", 32, 0),
      ("100000000000000000000000000000000", 33, 0),
      ("1000000000000000000000000000000000", 34, 0),
      ("10000000000000000000000000000000000", 35, 0),
      ("100000000000000000000000000000000000", 36, 0),
      ("1000000000000000000000000000000000000", 37, 0)
    )
    POWERS_OF_10(pow)
  }

  // This enum is used to specify the function arg.
  object FunctionConfig extends Enumeration {
    type Config = Value
    val REQ, OPT, NON = Value
  }

  /**
   * Get the signature name of a type based on Substrait's definition in
   * https://substrait.io/extensions/#function-signature-compound-names.
   * @param dataType:
   *   the input data type.
   * @return
   *   the corresponding signature name.
   */
  def getTypeSigName(dataType: DataType): String = {
    dataType match {
      case BooleanType => // TODO: Not in Substrait yet.
        "bool"
      case ByteType => "i8"
      case ShortType => "i16"
      case IntegerType => "i32"
      case LongType => "i64"
      case FloatType => "fp32"
      case DoubleType => "fp64"
      case DateType => "date"
      case TimestampType => "ts"
      case StringType => "str"
      case BinaryType => "vbin"
      case DecimalType() =>
        val decimalType = dataType.asInstanceOf[DecimalType]
        val precision = decimalType.precision
        val scale = decimalType.scale
        // TODO: different with Substrait due to more details here.
        "dec<" + precision + "," + scale + ">"
      case ArrayType(elementType, _) =>
        s"list<${getTypeSigName(elementType)}>"
      case StructType(fields) =>
        // TODO: different with Substrait due to more details here.
        var sigName = "struct<"
        var index = 0
        fields.foreach(
          field => {
            sigName = sigName.concat(getTypeSigName(field.dataType))
            sigName = sigName.concat(if (index < fields.length - 1) "," else "")
            index += 1
          })
        sigName = sigName.concat(">")
        sigName
      case MapType(keyType, valueType, _) =>
        var sigName = "map<"
        sigName = sigName.concat(getTypeSigName(keyType))
        sigName = sigName.concat(",")
        sigName = sigName.concat(getTypeSigName(valueType))
        sigName = sigName.concat(">")
        sigName
      case CharType(_) =>
        "fchar"
      case NullType =>
        "nothing"
      case other =>
        throw new GlutenNotSupportException(s"Type $other not supported.")
    }
  }

  // This method is used to create a function name with input types.
  // The format would be aligned with that specified in Substrait.
  // The function name Format:
  // <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>
  def makeFuncName(
      funcName: String,
      datatypes: Seq[DataType],
      config: FunctionConfig.Config = FunctionConfig.NON): String = {
    var typedFuncName = config match {
      case FunctionConfig.REQ =>
        funcName.concat(":req_")
      case FunctionConfig.OPT =>
        funcName.concat(":opt_")
      case FunctionConfig.NON =>
        funcName.concat(":")
      case other =>
        throw new GlutenNotSupportException(s"$other is not supported.")
    }

    for (idx <- datatypes.indices) {
      typedFuncName = typedFuncName.concat(getTypeSigName(datatypes(idx)))
      // For the last item, no need to append _.
      if (idx < datatypes.size - 1) {
        typedFuncName = typedFuncName.concat("_")
      }
    }
    typedFuncName
  }

  def convertJoinType(joinType: JoinType): String = {
    joinType match {
      case Inner =>
        "Inner"
      case FullOuter =>
        "Outer"
      case LeftOuter | RightOuter =>
        "Left"
      case LeftSemi =>
        "Semi"
      case LeftAnti =>
        "Anti"
      case other =>
        throw new GlutenNotSupportException(s"Unsupported join type: $other")
    }
  }

  // A prefix used in the iterator path.
  final val ITERATOR_PREFIX = "iterator:"
}
