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
package org.apache.spark.sql.expression

import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.exception.{GlutenException, GlutenNotSupportException}
import org.apache.gluten.expression._
import org.apache.gluten.extension.injector.FunctionDescription
import org.apache.gluten.jni.JniWorkspace
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.udf.UdfJniWrapper

import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, ExpressionInfo, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

import java.io.File
import java.net.URI
import java.nio.file.{Files, FileVisitOption, Paths}
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters.{asScalaIteratorConverter, seqAsJavaListConverter}
import scala.collection.mutable

case class UserDefinedAggregateFunction(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    children: Seq[Expression],
    override val aggBufferAttributes: Seq[AttributeReference])
  extends AggregateFunction {
  override def prettyName: String = name

  override def aggBufferSchema: StructType =
    StructType(
      aggBufferAttributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    this.copy(children = newChildren)
  }
}

trait UDFSignatureBase {
  val expressionType: ExpressionType
  val children: Seq[DataType]
  val variableArity: Boolean
  val allowTypeConversion: Boolean
}

case class UDFSignature(
    expressionType: ExpressionType,
    children: Seq[DataType],
    variableArity: Boolean,
    allowTypeConversion: Boolean)
  extends UDFSignatureBase

case class UDAFSignature(
    expressionType: ExpressionType,
    children: Seq[DataType],
    variableArity: Boolean,
    allowTypeConversion: Boolean,
    intermediateAttrs: Seq[AttributeReference])
  extends UDFSignatureBase

case class UDFExpression(
    name: String,
    alias: String,
    dataType: DataType,
    nullable: Boolean,
    children: Seq[Expression])
  extends Unevaluable
  with Transformable {
  override def nodeName: String = alias

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    this.copy(children = newChildren)
  }

  override def getTransformer(
      childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer = {
    if (childrenTransformers.size != children.size) {
      throw new IllegalStateException(
        this.getClass.getSimpleName +
          ": getTransformer called before children transformer initialized.")
    }

    GenericExpressionTransformer(name, childrenTransformers, this)
  }
}

object UDFResolver extends Logging {
  val UDFNames = mutable.HashSet[String]()
  // (udf_name, arg1, arg2, ...) => return type
  private val UDFMap = mutable.HashMap[String, mutable.ListBuffer[UDFSignature]]()

  val UDAFNames = mutable.HashSet[String]()
  // (udaf_name, arg1, arg2, ...) => return type, intermediate attributes
  private val UDAFMap =
    mutable.HashMap[String, mutable.ListBuffer[UDAFSignature]]()

  // Functions the library declared by name alone. They have no entry in UDFMap
  // / UDAFMap because no signature was stated for them: the one the library
  // registered with Velox is the signature, and it is bound per call site.
  //
  // Exposed like UDFNames / UDAFNames above so a test can restore them: every
  // register call below writes two of these four sets, and a name left behind
  // in one of them outlives the test that registered it.
  val RegistryUDFNames = mutable.HashSet[String]()
  val RegistryUDAFNames = mutable.HashSet[String]()

  // Memoize native resolution, which is a JNI call per distinct call shape.
  // Written during planning, so unlike the registration maps these need to be
  // safe for concurrent access.
  //
  // Scalar and aggregate resolutions are held apart because a name can be both:
  // Velox keeps its scalar and aggregate registries separately, so a library is
  // free to declare one of each. A single map keyed on (name, argument types)
  // would hand an aggregate call the scalar answer.
  private val registryUdfResolutions =
    new ConcurrentHashMap[(String, Seq[DataType]), Option[ExpressionType]]()
  private val registryUdafResolutions =
    new ConcurrentHashMap[(String, Seq[DataType]), Option[(ExpressionType, ExpressionType)]]()

  private val LIB_EXTENSION = ".so"

  // Called by JNI.
  def registerUDF(
      name: String,
      returnType: Array[Byte],
      argTypes: Array[Byte],
      variableArity: Boolean,
      allowTypeConversion: Boolean): Unit = {
    registerUDF(
      name,
      ConverterUtils.parseFromBytes(returnType),
      ConverterUtils.parseFromBytes(argTypes),
      variableArity,
      allowTypeConversion)
  }

  private def registerUDF(
      name: String,
      returnType: ExpressionType,
      argTypes: ExpressionType,
      variableArity: Boolean,
      allowTypeConversion: Boolean): Unit = {
    assert(argTypes.dataType.isInstanceOf[StructType])
    val v =
      UDFMap.getOrElseUpdate(name, mutable.ListBuffer[UDFSignature]())
    v += UDFSignature(
      returnType,
      argTypes.dataType.asInstanceOf[StructType].fields.map(_.dataType),
      variableArity,
      allowTypeConversion)
    UDFNames += name
    logInfo(s"Registered UDF: $name($argTypes) -> $returnType")
  }

  def registerUDAF(
      name: String,
      returnType: Array[Byte],
      argTypes: Array[Byte],
      intermediateTypes: Array[Byte],
      variableArity: Boolean,
      enableTypeConversion: Boolean): Unit = {
    registerUDAF(
      name,
      ConverterUtils.parseFromBytes(returnType),
      ConverterUtils.parseFromBytes(argTypes),
      ConverterUtils.parseFromBytes(intermediateTypes),
      variableArity,
      enableTypeConversion
    )
  }

  // Called by JNI.
  def registerRegistryUDF(name: String): Unit = {
    RegistryUDFNames += name
    // UDFNames gates whether a call is offloaded at all, in
    // VeloxHiveUDFTransformer and getFunctionDescriptions.
    UDFNames += name
    logInfo(s"Registered UDF by name, signature from the Velox registry: $name")
  }

  // Called by JNI.
  def registerRegistryUDAF(name: String): Unit = {
    RegistryUDAFNames += name
    UDAFNames += name
    logInfo(s"Registered UDAF by name, signature from the Velox registry: $name")
  }

  private def aggBufferAttributesOf(intermediateType: DataType): Seq[AttributeReference] =
    intermediateType match {
      case StructType(fields) =>
        fields.zipWithIndex.map {
          case (f, index) =>
            AttributeReference(s"agg_inter_$index", f.dataType, f.nullable)()
        }
      case t =>
        Seq(AttributeReference(s"agg_inter", t)())
    }

  private def registerUDAF(
      name: String,
      returnType: ExpressionType,
      argTypes: ExpressionType,
      intermediateTypes: ExpressionType,
      variableArity: Boolean,
      allowTypeConversion: Boolean): Unit = {
    assert(argTypes.dataType.isInstanceOf[StructType])

    val aggBufferAttributes = aggBufferAttributesOf(intermediateTypes.dataType)

    val v =
      UDAFMap.getOrElseUpdate(name, mutable.ListBuffer[UDAFSignature]())
    v += UDAFSignature(
      returnType,
      argTypes.dataType.asInstanceOf[StructType].fields.map(_.dataType),
      variableArity,
      allowTypeConversion,
      aggBufferAttributes)
    UDAFNames += name
    logInfo(s"Registered UDAF: $name($argTypes) -> $returnType")
  }

  def parseName(name: String): (String, String) = {
    val index = name.lastIndexOf("#")
    if (index == -1) {
      (name, Paths.get(name).getFileName.toString)
    } else {
      (name.substring(0, index), name.substring(index + 1))
    }
  }

  private def getFilesWithExtension(
      directory: java.nio.file.Path,
      extension: String): Seq[String] = {
    Files
      .walk(directory, FileVisitOption.FOLLOW_LINKS)
      .iterator()
      .asScala
      .filter(p => Files.isRegularFile(p) && p.toString.endsWith(extension))
      .map(p => p.toString)
      .toSeq
  }

  def resolveUdfConf(sparkConf: SparkConf, isDriver: Boolean): Unit = {
    val udfLibPaths = if (isDriver) {
      sparkConf
        .getOption(VeloxBackendSettings.GLUTEN_VELOX_DRIVER_UDF_LIB_PATHS)
        .orElse(sparkConf.getOption(VeloxBackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS))
    } else {
      sparkConf.getOption(VeloxBackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS)
    }

    udfLibPaths match {
      case Some(paths) =>
        // Set resolved paths to the internal config to parse on native side.
        sparkConf.set(
          VeloxBackendSettings.GLUTEN_VELOX_INTERNAL_UDF_LIB_PATHS,
          getAllLibraries(sparkConf, isDriver, paths))
      case None =>
    }
  }

  // Try to unpack archive. Throws exception if failed.
  private def unpack(source: File, destDir: File): File = {
    val sourceName = source.getName
    val dest = new File(destDir, sourceName)
    logInfo(
      s"Unpacking an archive $sourceName from ${source.getAbsolutePath} to ${dest.getAbsolutePath}")
    try {
      Utils.deleteRecursively(dest)
      Utils.unpack(source, dest)
    } catch {
      case e: Exception =>
        throw new GlutenException(
          s"Unpack ${source.toString} failed. Please check if it is an archive.",
          e)
    }
    dest
  }

  private def isRelativePath(path: String): Boolean = {
    try {
      val uri = new URI(path)
      !uri.isAbsolute && uri.getPath == path
    } catch {
      case _: Exception => false
    }
  }

  // Get the full paths of all libraries.
  // If it's a directory, get all files ends with ".so" recursively.
  private def getAllLibraries(sparkConf: SparkConf, isDriver: Boolean, files: String) = {
    val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)
    val master = sparkConf.getOption("spark.master")
    val isYarnCluster =
      master.isDefined && master.get.equals("yarn") && !Utils.isClientMode(sparkConf)
    val isYarnClient =
      master.isDefined && master.get.equals("yarn") && Utils.isClientMode(sparkConf)

    files
      .split(",")
      .map {
        f =>
          val file = new File(f)
          // Relative paths should be uploaded via --files or --archives
          if (isRelativePath(f)) {
            logInfo(s"resolve relative path: $f")
            if (isDriver && isYarnClient) {
              throw new IllegalArgumentException(
                "On yarn-client mode, driver only accepts absolute paths, but got " + f)
            }
            if (isYarnCluster || isYarnClient) {
              file
            } else {
              new File(SparkFiles.get(f))
            }
          } else {
            logInfo(s"resolve absolute URI path: $f")
            // Download or copy absolute paths to JniWorkspace.
            val uri = Utils.resolveURI(f)
            val name = file.getName
            val jniWorkspace = new File(JniWorkspace.getDefault.getWorkDir)
            if (!file.isDirectory && !f.endsWith(LIB_EXTENSION)) {
              val source = Utils
                .doFetchFile(uri.toString, Utils.createTempDir(), name, sparkConf, hadoopConf)
              unpack(source, jniWorkspace)
            } else {
              Utils.doFetchFile(uri.toString, jniWorkspace, name, sparkConf, hadoopConf)
            }
          }
      }
      .flatMap {
        f =>
          if (f.isDirectory) {
            getFilesWithExtension(f.toPath, LIB_EXTENSION)
          } else {
            Seq(f.toString)
          }
      }
      .mkString(",")
  }

  private def checkAllowTypeConversion: Boolean = {
    SQLConf.get
      .getConfString(VeloxBackendSettings.GLUTEN_VELOX_UDF_ALLOW_TYPE_CONVERSION, "false")
      .toBoolean
  }

  /**
   * One Spark function per loaded UDF whose name contains no dot. A dotted name is a Hive UDF class
   * name, which VeloxHiveUDFTransformer already resolves, so it is skipped here.
   *
   * A name is also skipped when it collides with a Spark built-in: the names are unqualified, so
   * injecting one would redirect that built-in to a native implementation with possibly different
   * semantics for every query on the session.
   *
   * Names differing only in case are skipped as a group. Spark lowercases a function name when it
   * registers it, so they would collapse to one entry and the last registration would win, leaving
   * a call to either name running the other one's implementation.
   */
  def getFunctionDescriptions: Seq[FunctionDescription] = {
    val candidates = UDFNames.toSeq.filterNot(_.contains(".")).sorted
    val byLowerCase = candidates.groupBy(_.toLowerCase(Locale.ROOT))

    val (ambiguous, distinct) =
      candidates.partition(name => byLowerCase(name.toLowerCase(Locale.ROOT)).size > 1)

    val (shadowing, injectable) =
      distinct.partition(name => FunctionRegistry.builtin.functionExists(FunctionIdentifier(name)))

    ambiguous.foreach(
      name =>
        logWarning(
          s"Not registering UDF '$name' by name: it differs only in case from another UDF " +
            s"loaded from the same libraries, and Spark function names are case-insensitive. " +
            s"Rename it in the UDF library to call it directly."))

    shadowing.foreach(
      name =>
        logWarning(
          s"Not registering UDF '$name' by name: it shadows a Spark built-in. " +
            s"Rename it in the UDF library to call it directly."))

    injectable.map {
      name =>
        (
          FunctionIdentifier(name),
          new ExpressionInfo(classOf[UDFExpression].getName, name),
          (children: Seq[Expression]) => getUdfExpression(name, name)(children))
    }
  }

  // Velox types carry no nullability, so it is not part of the question being asked here.
  private def encodeArgTypes(argTypes: Seq[DataType]): Array[Byte] = {
    val argTypeNodes = argTypes.map(t => ConverterUtils.getTypeNode(t, nullable = true))
    TypeBuilder.makeStruct(false, argTypeNodes.asJava).toProtobuf.toByteArray
  }

  private def logNoBind(name: String, argTypes: Seq[DataType]): Unit =
    logDebug(
      s"No Velox signature of $name binds to ${argTypes.map(_.simpleString).mkString(", ")}.")

  /**
   * Resolves the return type of a scalar call by binding 'argTypes' against the signatures the
   * library registered with Velox for 'name'. Returns None if nothing binds.
   *
   * Binding is exact: no cast is injected to make a call fit. Velox does not support coercion for
   * signatures carrying type variables, and where one is in play "cast the arguments until they
   * bind" has too many answers to pick from.
   */
  private def resolveUdfFromRegistry(
      name: String,
      argTypes: Seq[DataType]): Option[ExpressionType] = {
    registryUdfResolutions.computeIfAbsent(
      (name, argTypes),
      _ =>
        UdfJniWrapper.resolveUdfType(name, encodeArgTypes(argTypes)) match {
          case null =>
            logNoBind(name, argTypes)
            None
          case resolved =>
            Some(ConverterUtils.parseFromBytes(resolved))
        }
    )
  }

  /**
   * Like resolveUdfFromRegistry, for an aggregate. Returns the return type and the intermediate
   * type, both taken from the one signature that bound, or None if none did.
   */
  private def resolveUdafFromRegistry(
      name: String,
      argTypes: Seq[DataType]): Option[(ExpressionType, ExpressionType)] = {
    registryUdafResolutions.computeIfAbsent(
      (name, argTypes),
      _ =>
        UdfJniWrapper.resolveUdafTypes(name, encodeArgTypes(argTypes)) match {
          case null =>
            logNoBind(name, argTypes)
            None
          case resolved =>
            ConverterUtils.parseFromBytes(resolved).dataType match {
              case StructType(Array(returnField, intermediateField)) =>
                Some(
                  (
                    ExpressionType(returnField.dataType, returnField.nullable),
                    ExpressionType(intermediateField.dataType, intermediateField.nullable)))
              case other =>
                throw new GlutenException(
                  s"Expected a {returnType, intermediateType} struct for $name, got $other")
            }
        }
    )
  }

  def getUdfExpression(name: String, alias: String)(children: Seq[Expression]): UDFExpression = {
    def errorMessage: String =
      s"UDF $name -> ${children.map(_.dataType.simpleString).mkString(", ")} is not registered."

    val argTypes = children.map(_.dataType)
    val allowTypeConversion = checkAllowTypeConversion
    val signatures = UDFMap.getOrElse(name, mutable.ListBuffer.empty[UDFSignature]).toSeq

    // A stated signature wins over a registry lookup, so a library can pin one
    // call shape by hand and leave the rest to Velox.
    tryBind(signatures, argTypes, allowTypeConversion) match {
      case Some((sig, withTypeConversion)) =>
        UDFExpression(
          name,
          alias,
          sig.expressionType.dataType,
          sig.expressionType.nullable,
          if (!withTypeConversion) children
          else applyCast(children, sig)
        )
      case None if RegistryUDFNames.contains(name) =>
        resolveUdfFromRegistry(name, argTypes) match {
          case Some(returnType) =>
            UDFExpression(name, alias, returnType.dataType, returnType.nullable, children)
          case None =>
            throw new GlutenNotSupportException(errorMessage)
        }
      case None =>
        throw new GlutenNotSupportException(errorMessage)
    }
  }

  def getUdafExpression(name: String)(children: Seq[Expression]): UserDefinedAggregateFunction = {
    def errorMessage: String =
      s"UDAF $name -> ${children.map(_.dataType.simpleString).mkString(", ")} is not registered."

    val argTypes = children.map(_.dataType)
    val allowTypeConversion = checkAllowTypeConversion
    val signatures = UDAFMap.getOrElse(name, mutable.ListBuffer.empty[UDAFSignature]).toSeq

    tryBind(signatures, argTypes, allowTypeConversion) match {
      case Some((sig, withTypeConversion)) =>
        UserDefinedAggregateFunction(
          name,
          sig.expressionType.dataType,
          sig.expressionType.nullable,
          if (!withTypeConversion) children
          else applyCast(children, sig),
          sig.intermediateAttrs
        )
      case None if RegistryUDAFNames.contains(name) =>
        resolveUdafFromRegistry(name, argTypes) match {
          case Some((returnType, intermediateType)) =>
            UserDefinedAggregateFunction(
              name,
              returnType.dataType,
              returnType.nullable,
              children,
              aggBufferAttributesOf(intermediateType.dataType))
          case None =>
            throw new GlutenNotSupportException(errorMessage)
        }
      case None =>
        throw new GlutenNotSupportException(errorMessage)
    }
  }

  private def tryBind[U <: UDFSignatureBase](
      signatures: Seq[U],
      requiredDataTypes: Seq[DataType],
      allowTypeConversion: Boolean): Option[(U, Boolean)] = {
    signatures.find(sig => tryBindStrict(sig, requiredDataTypes)) match {
      case Some(sig) => Some((sig, false))
      case None =>
        val allowTypeConversionSignatures = if (allowTypeConversion) {
          signatures
        } else {
          signatures.filter(_.allowTypeConversion)
        }
        allowTypeConversionSignatures.find(
          sig => tryBindWithTypeConversion(sig, requiredDataTypes)) match {
          case Some(sig) => Some((sig, true))
          case None => None
        }
    }
  }

  // Returns true if required data types match the function signature.
  // If the function signature is variable arity, the number of the last argument can be zero
  // or more.
  private def tryBindWithTypeConversion(
      sig: UDFSignatureBase,
      requiredDataTypes: Seq[DataType]): Boolean = {
    tryBind0(sig, requiredDataTypes, Cast.canCast)
  }

  private def tryBindStrict(sig: UDFSignatureBase, requiredDataTypes: Seq[DataType]): Boolean = {
    tryBind0(sig, requiredDataTypes, DataTypeUtils.sameType)
  }

  private def tryBind0(
      sig: UDFSignatureBase,
      requiredDataTypes: Seq[DataType],
      checkType: (DataType, DataType) => Boolean): Boolean = {
    if (!sig.variableArity) {
      sig.children.size == requiredDataTypes.size &&
      requiredDataTypes
        .zip(sig.children)
        .forall { case (required, candidate) => checkType(required, candidate) }
    } else {
      // If variableArity is true, there must be at least one argument in the signature.
      if (requiredDataTypes.size < sig.children.size - 1) {
        false
      } else if (requiredDataTypes.size == sig.children.size - 1) {
        requiredDataTypes
          .zip(sig.children.dropRight(1))
          .forall { case (required, candidate) => checkType(required, candidate) }
      } else {
        val varArgStartIndex = sig.children.size - 1
        // First check all var args has the same type with the last argument of the signature.
        if (
          !requiredDataTypes
            .drop(varArgStartIndex)
            .forall(argType => checkType(argType, sig.children.last))
        ) {
          false
        } else if (varArgStartIndex == 0) {
          // No fixed args.
          true
        } else {
          // Whether fixed args matches.
          requiredDataTypes
            .dropRight(1 + requiredDataTypes.size - sig.children.size)
            .zip(sig.children.dropRight(1))
            .forall { case (required, candidate) => checkType(required, candidate) }
        }
      }
    }
  }

  private def applyCast(children: Seq[Expression], sig: UDFSignatureBase): Seq[Expression] = {
    def maybeCast(expr: Expression, toType: DataType): Expression = {
      if (!expr.dataType.sameType(toType)) {
        Cast(expr, toType)
      } else {
        expr
      }
    }

    if (!sig.variableArity) {
      children.zip(sig.children).map { case (expr, toType) => maybeCast(expr, toType) }
    } else {
      val fixedArgs = Math.min(children.size, sig.children.size)
      val newChildren = children.take(fixedArgs).zip(sig.children.take(fixedArgs)).map {
        case (expr, toType) => maybeCast(expr, toType)
      }
      if (children.size > sig.children.size) {
        val varArgType = sig.children.last
        newChildren ++ children.takeRight(children.size - sig.children.size).map {
          expr => maybeCast(expr, varArgType)
        }
      } else {
        newChildren
      }
    }
  }
}
