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
package org.apache.spark.sql.gluten

import org.apache.gluten.extension.injector.InjectorControl

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.rules.TestWatcher
import org.junit.runner.Description
import org.scalatest.{BeforeAndAfterEachTestData, Suite, TestData}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

object GlutenValidation {
  val canarySuite: String = "org.apache.spark.sql.gluten.GlutenActivationSuite"
  val canaryTest: String = "native execution with inherited Gluten defaults"
  val processId: String = ProcessHandle.current().pid().toString

  private val plugin = "org.apache.gluten.GlutenPlugin"
  private val threadKey = "gluten.enabledForCurrentThread"
  private val active = new ThreadLocal[Scope]
  private val mapper = new ObjectMapper()
  private lazy val output = {
    val directory = Paths.get(sys.env("GLUTEN_VALIDATION_DIR"))
    require(directory.isAbsolute, "GLUTEN_VALIDATION_DIR must be absolute")
    Files.createDirectories(directory)
    directory.resolve(s"jvm-$processId-${UUID.randomUUID()}.jsonl")
  }
  private lazy val exemptions =
    readExemptions(Paths.get(sys.env("GLUTEN_VALIDATION_EXEMPTIONS")))

  private case class Scope(suite: String, test: String, exemption: Option[String]) {
    val checks: ArrayBuffer[() => Unit] = ArrayBuffer.empty
  }

  // Only the legacy text exemption format is normalized; evidence keeps the raw test identity.
  private[gluten] def testKey(suite: String, test: String): String =
    s"$suite#${test.replace('\r', ' ').replace('\n', ' ')}".trim

  private[gluten] def readExemptions(path: Path): Map[String, String] = {
    val entries = Files.readAllLines(path, UTF_8).asScala
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .map {
        line =>
          val parts = line.split("\t", 2)
          require(parts.length == 2 && parts(1).trim.nonEmpty, s"Missing exemption reason: $line")
          require(parts(0).matches("[\\w.$]+#.+"), s"Invalid exemption key: ${parts(0)}")
          require(
            !parts(0).startsWith("org.apache.spark.sql.gluten."),
            "The validation harness cannot be exempted")
          parts(0) -> parts(1).trim
      }
    require(entries.map(_._1).distinct.size == entries.size, "Duplicate validation exemption")
    entries.toMap
  }

  private[gluten] def assertSettings(
      plugins: String,
      enabled: Boolean,
      threadEnabled: Option[String],
      rulesInstalled: Boolean): Unit = {
    require(
      plugins.split(",").map(_.trim).contains(plugin),
      s"Actual SparkContext is missing $plugin in spark.plugins")
    require(rulesInstalled, "Actual SparkSession is missing Gluten columnar rules")
    require(enabled, "spark.gluten.enabled is false at the test boundary")
    require(
      threadEnabled.forall(_.toBoolean),
      s"$threadKey is false at the test boundary")
  }

  private def checkContext(context: SparkContext): Unit =
    assertSettings(
      context.getConf.get("spark.plugins", ""),
      context.getConf.getBoolean("spark.gluten.enabled", true),
      Option(context.getLocalProperty(threadKey)),
      rulesInstalled = true
    )

  private def checkSession(session: SparkSession): Unit =
    assertSettings(
      session.sparkContext.getConf.get("spark.plugins", ""),
      session.conf.get("spark.gluten.enabled", "true").toBoolean,
      Option(session.sparkContext.getLocalProperty(threadKey)),
      session.sessionState.columnarRules.exists(_.isInstanceOf[InjectorControl.DisablerAware])
    )

  private def record(kind: String, fields: (String, AnyRef)*): Unit = synchronized {
    val row = new java.util.LinkedHashMap[String, AnyRef]()
    row.put("version", Int.box(1))
    row.put("pid", processId)
    row.put("kind", kind)
    fields.foreach { case (key, value) => row.put(key, value) }
    Files.write(
      output,
      (mapper.writeValueAsString(row) + "\n").getBytes(UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND)
  }

  private def verify(scope: Scope, phase: String)(check: => Unit): Unit = {
    try {
      check
    } catch {
      case NonFatal(error) =>
        record(
          "violation",
          "suite" -> scope.suite,
          "test" -> scope.test,
          "phase" -> phase,
          "message" -> error.toString)
        throw error
    }
  }

  def begin(suite: String, test: String): Unit = {
    require(active.get() == null, s"Overlapping Gluten validation scopes: $suite#$test")
    val exemption = exemptions.get(testKey(suite, test)).orElse(exemptions.get(s"$suite#*"))
    active.set(Scope(suite, test, exemption))
  }

  private def track(check: () => Unit): Unit = {
    val scope = Option(active.get()).getOrElse {
      throw new IllegalStateException("Missing Gluten validation test lifecycle")
    }
    if (scope.exemption.isEmpty) {
      verify(scope, "before")(check())
      scope.checks += check
    }
  }

  def checkedSession[S <: SparkSession](session: S): S = {
    track(() => checkSession(session))
    session
  }

  def checkedContext(context: SparkContext): SparkContext = {
    track(() => checkContext(context))
    context
  }

  implicit class CheckedSessionOps[S <: SparkSession](session: S) {
    def glutenChecked: S = checkedSession(session)
  }

  def end(suite: String, test: String): Unit = {
    val scope = Option(active.get()).getOrElse(Scope(suite, test, None))
    try {
      verify(scope, "after") {
        require(
          scope.suite == suite && scope.test == test,
          s"Mismatched Gluten validation lifecycle: $suite#$test")
        if (scope.exemption.isEmpty) {
          require(scope.checks.nonEmpty, "No Spark context/session was validated for this test")
          scope.checks.foreach(_())
        }
      }
      record(
        "test",
        "suite" -> suite,
        "test" -> test,
        "status" -> (if (scope.exemption.isDefined) "exempt" else "checked"),
        "checks" -> Int.box(scope.checks.size),
        "reason" -> scope.exemption.getOrElse("")
      )
    } finally {
      active.remove()
    }
  }

  def nativeCanary(session: SparkSession)(body: => Unit): Unit = {
    val scope = Scope(canarySuite, canaryTest, None)
    verify(scope, "canary") {
      checkSession(session)
      body
      checkSession(session)
      record("canary", "suite" -> canarySuite, "test" -> canaryTest, "status" -> "passed")
    }
  }
}

trait GlutenCheckedTest extends BeforeAndAfterEachTestData { this: Suite =>
  protected def glutenSession: Option[SparkSession] = None

  override protected def beforeEach(data: TestData): Unit = {
    super.beforeEach(data)
    GlutenValidation.begin(getClass.getName, data.name)
    glutenSession.foreach(GlutenValidation.checkedSession(_))
  }

  override protected def afterEach(data: TestData): Unit = {
    try {
      super.afterEach(data)
    } finally {
      GlutenValidation.end(getClass.getName, data.name)
    }
  }
}

trait GlutenSharedSparkSession extends SharedSparkSession with GlutenCheckedTest {
  override protected def glutenSession: Option[SparkSession] = Some(spark)
}

class GlutenValidationRule extends TestWatcher {
  override protected def starting(description: Description): Unit =
    GlutenValidation.begin(description.getClassName, description.getMethodName)

  override protected def finished(description: Description): Unit =
    GlutenValidation.end(description.getClassName, description.getMethodName)
}
