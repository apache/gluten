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
package org.apache.gluten.component

import org.apache.gluten.extension.injector.Injector

import org.scalatest.{Args, BeforeAndAfterAll, Reporter}
import org.scalatest.events.{Event, SuiteAborted, TestFailed}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class ComponentGraphResetSuite extends AnyFunSuite with BeforeAndAfterAll {
  import ComponentGraphResetSuite._

  override protected def afterAll(): Unit = {
    // Every test here registers components of its own into the JVM-global graph. Clearing from
    // afterAll rather than from the test body makes sure a failed assertion cannot leak them.
    clearAllForTesting()
    super.afterAll()
  }

  test("a cycle registered by an earlier suite does not outlive the reset") {
    // Reproduces what ComponentSuite leaves behind: components wired into a cycle. Without the
    // reset these stay in the JVM-global graph and every later Component#sorted call throws.
    clearAllForTesting()
    new CycleA().ensureRegistered()
    new CycleB().ensureRegistered()
    assertThrows[UnsupportedOperationException](Component.sortedUnsafe())

    clearAllForTesting()

    // The graph is empty again, so sorting no longer reports the cycle.
    assert(Component.sortedUnsafe().isEmpty)
  }

  test("a component cleared from the graph can be registered again") {
    // Covers the registration-flag reset the clear performs: without it this very instance could
    // never re-enter the graph, because ensureRegistered short-circuits on its own flag.
    clearAllForTesting()
    val standalone = new Standalone()
    standalone.ensureRegistered()
    assert(Component.sortedUnsafe().exists(_.name() == StandaloneName))

    clearAllForTesting()
    assert(Component.sortedUnsafe().isEmpty)

    standalone.ensureRegistered()
    assert(Component.sortedUnsafe().exists(_.name() == StandaloneName))
  }

  test("ComponentSuite leaves the component graph empty") {
    // Covers ComponentSuite's own cleanup without depending on suite execution order: run that
    // suite in-process, then assert it took its dummy components back out of the graph.
    clearAllForTesting()
    val reporter = new FailureCollectingReporter()
    val status = new ComponentSuite().run(None, Args(reporter))
    assert(status.succeeds(), s"ComponentSuite itself failed: ${reporter.failures.mkString("; ")}")
    assert(Component.sortedUnsafe().isEmpty)
  }
}

object ComponentGraphResetSuite {
  private val StandaloneName: String = "reset-standalone"

  /**
   * Keeps the nested suite's events out of the test log, while retaining enough of them to say what
   * went wrong if the nested suite itself fails.
   */
  private class FailureCollectingReporter extends Reporter {
    private val buffer = mutable.Buffer[String]()

    def failures: Seq[String] = buffer.toSeq

    override def apply(event: Event): Unit = event match {
      case e: TestFailed => buffer += s"${e.testName}: ${e.message}"
      case e: SuiteAborted => buffer += s"${e.suiteName} aborted: ${e.message}"
      case _ =>
    }
  }

  private class Standalone extends Component {
    override def name(): String = StandaloneName
    override def dependencies(): Seq[Class[_ <: Component]] = Nil
    override def injectRules(injector: Injector): Unit = {}
  }

  private class CycleA extends Component {
    override def name(): String = "reset-A"
    override def dependencies(): Seq[Class[_ <: Component]] = Seq(classOf[CycleB])
    override def injectRules(injector: Injector): Unit = {}
  }

  private class CycleB extends Component {
    override def name(): String = "reset-B"
    override def dependencies(): Seq[Class[_ <: Component]] = Seq(classOf[CycleA])
    override def injectRules(injector: Injector): Unit = {}
  }
}
