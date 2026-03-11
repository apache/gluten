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

import org.apache.gluten.backend.Backend
import org.apache.gluten.extension.injector.Injector

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class ComponentSuite extends AnyFunSuite with BeforeAndAfterAll {
  import ComponentSuite._

  test("Load order") {
    val a = new DummyBackend("A") {}
    val b = new DummyBackend("B") {}
    val c = new DummyComponent("C") {}
    val d = new DummyComponent("D") {}
    val e = new DummyComponent("E") {}

    c.dependsOn(a)
    d.dependsOn(a, b)
    e.dependsOn(a, d)

    a.ensureRegistered()
    b.ensureRegistered()
    c.ensureRegistered()
    d.ensureRegistered()
    e.ensureRegistered()

    val possibleOrders: Set[Seq[Component]] =
      Set(
        Seq(a, b, c, d, e),
        Seq(a, b, d, c, e),
        Seq(b, a, c, d, e),
        Seq(b, a, d, c, e)
      )

    assert(possibleOrders.contains(Component.sorted().filter(Seq(a, b, c, d, e).contains(_))))
  }

  test("Register again") {
    class DummyBackendA extends DummyBackend("A")
    new DummyBackendA().ensureRegistered()
    assertThrows[IllegalArgumentException] {
      new DummyBackendA().ensureRegistered()
    }
  }

  test("Incompatible component") {
    val a = new DummyBackend("A") {}
    val b = new DummyBackend("B") {}
    val c = new DummyComponent("C") {}
    val d = new DummyComponent("D") {}
    val e = new DummyComponent("E") {}

    c.dependsOn(a)
    d.dependsOn(a, b)
    e.dependsOn(a, d)

    d.setIncompatible()

    a.ensureRegistered()
    b.ensureRegistered()
    c.ensureRegistered()
    d.ensureRegistered()
    e.ensureRegistered()

    val possibleOrders: Set[Seq[Component]] =
      Set(
        Seq(a, b, c),
        Seq(b, a, c)
      )

    assert(possibleOrders.contains(Component.sorted().filter(Seq(a, b, c, d, e).contains(_))))
  }

  test("Incompatible backend") {
    val a = new DummyBackend("A") {}
    val b = new DummyBackend("B") {}
    val c = new DummyComponent("C") {}
    val d = new DummyComponent("D") {}
    val e = new DummyComponent("E") {}

    c.dependsOn(a)
    d.dependsOn(a, b)
    e.dependsOn(a, d)

    b.setIncompatible()

    a.ensureRegistered()
    b.ensureRegistered()
    c.ensureRegistered()
    d.ensureRegistered()
    e.ensureRegistered()

    val possibleOrders: Set[Seq[Component]] =
      Set(
        Seq(a, c)
      )

    assert(possibleOrders.contains(Component.sorted().filter(Seq(a, b, c, d, e).contains(_))))
  }

  test("Dependencies not registered") {
    val a = new DummyBackend("A") {}
    val c = new DummyComponent("C") {}

    c.dependsOn(a)
    c.ensureRegistered()
    assertThrows[IllegalArgumentException] {
      Component.sorted()
    }

    a.ensureRegistered()
    assert(Component.sorted().filter(Seq(a, c).contains(_)) === Seq(a, c))
  }

  test("Dependency cycle") {
    val a = new DummyComponent("A") {}
    val b = new DummyComponent("B") {}
    val c = new DummyComponent("C") {}
    val d = new DummyComponent("D") {}
    val e = new DummyComponent("E") {}

    // Cycle: b -> c -> d.
    d.dependsOn(c)
    c.dependsOn(b)
    b.dependsOn(d)

    b.dependsOn(a)
    e.dependsOn(a)

    a.ensureRegistered()
    b.ensureRegistered()
    c.ensureRegistered()
    d.ensureRegistered()
    e.ensureRegistered()

    assertThrows[UnsupportedOperationException] {
      Component.sorted()
    }
  }
}

object ComponentSuite {
  private trait DependencyBuilder extends Component {
    private val dependencyBuffer = mutable.Set[Class[_ <: Component]]()

    override def dependencies(): Seq[Class[_ <: Component]] = dependencyBuffer.toSeq

    def dependsOn(component: Component*): Unit = {
      dependencyBuffer ++= component.map(_.getClass)
    }
  }

  private trait CompatibilityHelper extends Component {
    private var _isRuntimeCompatible: Boolean = true;

    override def isRuntimeCompatible: Boolean = _isRuntimeCompatible

    def setIncompatible(): Unit = {
      _isRuntimeCompatible = false
    }
  }

  abstract private class DummyComponent(override val name: String)
    extends Component
    with DependencyBuilder
    with CompatibilityHelper {

    /** Query planner rules. */
    override def injectRules(injector: Injector): Unit = {}
  }

  abstract private class DummyBackend(override val name: String)
    extends Backend
    with CompatibilityHelper {

    /** Query planner rules. */
    override def injectRules(injector: Injector): Unit = {}
  }
}
