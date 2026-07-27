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
import org.apache.gluten.config.{ConfigRegistry, NativeConfRegistry}
import org.apache.gluten.extension.injector.Injector

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class ComponentSuite extends AnyFunSuite with BeforeAndAfterAll {
  import ComponentSuite._

  override protected def afterAll(): Unit = {
    // The component graph is JVM-global, and these tests register dummy components into it,
    // including a deliberate dependency cycle. Leaving them behind makes any later suite that
    // calls Component#sorted fail.
    clearAllForTesting()
    super.afterAll()
  }

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

  test("Component confs are empty by default and overridable") {
    val a = new DummyComponent("A") {}
    assert(a.confs().isEmpty)

    val withConfs = new DummyComponent("B") {
      override def confs(): Seq[ConfigRegistry] = Seq(ComponentSuite.EmptyComponentConfig)
    }
    assert(withConfs.confs() === Seq(ComponentSuite.EmptyComponentConfig))
  }

  test("Initializing a component's confs registers its native confs") {
    val modifiableKey = "spark.gluten.test.component.dynamic.conf"
    val staticKey = "spark.gluten.test.component.static.conf"
    val plainKey = "spark.gluten.test.component.plain.conf"

    val component = new DummyComponent("WithConfs") {
      override def confs(): Seq[ConfigRegistry] = Seq(ComponentSuite.DummyComponentConfig)
    }
    // This is what `ensureAllComponentsRegistered` does for every discovered component. It is
    // idempotent, so this test does not care whether an earlier run already initialized the object.
    component.confs().foreach(_.ensureRegistered())

    // A modifiable conf reaches both channels, a static one the backend channel only.
    assert(NativeConfRegistry.isRuntimeKey(modifiableKey))
    assert(NativeConfRegistry.isBackendKey(modifiableKey))
    assert(NativeConfRegistry.isBackendKey(staticKey))
    assert(!NativeConfRegistry.isRuntimeKey(staticKey))
    assert(NativeConfRegistry.isBackendKey(plainKey))
    assert(!NativeConfRegistry.isRuntimeKey(plainKey))

    assert(
      NativeConfRegistry
        .selectRuntimeConf(Map(modifiableKey -> "8"))
        .get(modifiableKey) === Some("8"))
    // The component declared a default for its plain key, so native always gets the key.
    assert(
      NativeConfRegistry
        .selectBackendConf(Map.empty[String, String])
        .get(plainKey) === Some("componentDefault"))
  }
}

object ComponentSuite {

  /** A conf object declaring nothing, used where touching it must have no side effect. */
  private object EmptyComponentConfig extends ConfigRegistry

  /** A conf object as a third-party component would declare one. */
  private object DummyComponentConfig extends ConfigRegistry {
    val DYNAMIC =
      buildConf("spark.gluten.test.component.dynamic.conf")
        .passToNative()
        .intConf
        .createWithDefault(4)

    val STATIC =
      buildStaticConf("spark.gluten.test.component.static.conf")
        .passToNative()
        .booleanConf
        .createWithDefault(false)

    // A plain Spark-style key with no Gluten entry, read once at native backend init.
    registerStaticConf("spark.gluten.test.component.plain.conf")
      .stringConf
      .passToNative()
      .passDefault()
      .createWithDefault("componentDefault")
  }

  private trait DependencyBuilder extends Component {
    private val dependencyBuffer = mutable.Set[Class[_ <: Component]]()

    override def dependencies(): Seq[Class[_ <: Component]] = dependencyBuffer.toSeq

    def dependsOn(component: Component*): Unit = {
      dependencyBuffer ++= component.map(_.getClass)
    }
  }

  private trait CompatibilityHelper extends Component {
    private var _isRuntimeCompatible: Boolean = true

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
