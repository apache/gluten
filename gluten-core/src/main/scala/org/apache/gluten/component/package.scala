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
package org.apache.gluten

import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicBoolean

package object component extends Logging {
  private val allComponentsLoaded: AtomicBoolean = new AtomicBoolean(false)

  private[component] def ensureAllComponentsRegistered(): Unit = {
    if (!allComponentsLoaded.compareAndSet(false, true)) {
      return
    }

    // Load all components in classpath.
    val all = Discovery.discoverAll()

    // Register all components.
    all.foreach(_.ensureRegistered())

    // Output log so user could view the component loading order.
    // Call #sortedUnsafe than on #sorted to avoid unnecessary recursion.
    val components = Component.sortedUnsafe()
    require(
      components.nonEmpty,
      s"No component files found in container directories named with " +
        s"'META-INF/gluten-components' from classpath. JVM classpath value " +
        s"is: ${System.getProperty("java.class.path")}"
    )
    logInfo(s"Components registered within order: ${components.map(_.name()).mkString(", ")}")

    // Initialize the configuration objects owned by the components. This must happen before any
    // component's `onDriverStart` / `onExecutorStart` runs, so that a component's registrations of
    // configurations to pass to native side are in place before the native backend is initialized
    // (which a backend does from its own `onDriverStart`) and before the first native runtime is
    // created. Only runtime-compatible components are visited, so an excluded component's
    // configurations never reach native side.
    components.foreach {
      component =>
        component.confs().foreach {
          conf =>
            logDebug(s"Initializing configurations ${conf.getClass.getName} of component " +
              s"${component.name()}.")
            conf.ensureRegistered()
        }
    }
  }

  /**
   * Empties the component graph and re-arms the discovery latch, so the next call to
   * [[Component.sorted]] runs classpath discovery again.
   *
   * Only the graph and the latch are reset. Values derived from an earlier [[Component.sorted]] are
   * not: `BackendsApiManager.backend`, `GlutenCostModel.costModelRegistry` and the `graphCache`
   * inside `Transition.factory` keep what they computed from the pre-clear component set. Neither
   * are the per-vertex `TransitionGraph.Vertex.initialized` flags, so a re-registered component
   * does not add its transition edges a second time. Rediscovery also constructs fresh component
   * instances, so such a value can end up holding an instance that is no longer the one in the
   * graph.
   *
   * Visible for testing. The graph and the latch are both JVM-global, so a suite that registers
   * components of its own leaks them into every later suite in the same JVM. Such a suite must call
   * this when it finishes.
   */
  private[gluten] def clearAllForTesting(): Unit = {
    Component.clearForTesting()
    // Re-arms discovery. Unobservable in gluten-core, whose test classpath carries no
    // 'META-INF/gluten-components' file; the backend modules are the ones that need it.
    allComponentsLoaded.set(false)
  }
}
