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
package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.{SparkTestUtil, Utils}

import scala.collection.mutable

class ShuffleManagerRegistry private[ShuffleManagerRegistry] {
  import ShuffleManagerRegistry._
  private val all: mutable.ListBuffer[(LookupKey, String)] = mutable.ListBuffer()
  private val routerBuilders: mutable.Buffer[RouterBuilder] = mutable.Buffer()
  private val classDeDup: mutable.Set[String] = mutable.Set()

  // The shuffle manager class registered through this API later
  // will take higher precedence during lookup.
  def register(lookupKey: LookupKey, shuffleManagerClass: String): Unit = {
    val clazz = Utils.classForName(shuffleManagerClass)
    // Check the ShuffleManager implementation first, so a class that is not a
    // ShuffleManager at all (e.g. a non-ShuffleManager supertype of
    // GlutenShuffleManager) is rejected with the accurate message rather than
    // the recursion one.
    require(
      classOf[ShuffleManager].isAssignableFrom(clazz),
      s"Shuffle manager class to register is not an implementation of Spark ShuffleManager: " +
        s"$shuffleManagerClass"
    )
    // Reject GlutenShuffleManager itself, its subclasses, and any supertype it extends
    // (such as the ShuffleManager interface): routing a shuffle to one of these would make
    // GlutenShuffleManager recurse into itself.
    val isGlutenShuffleManagerRelated =
      clazz.isAssignableFrom(classOf[GlutenShuffleManager]) ||
        classOf[GlutenShuffleManager].isAssignableFrom(clazz)
    require(
      !isGlutenShuffleManagerRelated,
      s"Cannot register GlutenShuffleManager, any of its subclasses, or a supertype such as " +
        s"the ShuffleManager interface, because the router would then recurse into itself. " +
        s"Offending class: $shuffleManagerClass"
    )
    require(
      !classDeDup.contains(shuffleManagerClass),
      s"Shuffle manager class already registered: $shuffleManagerClass")
    this.synchronized {
      classDeDup += shuffleManagerClass
      (lookupKey -> shuffleManagerClass) +=: all
      // Invalidate all shuffle managers cached in each alive router builder instances.
      // Then, once the router builder is accessed, a new router will be forced to create.
      routerBuilders.foreach(_.invalidateCache())
    }
  }

  // Visible for testing.
  private[shuffle] def clear(): Unit = {
    assert(SparkTestUtil.isTesting)
    this.synchronized {
      classDeDup.clear()
      all.clear()
      routerBuilders.foreach(_.invalidateCache())
    }
  }

  private[shuffle] def newRouterBuilder(conf: SparkConf, isDriver: Boolean): RouterBuilder =
    this.synchronized {
      val out = new RouterBuilder(this, conf, isDriver)
      routerBuilders += out
      out
    }
}

object ShuffleManagerRegistry {
  private val instance = {
    val r = new ShuffleManagerRegistry()
    r.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = {
          dependency.getClass == classOf[ShuffleDependency[_, _, _]]
        }
      },
      classOf[SortShuffleManager].getName
    )
    r
  }

  def get(): ShuffleManagerRegistry = instance

  class RouterBuilder(registry: ShuffleManagerRegistry, conf: SparkConf, isDriver: Boolean) {
    private var router: Option[ShuffleManagerRouter] = None

    private[ShuffleManagerRegistry] def invalidateCache(): Unit = synchronized {
      router = None
    }

    private[shuffle] def getOrBuild(): ShuffleManagerRouter = synchronized {
      if (router.isEmpty) {
        val instances = registry.all.map(key => key._1 -> instantiate(key._2, conf, isDriver))
        router = Some(new ShuffleManagerRouter(new ShuffleManagerLookup(instances.toSeq)))
      }
      router.get
    }

    private def instantiate(clazz: String, conf: SparkConf, isDriver: Boolean): ShuffleManager = {
      Utils
        .instantiateSerializerOrShuffleManager[ShuffleManager](clazz, conf, isDriver)
    }
  }
}
