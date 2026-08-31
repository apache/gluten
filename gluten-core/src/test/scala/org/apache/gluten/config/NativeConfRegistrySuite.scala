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
package org.apache.gluten.config

import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.MapProvider

import org.scalatest.funsuite.AnyFunSuite

import java.util.Locale

class NativeConfRegistrySuite extends AnyFunSuite {

  private def withRegisteredKeys(keys: String*)(f: => Unit): Unit = {
    try f
    finally keys.foreach(NativeConfRegistry.unregister)
  }

  // Other suites in the same JVM may have loaded conf objects (e.g. GlutenCoreConfig) whose
  // initializers register real native confs. Restrict select results to the keys under test so
  // assertions are not affected by those global registrations.
  private def selectRuntime(conf: Map[String, String], keys: String*): Map[String, String] = {
    NativeConfRegistry.selectRuntimeConf(conf).filter { case (k, _) => keys.contains(k) }
  }

  private def selectBackend(conf: Map[String, String], keys: String*): Map[String, String] = {
    NativeConfRegistry.selectBackendConf(conf).filter { case (k, _) => keys.contains(k) }
  }

  /** A conf object exercising the four declaration methods, as a module would declare them. */
  private object TestConfig extends ConfigRegistry {
    override def get: GlutenCoreConfig = GlutenCoreConfig.get

    val MODIFIABLE = "spark.gluten.test.native.modifiable.conf"
    val STATIC = "spark.gluten.test.native.static.conf"
    val JVM_ONLY = "spark.gluten.test.native.jvmOnly.conf"
    val FOREIGN_MODIFIABLE = "spark.gluten.test.native.foreign.modifiable.conf"
    val FOREIGN_STATIC = "spark.gluten.test.native.foreign.static.conf"

    def declareModifiable(): Unit =
      buildConf(MODIFIABLE).passToNative().booleanConf.createWithDefault(true)

    def declareStatic(): Unit =
      buildStaticConf(STATIC).passToNative().intConf.createWithDefault(1)

    def declareJvmOnly(): Unit = buildConf(JVM_ONLY).booleanConf.createWithDefault(false)

    def declareForeignModifiable(): Unit =
      registerConf(FOREIGN_MODIFIABLE).booleanConf.passToNative().createWithDefault(false)

    def declareForeignStatic(): Unit =
      registerStaticConf(FOREIGN_STATIC).stringConf.passToNative().createWithDefault("snappy")

    def declareBytesDefault(key: String): Unit =
      buildConf(key).passToNative().bytesConf(ByteUnit.BYTE).createWithDefaultString("1KB")

    def declareDynamicDefault(key: String, defaultFunc: () => String): Unit =
      buildConf(key).passToNative().stringConf.createWithDefaultFunction(defaultFunc)

    def declareForeignFallback(key: String): ConfigEntryForeignFallback[String] =
      registerConf(key)
        .stringConf
        .passToNative()
        .fallbackConf("spark.gluten.test.native.fallbackTarget.conf", "sparkDefault")

    def declareForeignStaticWithDefault(key: String): Unit =
      registerStaticConf(key).stringConf.passToNative().createWithDefault("declaredDefault")

    def declareTransform(key: String): Unit =
      registerConf(key)
        .stringConf
        .transform(_.toUpperCase(Locale.ROOT))
        .passToNative()
        .createOptional

    def declareTypedBytesWithDefault(key: String): Unit =
      registerConf(key)
        .bytesConf(ByteUnit.KiB)
        .passToNative()
        .createWithDefaultString("32k")

    def declareForeign(key: String): Unit =
      registerConf(key).stringConf.passToNative().createOptional

    /**
     * Foreign key declaring `createWithForeignDefault`: deliver the foreign-declared default per
     * delivery.
     */
    def declareForeignWithOwnerDefault(key: String): Unit =
      registerConf(key).stringConf.passToNative().createWithForeignDefault

    /** A Gluten conf may not use createWithForeignDefault - it has no foreign declaration. */
    def declareGlutenWithOwnerDefault(key: String): Unit =
      buildConf(key).stringConf.passToNative().createWithForeignDefault

    def declareForeignWithoutPassToNative(key: String): Unit =
      registerConf(key).stringConf.createOptional

    def declareForeignTwice(key: String): Unit = {
      registerStaticConf(key).stringConf.passToNative().createOptional
      registerStaticConf(key).stringConf.passToNative().createOptional
    }
  }

  test("a modifiable conf reaches both channels, a static one only the backend channel") {
    import TestConfig._
    withRegisteredKeys(MODIFIABLE, STATIC) {
      declareModifiable()
      declareStatic()
      declareJvmOnly()

      assert(NativeConfRegistry.isRuntimeKey(MODIFIABLE))
      assert(NativeConfRegistry.isBackendKey(MODIFIABLE))

      assert(!NativeConfRegistry.isRuntimeKey(STATIC))
      assert(NativeConfRegistry.isBackendKey(STATIC))

      assert(!NativeConfRegistry.isRuntimeKey(JVM_ONLY))
      assert(!NativeConfRegistry.isBackendKey(JVM_ONLY))
    }
  }

  test("registerConf and registerStaticConf follow the same scope rule") {
    import TestConfig._
    withRegisteredKeys(FOREIGN_MODIFIABLE, FOREIGN_STATIC) {
      declareForeignModifiable()
      declareForeignStatic()

      assert(NativeConfRegistry.isRuntimeKey(FOREIGN_MODIFIABLE))
      assert(NativeConfRegistry.isBackendKey(FOREIGN_MODIFIABLE))

      assert(!NativeConfRegistry.isRuntimeKey(FOREIGN_STATIC))
      assert(NativeConfRegistry.isBackendKey(FOREIGN_STATIC))

      // A foreign conf's declared default is what native gets when the user did not set the key,
      // and it is not registered as a Gluten entry since Spark / Hadoop owns the key.
      assert(
        selectRuntime(Map.empty[String, String], FOREIGN_MODIFIABLE) ===
          Map(FOREIGN_MODIFIABLE -> "false"))
      assert(
        selectBackend(Map.empty[String, String], FOREIGN_STATIC) ===
          Map(FOREIGN_STATIC -> "snappy"))
      assert(ConfigRegistry.findEntry(FOREIGN_MODIFIABLE).isEmpty)
    }
  }

  test("an unset conf is delivered with its declared default, in parsed form") {
    val bytesKey = "spark.gluten.test.native.withBytesDefault.conf"
    withRegisteredKeys(bytesKey) {
      // A bytes conf default is passed as the parsed value in bytes, not the raw string.
      TestConfig.declareBytesDefault(bytesKey)
      assert(selectRuntime(Map.empty[String, String], bytesKey) === Map(bytesKey -> "1024"))
    }
  }

  test("a dynamic default is re-resolved on each delivery") {
    val key = "spark.gluten.test.native.dynamicDefault.conf"
    withRegisteredKeys(key) {
      // Mirrors a Spark conf whose default follows mutable JVM state, e.g.
      // `spark.sql.session.timeZone` defaulting to the current JVM default time zone. A default
      // snapshotted at declaration would keep delivering the stale value.
      var current = "first"
      TestConfig.declareDynamicDefault(key, () => current)
      assert(selectRuntime(Map.empty[String, String], key) === Map(key -> "first"))
      current = "second"
      assert(selectRuntime(Map.empty[String, String], key) === Map(key -> "second"))
      // A user-set value still wins over the dynamic default.
      assert(selectRuntime(Map(key -> "userValue"), key) === Map(key -> "userValue"))
    }
  }

  test("a conf can fall back to one owned by Spark") {
    val key = "spark.gluten.test.native.withFallback.conf"
    val fallbackKey = "spark.gluten.test.native.fallbackTarget.conf"
    withRegisteredKeys(key) {
      val entry = TestConfig.declareForeignFallback(key)

      // Neither key set: the Spark default applies.
      assert(entry.readWithSource(new MapProvider(Map.empty[String, String])) ===
        (("sparkDefault", false)))
      // Only the Spark key set: its value is inherited, and reported as not explicitly set.
      assert(entry.readWithSource(new MapProvider(Map(fallbackKey -> "inherited"))) ===
        (("inherited", false)))
      // This key set: it wins and is reported as explicitly set.
      assert(
        entry.readWithSource(new MapProvider(Map(fallbackKey -> "inherited", key -> "own"))) ===
          (("own", true)))
      // Only a user-set value is delivered to native side; the fallback is a JVM-side notion.
      assert(selectRuntime(Map(key -> "own"), key) === Map(key -> "own"))
      assert(selectRuntime(Map.empty[String, String], key).isEmpty)
    }
  }

  test("a foreign key backed by a fallback entry delivers nothing rather than a sentinel") {
    // The owner renders an entry's default for display, not for consumption: `FallbackConfigEntry`
    // yields "<value of other.key>" and `OptionalConfigEntry` yields "<undefined>". Neither may
    // reach native as a value. `spark.locality.wait.process` is `fallbackConf(LOCALITY_WAIT)` and
    // `spark.driver.log.dfsDir` is `createOptional` in every supported Spark version. Both are
    // declared with `createWithForeignDefault` here to force the foreign-default resolution path.
    Seq("spark.locality.wait.process", "spark.driver.log.dfsDir").foreach {
      key =>
        withRegisteredKeys(key) {
          TestConfig.declareForeignWithOwnerDefault(key)
          val selected = selectRuntime(Map.empty[String, String], key)
          assert(
            selected.isEmpty,
            s"$key must not be delivered, but got ${selected.get(key)}"
          )
          // A user-set value is still delivered.
          assert(selectRuntime(Map(key -> "3s"), key) === Map(key -> "3s"))
        }
    }
  }

  test("createOptional on a foreign key delivers nothing even when the owner declares a default") {
    // Even though `spark.sql.legacy.sizeOfNull` has an foreign-declared default of `true`,
    // `createOptional` means "do not deliver anything when unset" - native's own fallback handles
    // it.
    val key = "spark.sql.legacy.sizeOfNull"
    withRegisteredKeys(key) {
      TestConfig.declareForeign(key)
      assert(selectRuntime(Map.empty[String, String], key).isEmpty)
      assert(selectRuntime(Map(key -> "false"), key) === Map(key -> "false"))
    }
  }

  test("createWithForeignDefault delivers the foreign-declared default per delivery") {
    // Uses `spark.sql.legacy.sizeOfNull` (foreign-declared default: "true", stable
    // across supported Spark versions) to verify the foreign-default resolution path.
    // Value is re-resolved each call, so a change to the foreign entry (or in the JVM
    // state a dynamic default reads) would take effect immediately.
    val key = "spark.sql.legacy.sizeOfNull"
    withRegisteredKeys(key) {
      TestConfig.declareForeignWithOwnerDefault(key)
      assert(selectRuntime(Map.empty[String, String], key) === Map(key -> "true"))
      // A user-set value overrides the foreign-declared default.
      assert(selectRuntime(Map(key -> "false"), key) === Map(key -> "false"))
    }
  }

  test("createWithForeignDefault on a Gluten-owned conf is rejected at declaration") {
    val key = "spark.gluten.test.native.glutenOwnedWithOwnerDefault.conf"
    withRegisteredKeys(key) {
      assertThrows[IllegalArgumentException] {
        TestConfig.declareGlutenWithOwnerDefault(key)
      }
    }
  }

  test("a user-set value is delivered through the conf's own value converter") {
    val stringKey = "spark.gluten.test.native.transform.conf"
    withRegisteredKeys(stringKey) {
      // A conf declared with `.transform(toUpperCase)` upper-cases the user's value
      // at delivery,
      // through the entry's own converter, not through a separate transform pipeline.
      TestConfig.declareTransform(stringKey)
      assert(selectRuntime(Map(stringKey -> "legacy"), stringKey) === Map(stringKey -> "LEGACY"))
      // No default declared, so nothing is delivered when unset.
      assert(selectRuntime(Map.empty[String, String], stringKey).isEmpty)
    }

    val sizeKey = "spark.gluten.test.native.typedBytes.conf"
    withRegisteredKeys(sizeKey) {
      // A conf declared with `bytesConf(unit)` mirrors Spark's own bytes-conf semantics: the
      // delivered value is in the declared unit (KiB here). A declared default may be a raw string
      // ("32k"), which goes through the same converter as a user-set value.
      TestConfig.declareTypedBytesWithDefault(sizeKey)
      assert(selectRuntime(Map.empty[String, String], sizeKey) === Map(sizeKey -> "32"))
      assert(selectRuntime(Map(sizeKey -> "64k"), sizeKey) === Map(sizeKey -> "64"))
      // Bare numbers are interpreted in the declared unit, matching Spark's `byteStringAs`.
      assert(selectRuntime(Map(sizeKey -> "128"), sizeKey) === Map(sizeKey -> "128"))
    }
  }

  test("a foreign conf not marked with passToNative is rejected") {
    val key = "spark.gluten.test.native.foreignWithoutPass.conf"
    withRegisteredKeys(key) {
      assertThrows[IllegalArgumentException] {
        TestConfig.declareForeignWithoutPassToNative(key)
      }
    }
  }

  test("confs not set by user are skipped unless a default is declared") {
    val noDefaultKey = "spark.gluten.test.select.noDefault.conf"
    val withDefaultKey = "spark.gluten.test.select.withDefault.conf"
    withRegisteredKeys(noDefaultKey, withDefaultKey) {
      TestConfig.declareTransform(noDefaultKey)
      TestConfig.declareForeignStaticWithDefault(withDefaultKey)

      val selected = selectBackend(Map.empty[String, String], noDefaultKey, withDefaultKey)
      assert(selected === Map(withDefaultKey -> "declaredDefault"))

      // A user-set value takes precedence over the declared default.
      val selected2 =
        selectBackend(Map(withDefaultKey -> "userValue"), noDefaultKey, withDefaultKey)
      assert(selected2 === Map(withDefaultKey -> "userValue"))
    }
  }

  test("declaring the same key twice for the same scope is not allowed") {
    val key = "spark.gluten.test.native.declaredTwice.conf"
    withRegisteredKeys(key) {
      assertThrows[IllegalArgumentException] {
        TestConfig.declareForeignTwice(key)
      }
    }
  }
}
