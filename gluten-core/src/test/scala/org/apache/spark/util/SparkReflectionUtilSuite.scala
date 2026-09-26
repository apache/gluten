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
package org.apache.spark.util

import org.scalatest.funsuite.AnyFunSuite

class SparkReflectionUtilSuite extends AnyFunSuite {

  test("isClassPresent returns true for a loadable class") {
    assert(SparkReflectionUtil.isClassPresent(classOf[String].getName))
  }

  test("isClassPresent returns false for an absent class") {
    // The ClassNotFoundException path: a name with no class on the classpath
    // must read as "not present" rather than escaping the probe.
    assert(!SparkReflectionUtil.isClassPresent("org.apache.gluten.DefinitelyDoesNotExist"))
  }

  test("isClassPresent treats a failing static initializer as not present") {
    // The probe initializes the class, so the failing static initializer
    // surfaces as ExceptionInInitializerError (a LinkageError); a second
    // probe of the same class surfaces NoClassDefFoundError. Both must read
    // as "not present" instead of escaping the probe.
    assert(!SparkReflectionUtil.isClassPresent(classOf[StaticInitThrower].getName))
    assert(!SparkReflectionUtil.isClassPresent(classOf[StaticInitThrower].getName))
  }
}
