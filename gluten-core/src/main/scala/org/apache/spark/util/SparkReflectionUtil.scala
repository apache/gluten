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

import org.apache.spark.internal.Logging

object SparkReflectionUtil extends Logging {
  def getSimpleClassName(cls: Class[_]): String = {
    Utils.getSimpleName(cls)
  }

  def classForName[C](
      className: String,
      initialize: Boolean = true,
      noSparkClassLoader: Boolean = false): Class[C] = {
    Utils.classForName(className, initialize, noSparkClassLoader)
  }

  def isClassPresent(className: String): Boolean = {
    try {
      classForName(className)
      true
    } catch {
      case _: ClassNotFoundException =>
        false
      case e @ (_: NoClassDefFoundError | _: ExceptionInInitializerError) =>
        // Present but unusable (a version-skewed optional dependency missing a supertype,
        // or a failing static initializer): treat as not present, but log the cause so the
        // skew is diagnosable. Broader linkage errors (VerifyError, UnsupportedClassVersionError,
        // ...) are left to propagate, since they signal a genuinely broken build.
        logWarning(s"Class $className is present but could not be linked; treating it as absent", e)
        false
    }
  }
}
