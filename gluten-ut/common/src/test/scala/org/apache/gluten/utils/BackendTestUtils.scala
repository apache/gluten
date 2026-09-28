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
package org.apache.gluten.utils

import org.apache.gluten.backendsapi.BackendsApiManager

// FIXME: We mixed the use of "Utils" and "Util" in utility class naming
// FIXME: The utility methods "is...BackendLoaded" stands because we rely
//  on legacy backend-specific code in UTs. The new usages of the methods should
//  be better avoided. We'd remove them until the time is ripe.
object BackendTestUtils {
  def isVeloxBackendLoaded(): Boolean = {
    BackendsApiManager.getBackendName == "velox" // the same with VeloxBackend.BACKEND_NAME
  }

  def isCHBackendLoaded(): Boolean = {
    BackendsApiManager.getBackendName == "ch" // the same with CHBackend.BACKEND_NAME
  }

  def isBoltBackendLoaded(): Boolean = {
    BackendsApiManager.getBackendName == "bolt" // the same with BoltBackend.BACKEND_NAME
  }
}
