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
package org.apache.gluten.table.runtime.operators;

import io.github.zhztheplayer.velox4j.session.Session;

import java.util.HashMap;
import java.util.Map;

/** Task-thread-local Gluten runtime context used by operators and serializers. */
public final class GlutenTaskSessionContext {
  private static final ThreadLocal<Map<String, GlutenSessionResource>> SESSION_RESOURCES =
      ThreadLocal.withInitial(HashMap::new);

  private GlutenTaskSessionContext() {}

  public static GlutenSessionResource getSessionResource(String id) {
    return SESSION_RESOURCES.get().get(id);
  }

  public static void addSessionResource(String id, GlutenSessionResource sessionResource) {
    SESSION_RESOURCES.get().put(id, sessionResource);
  }

  public static void registerSessionResource(String operatorId, GlutenSessionResource resource) {
    addSessionResource(operatorId, resource);
  }

  public static void unregisterSessionResource(String operatorId) {
    Map<String, GlutenSessionResource> resources = SESSION_RESOURCES.get();
    resources.remove(operatorId);
    if (resources.isEmpty()) {
      SESSION_RESOURCES.remove();
    }
  }

  public static Session getSession(String operatorId) {
    Map<String, GlutenSessionResource> resources = SESSION_RESOURCES.get();
    GlutenSessionResource resource = resources.get(operatorId);
    if (resource == null) {
      throw new IllegalStateException(
          "No Gluten session registered on the current task thread for operator "
              + operatorId
              + ". Registered operators: "
              + resources.keySet());
    }
    Session session = resource.getSession();
    if (session == null) {
      throw new IllegalStateException(
          "Gluten session is already closed for operator " + operatorId);
    }
    return session;
  }
}
