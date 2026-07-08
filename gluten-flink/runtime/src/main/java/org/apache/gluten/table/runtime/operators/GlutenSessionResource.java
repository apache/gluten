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

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;

import org.apache.flink.runtime.state.KeyedStateBackend;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

// Manage the session and resource for Velox.
class GlutenSessionResource {
  private Session session;
  private MemoryManager memoryManager;
  private BufferAllocator allocator;
  private KeyedStateBackend<?> keyedStateBackend;

  public GlutenSessionResource() {
    this.memoryManager = MemoryManager.create(AllocationListener.NOOP);
    this.session = Velox4j.newSession(memoryManager);
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  public void close() {
    if (session != null) {
      session.close();
      session = null;
    }
    if (memoryManager != null) {
      memoryManager.close();
      memoryManager = null;
    }
    if (allocator != null) {
      allocator.close();
      allocator = null;
    }
  }

  public Session getSession() {
    return session;
  }

  public MemoryManager getMemoryManager() {
    return memoryManager;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public KeyedStateBackend<?> getKeyedStateBackend() {
    return keyedStateBackend;
  }

  public void setKeyedStateBackend(KeyedStateBackend<?> keyedStateBackend) {
    this.keyedStateBackend = keyedStateBackend;
  }
}
