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
package org.apache.gluten.table.runtime.stream.common;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Preset;

public class Velox4jEnvironment {
  private static class Holder {
    private static final Velox4jEnvironment INSTANCE = new Velox4jEnvironment();
  }

  private Velox4jEnvironment() {
    Velox4j.configure(Preset.KEY, Preset.FLINK);
    Velox4j.initialize();
  }

  public static Velox4jEnvironment getInstance() {
    return Holder.INSTANCE;
  }

  public static boolean initializeOnce() {
    Velox4jEnvironment instance = Holder.INSTANCE;
    return instance != null;
  }
}
