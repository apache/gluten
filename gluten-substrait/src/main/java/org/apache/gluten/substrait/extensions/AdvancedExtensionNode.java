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
package org.apache.gluten.substrait.extensions;

import com.google.protobuf.Any;
import io.substrait.proto.AdvancedExtension;

import java.io.Serializable;

public class AdvancedExtensionNode implements Serializable {

  // An optimization is helpful information that don't influence semantics. May
  // be ignored by a consumer.
  private final Any optimization;

  // An enhancement alter semantics. Cannot be ignored by a consumer.
  private final Any enhancement;

  public AdvancedExtensionNode(Any enhancement) {
    this.optimization = null;
    this.enhancement = enhancement;
  }

  public AdvancedExtensionNode(Any optimization, Any enhancement) {
    this.optimization = optimization;
    this.enhancement = enhancement;
  }

  public Any getOptimization() {
    return optimization;
  }

  public Any getEnhancement() {
    return enhancement;
  }

  public AdvancedExtension toProtobuf() {
    AdvancedExtension.Builder extensionBuilder = AdvancedExtension.newBuilder();
    if (optimization != null) {
      extensionBuilder.setOptimization(optimization);
    }
    if (enhancement != null) {
      extensionBuilder.setEnhancement(enhancement);
    }
    return extensionBuilder.build();
  }
}
