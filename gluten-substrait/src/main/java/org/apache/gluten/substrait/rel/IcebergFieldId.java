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
package org.apache.gluten.substrait.rel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class IcebergFieldId implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String name;
  private final int fieldId;
  private final List<IcebergFieldId> children;

  public IcebergFieldId(String name, int fieldId, List<IcebergFieldId> children) {
    this.name = name;
    this.fieldId = fieldId;
    this.children = Collections.unmodifiableList(new ArrayList<>(children));
  }

  public String getName() {
    return name;
  }

  public int getFieldId() {
    return fieldId;
  }

  public List<IcebergFieldId> getChildren() {
    return children;
  }
}
