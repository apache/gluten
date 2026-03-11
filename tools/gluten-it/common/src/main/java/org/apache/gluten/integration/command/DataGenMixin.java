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
package org.apache.gluten.integration.command;

import org.apache.gluten.integration.action.Action;
import org.apache.gluten.integration.action.DataGenOnly;

import picocli.CommandLine;

public class DataGenMixin {
  @CommandLine.Option(
      names = {"--data-gen"},
      description = "The strategy of data generation, accepted values: skip, once, always",
      defaultValue = "once")
  private String dataGenStrategy;

  public Action[] makeActions() {
    final DataGenOnly.Strategy strategy;
    switch (dataGenStrategy) {
      case "skip":
        strategy = DataGenOnly.Skip$.MODULE$;
        break;
      case "once":
        strategy = DataGenOnly.Once$.MODULE$;
        break;
      case "always":
        strategy = DataGenOnly.Always$.MODULE$;
        break;
      default:
        throw new IllegalArgumentException("Unexpected data-gen strategy: " + dataGenStrategy);
    }
    return new Action[] {new org.apache.gluten.integration.action.DataGenOnly(strategy)};
  }
}
