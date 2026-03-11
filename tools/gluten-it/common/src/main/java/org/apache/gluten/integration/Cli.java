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
package org.apache.gluten.integration;

import org.apache.gluten.integration.command.DataGenOnly;
import org.apache.gluten.integration.command.Parameterized;
import org.apache.gluten.integration.command.Queries;
import org.apache.gluten.integration.command.QueriesCompare;
import org.apache.gluten.integration.command.SparkShell;

import picocli.CommandLine;

import java.util.Arrays;

@CommandLine.Command(
    name = "gluten-it",
    mixinStandardHelpOptions = true,
    showDefaultValues = true,
    subcommands = {
      DataGenOnly.class,
      Queries.class,
      QueriesCompare.class,
      SparkShell.class,
      Parameterized.class
    },
    description = "Gluten integration test using various of benchmark's data and queries.")
public class Cli {
  private static String[] COMMANDLINE_ARGS = new String[0];

  private Cli() {}

  public static String[] args() {
    return Arrays.copyOf(COMMANDLINE_ARGS, COMMANDLINE_ARGS.length);
  }

  public static void main(String... args) {
    COMMANDLINE_ARGS = args;
    final CommandLine cmd = new CommandLine(new Cli());
    final int exitCode = cmd.execute(args);
    System.exit(exitCode);
  }
}
