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

import java.lang.annotation.Annotation;

/**
 * Lists the ScalaTest suites ScalaTest's Runner would discover on the given runpath, one per line
 * as {@code <suite>\t<comma-separated class annotations>}. Launched by run.py in source-file mode
 * with the recorded fork's classpath, so it works for both Scala 2.12 and 2.13.
 */
public class Discover {
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void main(String[] args) throws Exception {
    scala.collection.immutable.List runpath = scala.collection.immutable.Nil$.MODULE$;
    for (int i = args.length - 1; i >= 0; i--) {
      runpath = new scala.collection.immutable.$colon$colon(args[i], runpath);
    }
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    // The same (package-private in Scala, public in bytecode) helper Runner uses for discovery.
    scala.collection.Iterator suites =
        org.scalatest.tools.SuiteDiscoveryHelper.discoverSuiteNames(
                runpath, loader, scala.Option.empty())
            .iterator();
    StringBuilder out = new StringBuilder();
    while (suites.hasNext()) {
      String suite = (String) suites.next();
      StringBuilder annotations = new StringBuilder();
      for (Annotation annotation : Class.forName(suite, false, loader).getAnnotations()) {
        if (annotations.length() > 0) {
          annotations.append(',');
        }
        annotations.append(annotation.annotationType().getName());
      }
      out.append(suite).append('\t').append(annotations).append('\n');
    }
    System.out.print(out);
  }
}
