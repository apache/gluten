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
package org.apache.gluten.udf;

public class UdfJniWrapper {

  public static native void registerFunctionSignatures();

  /**
   * Resolves the return type of a registry-declared scalar UDF against the Velox function registry.
   *
   * @param name the function name
   * @param argTypes a serialized substrait Type holding a struct of the actual argument types
   * @return a serialized substrait Type for the return type, or null if no signature binds
   */
  public static native byte[] resolveUdfType(String name, byte[] argTypes);

  /**
   * Resolves the return and intermediate types of a registry-declared UDAF against the Velox
   * aggregate registry.
   *
   * @param name the function name
   * @param argTypes a serialized substrait Type holding a struct of the actual argument types
   * @return a serialized substrait Type holding a struct of {returnType, intermediateType}, or null
   *     if no signature binds
   */
  public static native byte[] resolveUdafTypes(String name, byte[] argTypes);
}
