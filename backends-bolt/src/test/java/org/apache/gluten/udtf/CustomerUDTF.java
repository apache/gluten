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
package org.apache.gluten.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomerUDTF extends GenericUDTF {
  static final ObjectInspector STR_TYPE = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

  private PrimitiveObjectInspector arg0OI = null;

  private PrimitiveObjectInspector arg1OI = null;

  private final Map<Object, Object> mapResult = new HashMap<>();

  @Override
  public void close() throws HiveException {}

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    // Input
    if (argOIs.length != 2) {
      throw new UDFArgumentException(getClass().getSimpleName() + "() takes two arguments");
    }
    arg0OI = (PrimitiveObjectInspector) argOIs[0];
    arg1OI = (PrimitiveObjectInspector) argOIs[1];

    // Output
    final MapObjectInspector mapInspector =
        ObjectInspectorFactory.getStandardMapObjectInspector(STR_TYPE, STR_TYPE);
    List<String> fieldNames = Arrays.asList("strResult", "mapResult");
    List<ObjectInspector> fieldOIs = Arrays.asList(STR_TYPE, mapInspector);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    Object arg0 = arg0OI.getPrimitiveJavaObject(args[0]);
    Object arg1 = arg1OI.getPrimitiveJavaObject(args[1]);
    if (arg0 == null || arg1 == null) {
      return;
    }
    mapResult.clear();
    String[] strs = ((String) arg1).split(" ");
    mapResult.put(arg0, arg1);
    for (int i = 0; i < strs.length - 1; i += 2) {
      mapResult.put(strs[i], strs[i + 1]);
    }
    Object[] forwardObj = new Object[2];
    forwardObj[0] = arg0;
    forwardObj[1] = mapResult;
    forward(forwardObj);
  }
}
