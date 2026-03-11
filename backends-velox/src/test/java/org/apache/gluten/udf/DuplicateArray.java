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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** UDF for duplicating array. */
@Description(
    name = "array_duplicate",
    value =
        "_FUNC_(array(obj1, obj2,...)) - "
            + "The function returns an array of the same type as every element"
            + "in array is duplicated.",
    extended =
        "Example:\n"
            + "  > SELECT _FUNC_(array('b', 'd')) FROM src LIMIT 1;\n"
            + "  ['b', 'b', 'd', 'd']")
public class DuplicateArray extends GenericUDF {

  ListObjectInspector arrayOI;

  public DuplicateArray() {}

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("Argument size of array_duplicate must be 1.");
    }

    arrayOI = (ListObjectInspector) arguments[0];
    return ObjectInspectorFactory.getStandardListObjectInspector(
        arrayOI.getListElementObjectInspector());
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object array = arguments[0].get();

    // If the array is empty, return back the empty array
    if (arrayOI.getListLength(array) == 0) {
      return Collections.emptyList();
    } else if (arrayOI.getListLength(array) < 0) {
      return null;
    }

    List<?> retArray = arrayOI.getList(array);
    List<Object> result = new ArrayList<>();
    retArray.forEach(
        element -> {
          result.add(element);
          result.add(element);
        });
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "array_duplicate";
  }
}
