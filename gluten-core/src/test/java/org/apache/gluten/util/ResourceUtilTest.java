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
package org.apache.gluten.util;

import org.apache.gluten.utils.ResourceUtil;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.regex.Pattern;

public class ResourceUtilTest {
  @Test
  public void testFile() {
    // Use the class file of this test to verify the sanity of ResourceUtil.
    List<String> classes =
        ResourceUtil.getResources(
            "org", Pattern.compile("apache/gluten/util/ResourceUtilTest\\.class"));
    Assert.assertEquals(1, classes.size());
    Assert.assertEquals("apache/gluten/util/ResourceUtilTest.class", classes.get(0));
  }

  @Test
  public void testJar() {
    // Use the class file of Spark code to verify the sanity of ResourceUtil.
    List<String> classes =
        ResourceUtil.getResources("org", Pattern.compile("apache/spark/SparkContext\\.class"));
    Assert.assertEquals(1, classes.size());
    Assert.assertEquals("apache/spark/SparkContext.class", classes.get(0));
  }

  /** Verifies URI correctly decodes percent-encoded paths and preserves '+' characters. */
  @Test
  public void testUriDecodesPercentEncodedPaths() throws Exception {
    // %40 should decode to @
    URI uri1 = new URI("file:///path/user%40domain/file.jar");
    Assert.assertEquals("/path/user@domain/file.jar", uri1.getPath());

    // + should be preserved (not converted to space like URLDecoder does)
    URI uri2 = new URI("file:///path/file+name.jar");
    Assert.assertEquals("/path/file+name.jar", uri2.getPath());
  }
}
