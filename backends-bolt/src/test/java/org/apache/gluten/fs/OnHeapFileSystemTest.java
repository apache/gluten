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
package org.apache.gluten.fs;

import io.netty.util.internal.PlatformDependent;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// FIXME our checkstyle config doesn't allow "Suite" as suffix of Java tests
public class OnHeapFileSystemTest {
  private final JniFilesystem fs = OnHeapFileSystem.INSTANCE;

  @Test
  public void testRoundTrip() {
    final String path = "/foo";
    final String text = "HELLO WORLD";
    final long fileSize;
    JniFilesystem.WriteFile writeFile = fs.openFileForWrite(path);
    try {
      byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
      ByteBuffer buf = PlatformDependent.allocateDirectNoCleaner(bytes.length);
      buf.put(bytes);
      writeFile.append(bytes.length, PlatformDependent.directBufferAddress(buf));
      writeFile.flush();
      fileSize = writeFile.size();
      Assert.assertEquals(bytes.length, fileSize);
    } finally {
      writeFile.close();
    }

    JniFilesystem.ReadFile readFile = fs.openFileForRead(path);
    Assert.assertEquals(fileSize, readFile.size());
    ByteBuffer buf = PlatformDependent.allocateDirectNoCleaner((int) fileSize);
    readFile.pread(0, fileSize, PlatformDependent.directBufferAddress(buf));
    byte[] out = new byte[(int) fileSize];
    buf.get(out);
    String decoded = new String(out, StandardCharsets.UTF_8);

    Assert.assertEquals(text, decoded);
  }
}
