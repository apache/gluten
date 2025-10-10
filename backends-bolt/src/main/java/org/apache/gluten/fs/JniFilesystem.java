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

// Mirror of C++ side gluten::JniFileSystem, only for handling calls from C++ via JNI
public interface JniFilesystem {

  static JniFilesystem getFileSystem() {
    return OnHeapFileSystem.INSTANCE;
  }

  static boolean isCapableForNewFile(long size) {
    return getFileSystem().isCapableForNewFile0(size);
  }

  boolean isCapableForNewFile0(long size);

  ReadFile openFileForRead(String path); // todo read Map<String, String> as write options

  WriteFile openFileForWrite(String path); // todo read Map<String, String> as write options

  void remove(String path);

  void rename(String oldPath, String newPath, boolean overwrite);

  boolean exists(String path);

  String[] list(String path);

  void mkdir(String path);

  void rmdir(String path);

  interface ReadFile {
    void pread(long offset, long length, long buf); // uint64_t offset, uint64_t length, void* buf

    boolean shouldCoalesce();

    long size();

    long memoryUsage();

    long getNaturalReadSize();

    void close();
  }

  interface WriteFile {
    void append(long length, long buf);

    void flush();

    void close();

    long size();
  }
}
