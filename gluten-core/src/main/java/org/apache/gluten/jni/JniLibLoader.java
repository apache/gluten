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
package org.apache.gluten.jni;

import org.apache.gluten.exception.GlutenException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class JniLibLoader {
  private static final Logger LOG = LoggerFactory.getLogger(JniLibLoader.class);

  private static final Set<String> LOADED_LIBRARY_PATHS = new HashSet<>();

  private final String workDir;
  private final Set<String> loadedLibraries = new HashSet<>();

  JniLibLoader(String workDir) {
    this.workDir = workDir;
  }

  /**
   * Returns the canonical, fully-dereferenced absolute path for the given library path.
   *
   * <p>Delegates to {@link Path#toRealPath} so symbolic-link chains whose stored targets are
   * relative (e.g. {@code lib.so -> lib.so.1 -> lib.so.1.2.3}) and symbolic-link cycles are handled
   * by the JDK rather than a hand-rolled loop that resolved {@code readSymbolicLink} results
   * against the process working directory and could spin forever on a cycle.
   *
   * <p>Package-private for testing.
   */
  static String toRealPath(String libPath) {
    try {
      Path realPath = Paths.get(libPath).toRealPath();
      LOG.info("Read real path {} for libPath {}", realPath, libPath);
      return realPath.toString();
    } catch (Exception e) {
      // Wrap any operational failure (IOException, InvalidPathException, SecurityException,
      // NPE, ...) as GlutenException so callers see a consistent type. Error subclasses
      // (OOME, StackOverflowError, ...) are intentionally allowed to propagate.
      throw new GlutenException("Error to read real path for libPath: " + libPath, e);
    }
  }

  private static void loadFromPath0(String libPath) {
    libPath = toRealPath(libPath);
    if (LOADED_LIBRARY_PATHS.contains(libPath)) {
      LOG.debug("Library in path {} has already been loaded, skipping", libPath);
    } else {
      System.load(libPath);
      LOADED_LIBRARY_PATHS.add(libPath);
      LOG.info("Library {} has been loaded using path-loading method", libPath);
    }
  }

  public static synchronized void loadFromPath(String libPath) {
    final File file = new File(libPath);
    if (!file.isFile() || !file.exists()) {
      throw new GlutenException("library at path: " + libPath + " is not a file or does not exist");
    }
    loadFromPath0(file.getAbsolutePath());
  }

  /**
   * Extracts the library resource {@code libPath} to {@code workDir} and loads it. Returns
   * immediately if {@code libPath} was already loaded by this instance.
   */
  public synchronized void load(String libPath) {
    loadAndCreateLink(libPath, null);
  }

  /**
   * Same contract as {@link #load(String)}, with the addition of creating a symbolic link named
   * {@code linkName} in {@code workDir} pointing at the extracted library. Returns immediately if
   * {@code libPath} was already loaded by this instance. Passing a null {@code linkName} skips the
   * link creation.
   */
  public synchronized void loadAndCreateLink(String libPath, String linkName) {
    try {
      if (loadedLibraries.contains(libPath)) {
        LOG.debug("Library {} has already been loaded, skipping", libPath);
        return;
      }
      File file = moveToWorkDir(workDir, libPath);
      loadWithLink(file.getAbsolutePath(), linkName);
      loadedLibraries.add(libPath);
      LOG.info("Successfully loaded library {}", libPath);
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }

  private File moveToWorkDir(String workDir, String libraryToLoad) throws IOException {
    // final File temp = File.createTempFile(workDir, libraryToLoad);
    final Path libPath = Paths.get(workDir, libraryToLoad);
    if (Files.exists(libPath)) {
      Files.delete(libPath);
    }
    final File temp = libPath.toFile();
    if (!temp.getParentFile().exists()) {
      temp.getParentFile().mkdirs();
    }
    try (InputStream is = JniLibLoader.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      }
      try {
        Files.copy(is, temp.toPath());
      } catch (Exception e) {
        throw new GlutenException(e);
      }
    }
    return temp;
  }

  private void loadWithLink(String libPath, String linkName) throws IOException {
    loadFromPath0(libPath);
    LOG.info("Library {} has been loaded", libPath);
    if (linkName != null) {
      Path target = Paths.get(libPath);
      Path link = Paths.get(workDir, linkName);
      if (Files.exists(link)) {
        LOG.info("Symbolic link already exists for library {}, deleting", libPath);
        Files.delete(link);
      }
      Files.createSymbolicLink(link, target);
      LOG.info("Symbolic link {} created for library {}", link, libPath);
    }
  }
}
