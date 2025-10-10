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

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.spark.util.SparkShutdownManagerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.runtime.BoxedUnit;

/**
 * LoadXXX methods in the utility prevents reloading of a library internally. It's not necessary for
 * caller to manage a loaded library list.
 */
public class BoltJniLibLoader {
  private static final Logger LOG = LoggerFactory.getLogger(BoltJniLibLoader.class);

  private static final Set<String> LOADED_LIBRARY_PATHS = new HashSet<>();
  private static final Set<String> REQUIRE_UNLOAD_LIBRARY_PATHS = new LinkedHashSet<>();

  public static final int RTLD_GLOBAL = 0x00100;
  public static final int RTLD_LAZY = 0x00001;
  public static final int RTLD_LOCAL = 0x00000;

  static {
    SparkShutdownManagerUtil.addHookForLibUnloading(
        () -> {
          forceUnloadAll();
          return BoxedUnit.UNIT;
        });
  }

  private final String workDir;
  private final Set<String> loadedLibraries = new HashSet<>();
  private final Lock sync = new ReentrantLock();

  public BoltJniLibLoader(String workDir) {
    this.workDir = workDir;
  }

  public static native boolean nativeLoadLibrary(String lib, int rtldFlags)
      throws UnsatisfiedLinkError;

  public static synchronized void forceUnloadAll() {
    List<String> loaded = new ArrayList<>(REQUIRE_UNLOAD_LIBRARY_PATHS);
    Collections.reverse(loaded); // use reversed order to unload
    loaded.forEach(BoltJniLibLoader::unloadFromPath);
  }

  private static synchronized void loadFromPath0(String libPath, boolean requireUnload) {
    if (LOADED_LIBRARY_PATHS.contains(libPath)) {
      LOG.debug("Library in path {} has already been loaded, skipping", libPath);
    } else {
      System.load(libPath);
      LOADED_LIBRARY_PATHS.add(libPath);
      LOG.info("Library {} has been loaded using path-loading method", libPath);
    }
    if (requireUnload) {
      REQUIRE_UNLOAD_LIBRARY_PATHS.add(libPath);
    }
  }

  public static void loadFromPath(String libPath, boolean requireUnload) {
    final File file = new File(libPath);
    if (!file.isFile() || !file.exists()) {
      throw new GlutenException("library at path: " + libPath + " is not a file or does not exist");
    }
    loadFromPath0(file.getAbsolutePath(), requireUnload);
  }

  public void mapAndLoad(String unmappedLibName, boolean requireUnload) {
    newTransaction().mapAndLoad(unmappedLibName, requireUnload).commit();
  }

  public void mapAndLoadWithRtldFlag(String libName, boolean requireUnload, int rtldFlags) {
    newTransaction().mapAndLoadWithRtldFlag(libName, requireUnload, rtldFlags).commit();
  }

  public void mapAndLoadWithRtldFlag(
      String libName, boolean requireUnload, int rtldFlags, boolean fromResourceFile) {
    newTransaction()
        .mapAndLoadWithRtldFlag(libName, requireUnload, rtldFlags, fromResourceFile)
        .commit();
  }

  public void load(String libName, boolean requireUnload) {
    newTransaction().load(libName, requireUnload).commit();
  }

  public void load(String libName, boolean requireUnload, int rtldFlags) {
    newTransaction().load(libName, requireUnload, rtldFlags).commit();
  }

  public void loadAndCreateLink(String libName, String linkName, boolean requireUnload) {
    newTransaction().loadAndCreateLink(libName, linkName, requireUnload).commit();
  }

  public void loadAndCreateLink(String libName, String linkName) {
    loadAndCreateLink(libName, linkName, true);
  }

  public JniLoadTransaction newTransaction() {
    return new JniLoadTransaction();
  }

  public static synchronized void unloadFromPath(String libPath) {
    if (!LOADED_LIBRARY_PATHS.remove(libPath)) {
      LOG.warn("Library {} was not loaded or alreay unloaded:", libPath);
      return;
    }

    REQUIRE_UNLOAD_LIBRARY_PATHS.remove(libPath);

    try {
      while (Files.isSymbolicLink(Paths.get(libPath))) {
        libPath = Files.readSymbolicLink(Paths.get(libPath)).toString();
      }

      ClassLoader classLoader = BoltJniLibLoader.class.getClassLoader();
      Field field = ClassLoader.class.getDeclaredField("nativeLibraries");
      field.setAccessible(true);
      @SuppressWarnings("unchecked")
      Vector<Object> libs = (Vector<Object>) field.get(classLoader);
      Iterator it = libs.iterator();
      while (it.hasNext()) {
        Object object = it.next();
        Field[] fs = object.getClass().getDeclaredFields();
        for (int k = 0; k < fs.length; k++) {
          if (fs[k].getName().equals("name")) {
            fs[k].setAccessible(true);

            String verbosePath = fs[k].get(object).toString();
            File verboseFile = new File(verbosePath);
            String verboseFileName = verboseFile.getName();
            File libFile = new File(libPath);
            String libFileName = libFile.getName();

            if (verboseFileName.equals(libFileName)) {
              Method finalize = object.getClass().getDeclaredMethod("finalize");
              finalize.setAccessible(true);
              finalize.invoke(object);
            }
          }
        }
      }
    } catch (Throwable th) {
      LOG.error("Unload native library error: ", th);
    }
  }

  private static final class LoadRequest {
    final String libName;
    final String linkName;
    final boolean requireUnload;

    // dlopen RTLD_GLOBAL | RTLD_LAZY
    final int rtldFlags;

    // TRUE to load from resource file, otherwise from current dir
    final boolean fromResourceFile;

    private LoadRequest(String libName, String linkName, boolean requireUnload, int rtldFlags) {
      this(libName, linkName, requireUnload, rtldFlags, true);
    }

    private LoadRequest(
        String libName,
        String linkName,
        boolean requireUnload,
        int rtldFlags,
        boolean fromResourceFile) {
      this.libName = libName;
      this.linkName = linkName;
      this.requireUnload = requireUnload;

      this.rtldFlags = rtldFlags;

      this.fromResourceFile = fromResourceFile;
    }
  }

  private static final class LoadAction {
    final String libName;
    final String linkName;
    final boolean requireUnload;
    final File file;
    final int rtldFlags;

    private LoadAction(
        String libName, String linkName, boolean requireUnload, File file, int rtldFlags) {
      this.libName = libName;
      this.linkName = linkName;
      this.requireUnload = requireUnload;
      this.file = file;
      this.rtldFlags = rtldFlags;
    }

    public boolean requireLinking() {
      return !Objects.isNull(linkName);
    }
  }

  public class JniLoadTransaction {
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final Map<String, LoadRequest> toLoad = new LinkedHashMap<>(); // ordered

    private JniLoadTransaction() {
      BoltJniLibLoader.this.sync.lock();
    }

    public JniLoadTransaction mapAndLoad(String unmappedLibName, boolean requireUnload) {
      try {
        final String mappedLibName = System.mapLibraryName(unmappedLibName);
        load(mappedLibName, requireUnload);
        return this;
      } catch (Exception e) {
        abort();
        throw new GlutenException(e);
      }
    }

    public JniLoadTransaction mapAndLoadWithRtldFlag(
        String unmappedLibName, boolean requireUnload, int rtldFlags) {
      return mapAndLoadWithRtldFlag(unmappedLibName, requireUnload, rtldFlags, true);
    }

    public JniLoadTransaction mapAndLoadWithRtldFlag(
        String unmappedLibName, boolean requireUnload, int rtldFlags, boolean fromResourceFile) {
      try {
        final String mappedLibName = System.mapLibraryName(unmappedLibName);
        toLoad.put(
            mappedLibName,
            new LoadRequest(mappedLibName, null, requireUnload, rtldFlags, fromResourceFile));
        return this;
      } catch (Exception e) {
        abort();
        throw new GlutenException(e);
      }
    }

    public JniLoadTransaction load(String libName, boolean requireUnload) {
      try {
        toLoad.put(
            libName, new LoadRequest(libName, null, requireUnload, BoltJniLibLoader.RTLD_LAZY));
        return this;
      } catch (Exception e) {
        abort();
        throw new GlutenException(e);
      }
    }

    public JniLoadTransaction load(String libName, boolean requireUnload, int rtldFlags) {
      try {
        toLoad.put(libName, new LoadRequest(libName, null, requireUnload, rtldFlags));
        return this;
      } catch (Exception e) {
        abort();
        throw new GlutenException(e);
      }
    }

    public JniLoadTransaction loadAndCreateLink(
        String libName, String linkName, boolean requireUnload) {
      try {
        toLoad.put(
            libName, new LoadRequest(libName, linkName, requireUnload, BoltJniLibLoader.RTLD_LAZY));
        return this;
      } catch (Exception e) {
        abort();
        throw new GlutenException(e);
      }
    }

    public void commit() {
      try {
        terminate();
        toLoad.entrySet().stream()
            .flatMap(
                e -> {
                  try {
                    final LoadRequest req = e.getValue();
                    if (loadedLibraries.contains(req.libName)) {
                      LOG.debug("Library {} has already been loaded, skipping", req.libName);
                      return Stream.empty();
                    }
                    // load only libraries not loaded yet
                    File file;
                    String libraryToLoad = req.libName;
                    if (req.fromResourceFile) {
                      file = moveToWorkDir(workDir, libraryToLoad);
                    } else {
                      String currentDir =
                          System.getenv()
                              .getOrDefault(
                                  ApplicationConstants.Environment.PWD.key(),
                                  System.getProperty("user.dir"));
                      file = new File(currentDir + "/" + libraryToLoad);
                    }
                    LOG.info("try to load lib from " + file.getAbsolutePath());
                    return Stream.of(
                        new LoadAction(
                            req.libName, req.linkName, req.requireUnload, file, req.rtldFlags));
                  } catch (IOException ex) {
                    throw new GlutenException(ex);
                  }
                })
            .collect(Collectors.toList())
            .forEach(
                e -> {
                  try {
                    loadWithLink(workDir, e);
                    loadedLibraries.add(e.libName);
                    LOG.info("Successfully loaded library {}", e.libName);
                  } catch (Exception ex) {
                    throw new GlutenException(ex);
                  }
                });
      } finally {
        BoltJniLibLoader.this.sync.unlock();
      }
    }

    public void abort() {
      try {
        terminate();
        // do nothing as of now
      } finally {
        BoltJniLibLoader.this.sync.unlock();
      }
    }

    private void terminate() {
      if (!finished.compareAndSet(false, true)) {
        throw new IllegalStateException();
      }
    }

    private File moveToWorkDir(String workDir, String libraryToLoad) throws IOException {
      // final File temp = File.createTempFile(workDir, libraryToLoad);
      final Path libPath = Paths.get(workDir + "/" + libraryToLoad);
      if (Files.exists(libPath)) {
        Files.delete(libPath);
      }
      final File temp = new File(workDir + "/" + libraryToLoad);
      if (!temp.getParentFile().exists()) {
        temp.getParentFile().mkdirs();
      }

      final String libToLoadPath = libraryToLoad;
      try (InputStream is =
          BoltJniLibLoader.class.getClassLoader().getResourceAsStream(libToLoadPath)) {
        if (is == null) {
          throw new FileNotFoundException(libToLoadPath);
        }
        try {
          Files.copy(is, temp.toPath());
        } catch (Exception e) {
          throw new GlutenException(e);
        }
      }
      return temp;
    }

    private void loadWithLink(String workDir, LoadAction req) throws IOException {
      String libPath = req.file.getAbsolutePath();
      LOG.info("Loading library {} via dlopen wiht flag {}.", libPath, req.rtldFlags);
      if ((req.rtldFlags & BoltJniLibLoader.RTLD_GLOBAL) != 0) {

        // A hacky workround. JNI sucks..
        //  Use Java 21's Foreign Function and Memory in future?
        //
        // 1. we need to expose the Bolt's symbols, so that llvm ir module can
        //    call Bolt's C/C++ functions directly.  JDK's System.loadLibrary() use dlopen()
        //    to load libraries, however there is no way to pass in any RTLD_xxxx flags.
        //
        // 2. If we use dlopen with RTLD_GLOBAL flag directly, JVM won't know on which
        //    library to search the symbols.
        //    Please refer to HotSpot's fuction: NativeLookup::lookup_style
        //
        // 3. It is safe to call dlopen('same_lib').
        //    If the same shared object is opened again with dlopen(),
        //    the same object handle is returned.
        //    Refere to: https://man7.org/linux/man-pages/man3/dlopen.3.html
        //
        //  Here, firstly load library via dlopen(), then call System.loadLibrary()
        //  to load it again. System.loadLibrary() will register the library path to JVM.
        LOG.info("Loading library {} via dlopen with flag {}.", libPath, req.rtldFlags);
        BoltJniLibLoader.nativeLoadLibrary(libPath, req.rtldFlags);

        loadFromPath0(libPath, req.requireUnload);
      } else {
        loadFromPath0(libPath, req.requireUnload);
      }

      LOG.info("Library {} has been loaded", libPath);
      if (!req.requireLinking()) {
        LOG.debug("Symbolic link not required for library {}, skipping", libPath);
        return;
      }
      // create link
      Path target = Paths.get(req.file.getPath());
      Path link = Paths.get(workDir, req.linkName);
      if (Files.exists(link)) {
        LOG.info("Symbolic link already exists for library {}, deleting", libPath);
        Files.delete(link);
      }
      Files.createSymbolicLink(link, target);
      LOG.info("Symbolic link {} created for library {}", link, libPath);
    }
  }
}
