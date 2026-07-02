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

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/** Tests for the symlink resolution path of {@link JniLibLoader#toRealPath(String)}. */
public class JniLibLoaderTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void assumeSymlinkSupport() throws Exception {
    // Skip cleanly on filesystems / platforms that disallow symlinks or refuse to resolve them
    // (FAT/SMB, Windows without privilege, some FUSE mounts, ...). The probe mirrors what the
    // tests actually exercise — create a relative-target symlink AND walk it via toRealPath —
    // so an environment that permits one but not the other still skips instead of red-failing.
    File probeDir = tempFolder.newFolder("symlink-probe");
    try {
      Path target = probeDir.toPath().resolve("target");
      Files.createFile(target);
      Path link = probeDir.toPath().resolve("link");
      Files.createSymbolicLink(link, Paths.get("target"));
      link.toRealPath();
    } catch (UnsupportedOperationException | SecurityException | java.io.IOException e) {
      Assume.assumeNoException("filesystem does not support symbolic-link resolution", e);
    }
  }

  @Test
  public void toRealPathReturnsCanonicalAbsoluteForRegularFile() throws Exception {
    // Force the input and the canonical real path to differ on every platform: address the file
    // via a symlinked parent directory. A regression that returned the input verbatim would
    // therefore fail on Linux (where /tmp is not a symlink) and not just on macOS (where /var is).
    File realDir = tempFolder.newFolder("real-dir");
    File regular = new File(realDir, "plain.so");
    Assert.assertTrue("seed file created", regular.createNewFile());
    Path linkDir = tempFolder.getRoot().toPath().resolve("link-dir");
    Files.createSymbolicLink(linkDir, realDir.toPath());
    String input = linkDir.resolve("plain.so").toString();

    String resolved = JniLibLoader.toRealPath(input);

    Assert.assertNotEquals(
        "toRealPath must canonicalize the parent directory, not return the input verbatim",
        input,
        resolved);
    Assert.assertEquals(
        "regular file path should canonicalize to its real path",
        regular.toPath().toRealPath().toString(),
        resolved);
  }

  @Test
  public void toRealPathResolvesRelativeSymlinkAgainstLinkParent() throws Exception {
    // Versioned-library symlink chain:
    //   <dir>/libfoo.so -> libfoo.so.1 -> libfoo.so.1.2.3 (regular file)
    File dir = tempFolder.newFolder("nested-dir");
    File realFile = new File(dir, "libfoo.so.1.2.3");
    Assert.assertTrue("seed file created", realFile.createNewFile());

    Path versioned = dir.toPath().resolve("libfoo.so.1");
    Files.createSymbolicLink(versioned, Paths.get("libfoo.so.1.2.3"));
    Path soname = dir.toPath().resolve("libfoo.so");
    Files.createSymbolicLink(soname, Paths.get("libfoo.so.1"));

    String resolved = JniLibLoader.toRealPath(soname.toString());

    // Compare to the JDK-canonical real path of the underlying file. Do NOT re-canonicalize the
    // returned string — a regression that returned the unresolved symlink path would otherwise be
    // hidden by the comparison side following the link again.
    Assert.assertEquals(
        "relative symlinks must resolve against the link's parent, not the process CWD",
        realFile.toPath().toRealPath().toString(),
        resolved);
  }

  @Test
  public void toRealPathRejectsSymlinkCycle() throws Exception {
    // a -> b, b -> a — pre-fix the loop ran forever; the JDK throws FileSystemLoopException now.
    File dir = tempFolder.newFolder("cycle-dir");
    Path a = dir.toPath().resolve("a");
    Path b = dir.toPath().resolve("b");
    Files.createSymbolicLink(a, Paths.get("b"));
    Files.createSymbolicLink(b, Paths.get("a"));

    try {
      JniLibLoader.toRealPath(a.toString());
      Assert.fail("symlink cycle must not silently resolve to a real path");
    } catch (GlutenException expected) {
      // The cycle-detection contract: the JDK reports the loop. Linux maps ELOOP to
      // FileSystemLoopException; macOS (current openjdk) surfaces a plain FileSystemException
      // with "Too many levels of symbolic links" in the message. Accept either, but reject any
      // other FileSystemException subclass (NoSuchFileException, AccessDeniedException, ...)
      // so this test doesn't quietly degrade into "any FS failure passes".
      Throwable cause = expected.getCause();
      String causeMsg = cause == null ? "" : String.valueOf(cause.getMessage());
      boolean reportsLoop =
          cause instanceof java.nio.file.FileSystemLoopException
              || (cause instanceof java.nio.file.FileSystemException
                  && causeMsg
                      .toLowerCase(Locale.ROOT)
                      .contains("too many levels of symbolic links"));
      Assert.assertTrue("cause should report a symlink loop, got: " + cause, reportsLoop);
    }
  }
}
