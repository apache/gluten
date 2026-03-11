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
package org.apache.gluten.utils;

import org.apache.gluten.exception.GlutenException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Code is copied from <a
 * href="https://stackoverflow.com/questions/3923129/get-a-list-of-resources-from-classpath-directory">here</a>
 * and then modified for Gluten's use.
 */
public class ResourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtil.class);

  /**
   * Get a collection of resource paths by the input RegEx pattern in a certain container folder.
   *
   * @param container The container folder. E.g., `META-INF`. Should not be left empty, because
   *     Classloader requires for at a meaningful file name to search inside the loaded jar files.
   * @param pattern The pattern to match on the file names.
   * @return The relative resource paths in the order they are found.
   */
  public static List<String> getResources(final String container, final Pattern pattern) {
    Preconditions.checkArgument(
        !container.isEmpty(),
        "Resource search should only be used under a certain container folder");
    Preconditions.checkArgument(
        !container.startsWith("/") && !container.endsWith("/"),
        "Resource container should not start or end with\"/\"");
    final List<String> buffer = new ArrayList<>();
    final Enumeration<URL> containerUrls;
    try {
      containerUrls = ResourceUtil.class.getClassLoader().getResources(container);
    } catch (IOException e) {
      throw new GlutenException(e);
    }
    while (containerUrls.hasMoreElements()) {
      final URL containerUrl = containerUrls.nextElement();
      getResources(containerUrl, pattern, buffer);
    }
    return Collections.unmodifiableList(buffer);
  }

  private static void getResources(
      final URL containerUrl, final Pattern pattern, final List<String> buffer) {
    final String protocol = containerUrl.getProtocol();
    switch (protocol) {
      case "file":
        final File fileContainer = new File(containerUrl.getPath());
        Preconditions.checkState(
            fileContainer.exists() && fileContainer.isDirectory(),
            "Specified file container " + containerUrl + " is not a directory or not a file");
        getResourcesFromDirectory(fileContainer, fileContainer, pattern, buffer);
        break;
      case "jar":
        final String jarContainerPath = containerUrl.getPath();
        final Pattern jarContainerPattern = Pattern.compile("file:([^!]+)!/(.+)");
        final Matcher m = jarContainerPattern.matcher(jarContainerPath);
        if (!m.matches()) {
          throw new GlutenException("Illegal Jar container URL: " + containerUrl);
        }
        final String jarPath = m.group(1);
        final File jarFile = new File(jarPath);
        Preconditions.checkState(
            jarFile.exists() && jarFile.isFile(),
            "Specified Jar container " + containerUrl + " is not a Jar file");
        final String dir = m.group(2);
        getResourcesFromJarFile(jarFile, dir, pattern, buffer);
        break;
      default:
        throw new GlutenException("Unrecognizable resource protocol: " + protocol);
    }
  }

  private static void getResourcesFromJarFile(
      final File jarFile, final String dir, final Pattern pattern, final List<String> buffer) {
    try (ZipFile zf = new ZipFile(jarFile)) {
      final Enumeration<? extends ZipEntry> e = zf.entries();
      while (e.hasMoreElements()) {
        final ZipEntry ze = e.nextElement();
        final String fileName = ze.getName();
        if (!fileName.startsWith(dir)) {
          continue;
        }
        final String relativeFileName =
            new File(dir).toURI().relativize(new File(fileName).toURI()).getPath();
        final boolean accept = pattern.matcher(relativeFileName).matches();
        if (accept) {
          buffer.add(relativeFileName);
        }
      }
    } catch (final IOException e) {
      throw new GlutenException(e);
    }
  }

  private static void getResourcesFromDirectory(
      final File root, final File directory, final Pattern pattern, final List<String> buffer) {
    final File[] fileList = Objects.requireNonNull(directory.listFiles());
    for (final File file : fileList) {
      if (file.isDirectory()) {
        getResourcesFromDirectory(root, file, pattern, buffer);
      } else {
        final String relative = root.toURI().relativize(file.toURI()).getPath();
        final boolean accept = pattern.matcher(relative).matches();
        if (accept) {
          buffer.add(relative);
        }
      }
    }
  }
}
