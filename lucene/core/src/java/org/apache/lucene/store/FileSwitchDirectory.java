/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;


import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.util.IOUtils;

/**
 * Expert: A Directory instance that switches files between
 * two other Directory instances.

 * <p>Files with the specified extensions are placed in the
 * primary directory; others are placed in the secondary
 * directory.  The provided Set must not change once passed
 * to this class, and must allow multiple threads to call
 * contains at once.</p>
 *
 * <p>Locks with a name having the specified extensions are
 * delegated to the primary directory; others are delegated
 * to the secondary directory. Ideally, both Directory
 * instances should use the same lock factory.</p>
 *
 * @lucene.experimental
 */

public class FileSwitchDirectory extends Directory {
  private final Directory secondaryDir;
  private final Directory primaryDir;
  private final Set<String> primaryExtensions;
  private boolean doClose;
  private static final Pattern EXT_PATTERN = Pattern.compile("\\.([a-zA-Z]+)");

  public FileSwitchDirectory(Set<String> primaryExtensions, Directory primaryDir, Directory secondaryDir, boolean doClose) {
    if (primaryExtensions.contains("tmp")) {
      throw new IllegalArgumentException("tmp is a reserved extension");
    }
    this.primaryExtensions = primaryExtensions;
    this.primaryDir = primaryDir;
    this.secondaryDir = secondaryDir;
    this.doClose = doClose;
  }

  /** Return the primary directory */
  public Directory getPrimaryDir() {
    return primaryDir;
  }
  
  /** Return the secondary directory */
  public Directory getSecondaryDir() {
    return secondaryDir;
  }
  
  @Override
  public Lock obtainLock(String name) throws IOException {
    return getDirectory(name).obtainLock(name);
  }

  @Override
  public void close() throws IOException {
    if (doClose) {
      IOUtils.close(primaryDir, secondaryDir);
      doClose = false;
    }
  }
  
  @Override
  public String[] listAll() throws IOException {
    List<String> files = new ArrayList<>();
    // LUCENE-3380: either or both of our dirs could be FSDirs,
    // but if one underlying delegate is an FSDir and mkdirs() has not
    // yet been called, because so far everything is written to the other,
    // in this case, we don't want to throw a NoSuchFileException
    NoSuchFileException exc = null;
    try {
      for(String f : primaryDir.listAll()) {
        String ext = getExtension(f);
        // we should respect the extension here as well to ensure that we don't list a file that is already
        // deleted or rather in the one of the directories pending deletions if both directories point
        // to the same filesystem path. This is quite common for instance to use NIOFS as a primary
        // and MMap as a secondary to only mmap files like docvalues or term dictionaries.
        if (primaryExtensions.contains(ext)) {
          files.add(f);
        }
      }
    } catch (NoSuchFileException e) {
      exc = e;
    }
    try {
      for(String f : secondaryDir.listAll()) {
        String ext = getExtension(f);
        if (primaryExtensions.contains(ext) == false) {
          files.add(f);
        }
      }
    } catch (NoSuchFileException e) {
      // we got NoSuchFileException from both dirs
      // rethrow the first.
      if (exc != null) {
        throw exc;
      }
      // we got NoSuchFileException from the secondary,
      // and the primary is empty.
      if (files.isEmpty()) {
        throw e;
      }
    }
    // we got NoSuchFileException from the primary,
    // and the secondary is empty.
    if (exc != null && files.isEmpty()) {
      throw exc;
    }
    String[] result = files.toArray(new String[files.size()]);
    Arrays.sort(result);
    return result;
  }

  /** Utility method to return a file's extension. */
  public static String getExtension(String name) {
    int i = name.lastIndexOf('.');
    if (i == -1) {
      return "";
    }
    String ext = name.substring(i + 1);
    if (ext.equals("tmp")) {
      Matcher matcher = EXT_PATTERN.matcher(name.substring(0, i + 1));
      if (matcher.find()) {
        return matcher.group(1);
      }
    }
    return ext;
  }

  private Directory getDirectory(String name) {
    String ext = getExtension(name);
    if (primaryExtensions.contains(ext)) {
      return primaryDir;
    } else {
      return secondaryDir;
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    if (getDirectory(name) == primaryDir) {
      primaryDir.deleteFile(name);
    } else {
      secondaryDir.deleteFile(name);
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    return getDirectory(name).fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return getDirectory(name).createOutput(name, context);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
    // this is best effort - it's ok to create a tmp file with any prefix and suffix. Yet if this file is then
    // in-turn used to rename they must match to the same directory hence we use the full file-name to find
    // the right directory. Here we can't make a decision but we need to ensure that all other operations
    // map to the right directory.
    String tmpFileName = getTempFileName(prefix, suffix, 0);
    return getDirectory(tmpFileName).createTempOutput(prefix, suffix, context);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    List<String> primaryNames = new ArrayList<>();
    List<String> secondaryNames = new ArrayList<>();

    for (String name : names)
      if (primaryExtensions.contains(getExtension(name))) {
        primaryNames.add(name);
      } else {
        secondaryNames.add(name);
      }

    primaryDir.sync(primaryNames);
    secondaryDir.sync(secondaryNames);
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    Directory sourceDir = getDirectory(source);
    // won't happen with standard lucene index files since pending and commit will
    // always have the same extension ("")
    if (sourceDir != getDirectory(dest)) {
      throw new AtomicMoveNotSupportedException(source, dest, "source and dest are in different directories");
    }
    sourceDir.rename(source, dest);
  }

  @Override
  public void syncMetaData() throws IOException {
    primaryDir.syncMetaData();
    secondaryDir.syncMetaData();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return getDirectory(name).openInput(name, context);
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    Set<String> primaryDeletions = primaryDir.getPendingDeletions();
    Set<String> secondaryDeletions = secondaryDir.getPendingDeletions();
    if (primaryDeletions.isEmpty() && secondaryDeletions.isEmpty()) {
      return Collections.emptySet();
    } else {
      HashSet<String> combined = new HashSet<>();
      combined.addAll(primaryDeletions);
      combined.addAll(secondaryDeletions);
      return Collections.unmodifiableSet(combined);
    }
  }
}
