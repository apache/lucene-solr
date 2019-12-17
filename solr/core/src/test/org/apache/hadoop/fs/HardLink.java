/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

import static java.nio.file.Files.createLink;

/**
 * Class for creating hardlinks.
 * Supports Unix/Linux, Windows via winutils , and Mac OS X.
 *
 * The HardLink class was formerly a static inner class of FSUtil,
 * and the methods provided were blatantly non-thread-safe.
 * To enable volume-parallel Update snapshots, we now provide static
 * threadsafe methods that allocate new buffer string arrays
 * upon each call.  We also provide an API to hardlink all files in a
 * directory with a single command, which is up to 128 times more
 * efficient - and minimizes the impact of the extra buffer creations.
 */
public class HardLink {
  public static final Object SOLR_HACK_FOR_CLASS_VERIFICATION = new Object();

  public final LinkStats linkStats; //not static

  public HardLink() {
    linkStats = new LinkStats();
  }

  /**
   * This abstract class bridges the OS-dependent implementations of the
   * needed functionality for querying link counts.
   * The particular implementation class is chosen during
   * static initialization phase of the HardLink class.
   * The "getter" methods construct shell command strings.
   */
  private static abstract class HardLinkCommandGetter {
    /**
     * Get the command string to query the hardlink count of a file
     */
    abstract String[] linkCount(File file) throws IOException;
  }

  /*
   * ****************************************************
   * Complexity is above.  User-visible functionality is below
   * ****************************************************
   */

  /**
   * Creates a hardlink
   * @param file - existing source file
   * @param linkName - desired target link file
   */
  public static void createHardLink(File file, File linkName)
      throws IOException {
    if (file == null) {
      throw new IOException(
          "invalid arguments to createHardLink: source file is null");
    }
    if (linkName == null) {
      throw new IOException(
          "invalid arguments to createHardLink: link name is null");
    }
    createLink(linkName.toPath(), file.toPath());
  }

  /**
   * Creates hardlinks from multiple existing files within one parent
   * directory, into one target directory.
   * @param parentDir - directory containing source files
   * @param fileBaseNames - list of path-less file names, as returned by
   *                        parentDir.list()
   * @param linkDir - where the hardlinks should be put. It must already exist.
   */
  public static void createHardLinkMult(File parentDir, String[] fileBaseNames,
                                        File linkDir) throws IOException {
    if (parentDir == null) {
      throw new IOException(
          "invalid arguments to createHardLinkMult: parent directory is null");
    }
    if (linkDir == null) {
      throw new IOException(
          "invalid arguments to createHardLinkMult: link directory is null");
    }
    if (fileBaseNames == null) {
      throw new IOException(
          "invalid arguments to createHardLinkMult: "
              + "filename list can be empty but not null");
    }
    if (!linkDir.exists()) {
      throw new FileNotFoundException(linkDir + " not found.");
    }
    for (String name : fileBaseNames) {
      createLink(linkDir.toPath().resolve(name),
          parentDir.toPath().resolve(name));
    }
  }

  /**
   * Retrieves the number of links to the specified file.
   */
  public static int getLinkCount(File fileName) throws IOException {
    if (fileName == null) {
      throw new IOException(
          "invalid argument to getLinkCount: file name is null");
    }
    if (!fileName.exists()) {
      throw new FileNotFoundException(fileName + " not found.");
    }

    return (Integer)Files.getAttribute(fileName.toPath(), "unix:nlink");
  }

  /* Create an IOException for failing to get link count. */
  private static IOException createIOException(File f, String message,
                                               String error, int exitvalue, Exception cause) {

    final String s = "Failed to get link count on file " + f
        + ": message=" + message
        + "; error=" + error
        + "; exit value=" + exitvalue;
    return (cause == null) ? new IOException(s) : new IOException(s, cause);
  }


  /**
   * HardLink statistics counters and methods.
   * Not multi-thread safe, obviously.
   * Init is called during HardLink instantiation, above.
   *
   * These are intended for use by knowledgeable clients, not internally,
   * because many of the internal methods are static and can't update these
   * per-instance counters.
   */
  public static class LinkStats {
    public int countDirs = 0;
    public int countSingleLinks = 0;
    public int countMultLinks = 0;
    public int countFilesMultLinks = 0;
    public int countEmptyDirs = 0;
    public int countPhysicalFileCopies = 0;

    public void clear() {
      countDirs = 0;
      countSingleLinks = 0;
      countMultLinks = 0;
      countFilesMultLinks = 0;
      countEmptyDirs = 0;
      countPhysicalFileCopies = 0;
    }

    public String report() {
      return "HardLinkStats: " + countDirs + " Directories, including "
          + countEmptyDirs + " Empty Directories, "
          + countSingleLinks
          + " single Link operations, " + countMultLinks
          + " multi-Link operations, linking " + countFilesMultLinks
          + " files, total " + (countSingleLinks + countFilesMultLinks)
          + " linkable files.  Also physically copied "
          + countPhysicalFileCopies + " other files.";
    }
  }
}
