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
package org.apache.solr.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileExistsException;

/**
 *
 */
public class FileUtils {

  /**
   * Resolves a path relative a base directory.
   *
   * <p>
   * This method does what "new File(base,path)" <b>Should</b> do, if it wasn't
   * completely lame: If path is absolute, then a File for that path is returned;
   * if it's not absolute, then a File is returned using "path" as a child
   * of "base")
   * </p>
   */
  public static File resolvePath(File base, String path) {
    File r = new File(path);
    return r.isAbsolute() ? r : new File(base, path);
  }

  public static void copyFile(File src , File destination) throws IOException {
    try (FileChannel in = new FileInputStream(src).getChannel();
         FileChannel out = new FileOutputStream(destination).getChannel()) {
      in.transferTo(0, in.size(), out);
    }
  }

  /**
   * Copied from Lucene's FSDirectory.fsync(String)
   *
   * @param fullFile the File to be synced to disk
   * @throws IOException if the file could not be synced
   */
  public static void sync(File fullFile) throws IOException  {
    if (fullFile == null || !fullFile.exists())
      throw new FileNotFoundException("File does not exist " + fullFile);

    boolean success = false;
    int retryCount = 0;
    IOException exc = null;
    while(!success && retryCount < 5) {
      retryCount++;
      try (RandomAccessFile file = new RandomAccessFile(fullFile, "rw")) {
        file.getFD().sync();
        success = true;
      } catch (IOException ioe) {
        if (exc == null)
          exc = ioe;
        try {
          // Pause 5 msec
          Thread.sleep(5);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    if (!success)
      // Throw original exception
      throw exc;
  }

  public static boolean fileExists(String filePathString) {
    return new File(filePathString).exists();
  }

  // Files.createDirectories has odd behavior if the path is a symlink and it already exists
  // _even if it's a symlink to a directory_. 
  // 
  // oddly, if the path to be created just contains a symlink in intermediate levels, Files.createDirectories
  // works just fine.
  //
  // This works around that issue
  public static Path createDirectories(Path path) throws IOException {
    if (Files.exists(path) && Files.isSymbolicLink(path)) {
      Path real = path.toRealPath();
      if (Files.isDirectory(real)) return real;
      throw new FileExistsException("Tried to create a directory at to an existing non-directory symlink: " + path.toString());
    }
    return Files.createDirectories(path);
  }
}
