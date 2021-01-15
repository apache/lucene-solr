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

package org.apache.solr.blob;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public abstract class BlobStore implements Closeable {

  /**
   * Creates a file.
   *
   * @param dirPath The path to the directory in which to create the file.
   * @param fileName The file name.
   */
  public abstract void create(String dirPath, String fileName, InputStream inputStream, long contentLength)
          throws IOException;

  /**
   * Deletes files.
   *
   * @param dirPath The path to the directory in which to delete files.
   * @param fileNames The file names.
   */
  public abstract void delete(String dirPath, Collection<String> fileNames)
          throws IOException;

  /**
   * Deletes a directory.
   *
   * @param dirPath The path to the directory to delete. It is deleted even if it is not empty.
   */
  public abstract void deleteDirectory(String dirPath)
          throws IOException;

  /**
   * Deletes directories from the blob storage.
   *
   * @param dirPath The path to the directory in which to delete sub-directories.
   * @param dirNames The directory names. They are deleted even if they are not empty.
   */
  public abstract void deleteDirectories(String dirPath, Collection<String> dirNames)
          throws IOException;

  /**
   * Lists all files and sub-directories that are selected by the provided filter in a given directory.
   *
   * @param dirPath The path to the directory in which to list files/sub-directories.
   * @param nameFilter Filters the listed file/directory names (does not include the path).
   */
  public abstract List<String> listInDirectory(String dirPath, Predicate<String> nameFilter)
          throws IOException;

  /**
   * Reads a file.
   *
   * @param dirPath The path to the directory in which to read the file.
   * @param fileName The file name.
   */
  public abstract InputStream read(String dirPath, String fileName)
          throws IOException;
}
