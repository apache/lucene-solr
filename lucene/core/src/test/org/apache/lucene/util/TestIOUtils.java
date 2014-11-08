package org.apache.lucene.util;

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

import java.nio.file.Files;
import java.nio.file.Path;

/** Simple test methods for IOUtils */
public class TestIOUtils extends LuceneTestCase {
  
  public void testDeleteFileIgnoringExceptions() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Files.createFile(file1);
    IOUtils.deleteFilesIgnoringExceptions(file1);
    assertFalse(Files.exists(file1));
    // actually deletes
  }
  
  public void testDontDeleteFileIgnoringExceptions() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    IOUtils.deleteFilesIgnoringExceptions(file1);
    // no exception
  }
  
  public void testDeleteTwoFilesIgnoringExceptions() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Path file2 = dir.resolve("file2");
    // only create file2
    Files.createFile(file2);
    IOUtils.deleteFilesIgnoringExceptions(file1, file2);
    assertFalse(Files.exists(file2));
    // no exception
    // actually deletes file2
  }
  
  public void testDeleteFileIfExists() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Files.createFile(file1);
    IOUtils.deleteFilesIfExist(file1);
    assertFalse(Files.exists(file1));
    // actually deletes
  }
  
  public void testDontDeleteDoesntExist() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    IOUtils.deleteFilesIfExist(file1);
    // no exception
  }
  
  public void testDeleteTwoFilesIfExist() throws Exception {
    Path dir = createTempDir();
    Path file1 = dir.resolve("file1");
    Path file2 = dir.resolve("file2");
    // only create file2
    Files.createFile(file2);
    IOUtils.deleteFilesIfExist(file1, file2);
    assertFalse(Files.exists(file2));
    // no exception
    // actually deletes file2
  }
}
