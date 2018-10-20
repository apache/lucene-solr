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
package org.apache.lucene.mockfile;

import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Basic tests for ExtrasFS */
public class TestExtrasFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    return wrap(path, random().nextBoolean(), random().nextBoolean());
  }
  
  Path wrap(Path path, boolean active, boolean createDirectory) {
    FileSystem fs = new ExtrasFS(path.getFileSystem(), active, createDirectory).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** test where extra file is created */
  public void testExtraFile() throws Exception {
    Path dir = wrap(createTempDir(), true, false);
    Files.createDirectory(dir.resolve("foobar"));
    
    List<String> seen = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.resolve("foobar"))) {
      for (Path path : stream) {
        seen.add(path.getFileName().toString());
      }
    }
    assertEquals(Arrays.asList("extra0"), seen);
    assertTrue(Files.isRegularFile(dir.resolve("foobar").resolve("extra0")));
  }
  
  /** test where extra directory is created */
  public void testExtraDirectory() throws Exception {
    Path dir = wrap(createTempDir(), true, true);
    Files.createDirectory(dir.resolve("foobar"));
    
    List<String> seen = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.resolve("foobar"))) {
      for (Path path : stream) {
        seen.add(path.getFileName().toString());
      }
    }
    assertEquals(Arrays.asList("extra0"), seen);
    assertTrue(Files.isDirectory(dir.resolve("foobar").resolve("extra0")));
  }
  
  /** test where no extras are created: its a no-op */
  public void testNoExtras() throws Exception {
    Path dir = wrap(createTempDir(), false, false);
    Files.createDirectory(dir.resolve("foobar"));
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.resolve("foobar"))) {
      for (Path path : stream) {
        fail("should not have found file: " + path);
      }
    }
  }
}
