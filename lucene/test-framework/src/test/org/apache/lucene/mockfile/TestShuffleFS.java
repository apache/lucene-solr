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

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** Basic tests for ShuffleFS */
public class TestShuffleFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    return wrap(path, random().nextLong());
  }
  
  Path wrap(Path path, long seed) {
    FileSystem fs = new ShuffleFS(path.getFileSystem(), seed).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** test that we return directory listings correctly */
  public void testShuffleWorks() throws IOException {
    Path dir = wrap(createTempDir());
    
    Files.createFile(dir.resolve("file1"));
    Files.createFile(dir.resolve("file2"));
    Files.createFile(dir.resolve("file3"));
    
    List<Path> seen = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        seen.add(path);
      }
    }
    
    assertEquals(3, seen.size());
    assertTrue(seen.contains(dir.resolve("file1")));
    assertTrue(seen.contains(dir.resolve("file2")));
    assertTrue(seen.contains(dir.resolve("file3")));
  }
  
  /** test that we change order of directory listings */
  public void testActuallyShuffles() throws IOException {
    Path dir = createTempDir();
    for (int i = 0; i < 100; i++) {
      Files.createFile(dir.resolve("file" + i));
    }
    List<String> expected = new ArrayList<>();
    
    // get the raw listing from the actual filesystem
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        expected.add(path.getFileName().toString());
      }
    }
    
    // shuffle until the order changes.
    for (int i = 0; i < 10000; i++) {
      Path wrapped = wrap(dir, random().nextLong());
    
      List<String> seen = new ArrayList<>();
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(wrapped)) {
        for (Path path : stream) {
          seen.add(path.getFileName().toString());
        }
      }
      
      // we should always see the same files.
      assertEquals(new HashSet<>(expected), new HashSet<>(seen));
      if (!expected.equals(seen)) {
        return;
      }
    }
    fail("ordering never changed");
  }
  
  /** 
   * shuffle underlying contents randomly with different seeds,
   * and ensure shuffling that again with the same seed is consistent.
   */
  public void testConsistentOrder() throws IOException {
    Path raw = createTempDir();
    for (int i = 0; i < 100; i++) {
      Files.createFile(raw.resolve("file" + i));
    }
    
    long seed = random().nextLong();
    Path dirExpected = wrap(raw, seed);

    // get the shuffled listing for the seed.
    List<String> expected = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirExpected)) {
      for (Path path : stream) {
        expected.add(path.getFileName().toString());
      }
    }
    
    // shuffle wrapping a different scrambled ordering each time, it should always be the same.
    for (int i = 0; i < 100; i++) {
      Path scrambled = wrap(raw, random().nextLong());
      Path ordered = wrap(scrambled, seed);
    
      List<String> seen = new ArrayList<>();
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(ordered)) {
        for (Path path : stream) {
          seen.add(path.getFileName().toString());
        }
      }
      
      // we should always see the same files in the same order
      assertEquals(expected, seen);
    }
  }
  
  /** 
   * test that we give a consistent order 
   * for the same file names within different directories 
   */
  public void testFileNameOnly() throws IOException {
    Path dir = wrap(createTempDir());
    
    Files.createFile(dir.resolve("file1"));
    Files.createFile(dir.resolve("file2"));
    Files.createFile(dir.resolve("file3"));
    
    List<String> expected = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        expected.add(path.getFileName().toString());
      }
    }
    
    Path subdir = dir.resolve("subdir");
    Files.createDirectory(subdir);
    Files.createFile(subdir.resolve("file3"));
    Files.createFile(subdir.resolve("file2"));
    Files.createFile(subdir.resolve("file1"));
    
    List<String> actual = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(subdir)) {
      for (Path path : stream) {
        actual.add(path.getFileName().toString());
      }
    }
    
    assertEquals(expected, actual);
  }
}
