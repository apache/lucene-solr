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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.IOUtils;

/** Basic tests for HandleLimitFS */
public class TestHandleLimitFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    return wrap(path, 4096);
  }
  
  Path wrap(Path path, int limit) {
    FileSystem fs = new HandleLimitFS(path.getFileSystem(), limit).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** set a limit at n files, then open more than that and ensure we hit exception */
  public void testTooManyOpenFiles() throws IOException {
    int n = 60;

    Path dir = wrap(createTempDir(), n);
    
    // create open files to exact limit
    List<Closeable> toClose = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      Path p = Files.createTempFile(dir, null, null);
      toClose.add(Files.newOutputStream(p));
    }
    
    // now exceed
    try {
      Files.newOutputStream(Files.createTempFile(dir, null, null));
      fail("didn't hit exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Too many open files"));
    }
    
    IOUtils.close(toClose);
  }
}
