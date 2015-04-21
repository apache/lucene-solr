package org.apache.lucene.mockfile;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.lucene.util.Constants;

/** Basic tests for WindowsFS */
public class TestWindowsFS extends MockFileSystemTestCase {
  
  // currently we don't emulate windows well enough to work on windows!
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeFalse("windows is not supported", Constants.WINDOWS);
  }

  @Override
  protected Path wrap(Path path) {
    FileSystem fs = new WindowsFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** Test Files.delete fails if a file has an open inputstream against it */
  public void testDeleteOpenFile() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(dir.resolve("stillopen"));
    try {
      Files.delete(dir.resolve("stillopen"));
      fail("should have gotten exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("access denied"));
    }
    is.close();
  }
  
  /** Test Files.deleteIfExists fails if a file has an open inputstream against it */
  public void testDeleteIfExistsOpenFile() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(dir.resolve("stillopen"));
    try {
      Files.deleteIfExists(dir.resolve("stillopen"));
      fail("should have gotten exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("access denied"));
    }
    is.close();
  }
  
  /** Test Files.rename fails if a file has an open inputstream against it */
  // TODO: what does windows do here?
  public void testRenameOpenFile() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(dir.resolve("stillopen"));
    try {
      Files.move(dir.resolve("stillopen"), dir.resolve("target"), StandardCopyOption.ATOMIC_MOVE);
      fail("should have gotten exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("access denied"));
    }
    is.close();
  }
}
