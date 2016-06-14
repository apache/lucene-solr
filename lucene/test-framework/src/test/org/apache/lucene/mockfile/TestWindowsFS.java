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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Exception;
import java.lang.RuntimeException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.mockfile.WindowsFS;
import org.apache.lucene.util.Constants;

/** Basic tests for WindowsFS */
public class TestWindowsFS extends MockFileSystemTestCase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // irony: currently we don't emulate windows well enough to work on windows!
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

  public void testOpenDeleteConcurrently() throws IOException, Exception {
    final Path dir = wrap(createTempDir());
    final Path file = dir.resolve("thefile");
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final AtomicBoolean stopped = new AtomicBoolean(false);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          barrier.await();
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
        while (stopped.get() == false) {
          try {
            if (random().nextBoolean()) {
              Files.delete(file);
            } else if (random().nextBoolean()) {
              Files.deleteIfExists(file);
            } else {
              Path target = file.resolveSibling("other");
              Files.move(file, target);
              Files.delete(target);
            }
          } catch (IOException ex) {
            // continue
          }
        }
      }
    };
    t.start();
    barrier.await();
    try {
      final int iters = 10 + random().nextInt(100);
      for (int i = 0; i < iters; i++) {
        boolean opened = false;
        try (OutputStream stream = Files.newOutputStream(file)) {
          opened = true;
          stream.write(0);
          // just create
        } catch (FileNotFoundException | NoSuchFileException ex) {
          assertEquals("File handle leaked - file is closed but still registered", 0, ((WindowsFS) dir.getFileSystem().provider()).openFiles.size());
          assertFalse("caught FNF on close", opened);
        }
        assertEquals("File handle leaked - file is closed but still registered", 0, ((WindowsFS) dir.getFileSystem().provider()).openFiles.size());
        Files.deleteIfExists(file);
      }
    } finally {
      stopped.set(true);
      t.join();
    }
  }
}
