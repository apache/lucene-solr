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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

/** Basic tests for LeakFS */
public class TestLeakFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    FileSystem fs = new LeakFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** Test leaks via Files.newInputStream */
  public void testLeakInputStream() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream leak = Files.newInputStream(dir.resolve("stillopen"));
    try {
      dir.getFileSystem().close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  /** Test leaks via Files.newOutputStream */
  public void testLeakOutputStream() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream leak = Files.newOutputStream(dir.resolve("leaky"));
    try {
      dir.getFileSystem().close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  /** Test leaks via FileChannel.open */
  public void testLeakFileChannel() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    FileChannel leak = FileChannel.open(dir.resolve("stillopen"));
    try {
      dir.getFileSystem().close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  /** Test leaks via AsynchronousFileChannel.open */
  public void testLeakAsyncFileChannel() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    AsynchronousFileChannel leak = AsynchronousFileChannel.open(dir.resolve("stillopen"));
    try {
      dir.getFileSystem().close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  /** Test leaks via Files.newByteChannel */
  public void testLeakByteChannel() throws IOException {
    Path dir = wrap(createTempDir());
    
    OutputStream file = Files.newOutputStream(dir.resolve("stillopen"));
    file.write(5);
    file.close();
    SeekableByteChannel leak = Files.newByteChannel(dir.resolve("stillopen"));
    try {
      dir.getFileSystem().close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
}
