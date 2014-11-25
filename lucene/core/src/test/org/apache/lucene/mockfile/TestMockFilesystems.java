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
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;

public class TestMockFilesystems extends LuceneTestCase {
  
  public void testLeakInputStream() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new LeakFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream leak = Files.newInputStream(wrapped.resolve("stillopen"));
    try {
      fs.close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  public void testLeakOutputStream() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new LeakFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream leak = Files.newOutputStream(wrapped.resolve("leaky"));
    try {
      fs.close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  public void testLeakFileChannel() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new LeakFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("stillopen"));
    file.write(5);
    file.close();
    FileChannel leak = FileChannel.open(wrapped.resolve("stillopen"));
    try {
      fs.close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  public void testLeakAsyncFileChannel() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new LeakFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("stillopen"));
    file.write(5);
    file.close();
    AsynchronousFileChannel leak = AsynchronousFileChannel.open(wrapped.resolve("stillopen"));
    try {
      fs.close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
  
  public void testLeakByteChannel() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new LeakFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("stillopen"));
    file.write(5);
    file.close();
    SeekableByteChannel leak = Files.newByteChannel(wrapped.resolve("stillopen"));
    try {
      fs.close();
      fail("should have gotten exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("file handle leaks"));
    }
    leak.close();
  }
 
  public void testDeleteOpenFile() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new WindowsFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(wrapped.resolve("stillopen"));
    try {
      Files.delete(wrapped.resolve("stillopen"));
      fail("should have gotten exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("access denied"));
    }
    is.close();
  }
  
  public void testDeleteIfExistsOpenFile() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new WindowsFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(wrapped.resolve("stillopen"));
    try {
      Files.deleteIfExists(wrapped.resolve("stillopen"));
      fail("should have gotten exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("access denied"));
    }
    is.close();
  }
  
  public void testRenameOpenFile() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new WindowsFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("stillopen"));
    file.write(5);
    file.close();
    InputStream is = Files.newInputStream(wrapped.resolve("stillopen"));
    try {
      Files.move(wrapped.resolve("stillopen"), wrapped.resolve("target"), StandardCopyOption.ATOMIC_MOVE);
      fail("should have gotten exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("access denied"));
    }
    is.close();
  }
  
  public void testVerboseWrite() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    final AtomicBoolean seenMessage = new AtomicBoolean(false);
    InfoStream testStream = new InfoStream() {
      @Override
      public void close() throws IOException {}

      @Override
      public void message(String component, String message) {
        if ("FS".equals(component) && message.startsWith("newOutputStream")) {
          seenMessage.set(true);
        }
      }

      @Override
      public boolean isEnabled(String component) {
        return true;
      }
    };
    FileSystem fs = new VerboseFS(dir.getFileSystem(), testStream).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);
    
    OutputStream file = Files.newOutputStream(wrapped.resolve("output"));
    assertTrue(seenMessage.get());
    file.close();
  }
}
