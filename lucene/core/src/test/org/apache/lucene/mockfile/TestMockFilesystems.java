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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
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
    assumeFalse("windows is not supported", Constants.WINDOWS);
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
    assumeFalse("windows is not supported", Constants.WINDOWS);
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
    assumeFalse("windows is not supported", Constants.WINDOWS);
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
  
  public void testVerboseFSNoSuchFileException() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new VerboseFS(dir.getFileSystem(), InfoStream.NO_OUTPUT).getFileSystem(URI.create("file:///"));    
    Path wrapped = new FilterPath(dir, fs);
    try {
      AsynchronousFileChannel.open(wrapped.resolve("doesNotExist.rip"));
      fail("did not hit exception");
    } catch (NoSuchFileException nsfe) {
      // expected
    }
    try {
      FileChannel.open(wrapped.resolve("doesNotExist.rip"));
      fail("did not hit exception");
    } catch (NoSuchFileException nsfe) {
      // expected
    }
    try {
      Files.newByteChannel(wrapped.resolve("stillopen"));
      fail("did not hit exception");
    } catch (NoSuchFileException nsfe) {
      // expected
    }
  }

  public void testTooManyOpenFiles() throws IOException {
    int n = 60;

    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new HandleLimitFS(dir.getFileSystem(), n).getFileSystem(URI.create("file:///"));
    dir = new FilterPath(dir, fs);
    
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

  public void testDirectoryStreamFiltered() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new FilterFileSystemProvider("test://", dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);

    OutputStream file = Files.newOutputStream(wrapped.resolve("file1"));
    file.write(5);
    file.close();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(wrapped)) {
      int count = 0;
      for (Path path : stream) {
        assertTrue(path instanceof FilterPath);
        if (!path.getFileName().toString().startsWith("extra")) {
          count++;
        }
      }
      assertEquals(1, count);
    }

    // check with LeakFS, a subclass of HandleTrackingFS which mucks with newDirectoryStream
    dir = FilterPath.unwrap(createTempDir());
    fs = new LeakFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    wrapped = new FilterPath(dir, fs);

    file = Files.newOutputStream(wrapped.resolve("file1"));
    file.write(5);
    file.close();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(wrapped)) {
      int count = 0;
      for (Path path : stream) {
        assertTrue(path instanceof FilterPath);
        if (!path.getFileName().toString().startsWith("extra")) {
          count++;
        }
      }
      assertEquals(1, count);
    }
  }

  public void testDirectoryStreamGlobFiltered() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new FilterFileSystemProvider("test://", dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);

    OutputStream file = Files.newOutputStream(wrapped.resolve("foo"));
    file.write(5);
    file.close();
    file = Files.newOutputStream(wrapped.resolve("bar"));
    file.write(5);
    file.close();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(wrapped, "f*")) {
      int count = 0;
      for (Path path : stream) {
        assertTrue(path instanceof FilterPath);
        ++count;
      }
      assertEquals(1, count);
    }

    // check with LeakFS, a subclass of HandleTrackingFS which mucks with newDirectoryStream
    dir = FilterPath.unwrap(createTempDir());
    fs = new LeakFS(dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    wrapped = new FilterPath(dir, fs);

    file = Files.newOutputStream(wrapped.resolve("foo"));
    file.write(5);
    file.close();
    file = Files.newOutputStream(wrapped.resolve("bar"));
    file.write(5);
    file.close();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(wrapped, "f*")) {
      int count = 0;
      for (Path path : stream) {
        assertTrue(path instanceof FilterPath);
        ++count;
      }
      assertEquals(1, count);
    }
  }
  
  public void testHashCodeEquals() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new FilterFileSystemProvider("test://", dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);

    Path f1 = wrapped.resolve("file1");
    Path f1Again = wrapped.resolve("file1");
    Path f2 = wrapped.resolve("file2");
    
    assertEquals(f1, f1);
    assertFalse(f1.equals(null));
    assertEquals(f1, f1Again);
    assertEquals(f1.hashCode(), f1Again.hashCode());
    assertFalse(f1.equals(f2));
  }
  
  public void testURI() throws IOException {
    Path dir = FilterPath.unwrap(createTempDir());
    FileSystem fs = new FilterFileSystemProvider("test://", dir.getFileSystem()).getFileSystem(URI.create("file:///"));
    Path wrapped = new FilterPath(dir, fs);

    Path f1 = wrapped.resolve("file1");
    URI uri = f1.toUri();
    Path f2 = fs.provider().getPath(uri);
    assertEquals(f1, f2);
    
    assumeTrue(Charset.defaultCharset().name() + " can't encode chinese", 
               Charset.defaultCharset().newEncoder().canEncode("中国"));
    Path f3 = wrapped.resolve("中国");
    URI uri2 = f3.toUri();
    Path f4 = fs.provider().getPath(uri2);
    assertEquals(f3, f4);
  }
}
