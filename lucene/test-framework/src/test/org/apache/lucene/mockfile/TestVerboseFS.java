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
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.InfoStream;

/** Basic tests for VerboseFS */
public class TestVerboseFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    return wrap(path, InfoStream.NO_OUTPUT);
  }
  
  Path wrap(Path path, InfoStream stream) {
    FileSystem fs = new VerboseFS(path.getFileSystem(), stream).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** InfoStream that looks for a substring and indicates if it saw it */
  static class InfoStreamListener extends InfoStream {
    /** True if we saw the message */
    final AtomicBoolean seenMessage = new AtomicBoolean(false);
    /** Expected message */ 
    final String messageStartsWith;
    
    InfoStreamListener(String messageStartsWith) {
      this.messageStartsWith = messageStartsWith;
    }
    
    @Override
    public void close() throws IOException {}

    @Override
    public void message(String component, String message) {
      if ("FS".equals(component) && message.startsWith(messageStartsWith)) {
        seenMessage.set(true);
      }
    }

    @Override
    public boolean isEnabled(String component) {
      return true;
    }
    
    boolean sawMessage() {
      return seenMessage.get();
    }
  }
  
  /** Test createDirectory */
  public void testCreateDirectory() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("createDirectory");
    Path dir = wrap(createTempDir(), stream);
    Files.createDirectory(dir.resolve("subdir"));
    assertTrue(stream.sawMessage());

    expectThrows(IOException.class, () -> Files.createDirectory(dir.resolve("subdir")));
  }
  
  /** Test delete */
  public void testDelete() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("delete");
    Path dir = wrap(createTempDir(), stream);
    Files.createFile(dir.resolve("foobar"));
    Files.delete(dir.resolve("foobar"));
    assertTrue(stream.sawMessage());

    expectThrows(IOException.class, () -> Files.delete(dir.resolve("foobar")));
  }
  
  /** Test deleteIfExists */
  public void testDeleteIfExists() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("deleteIfExists");
    Path dir = wrap(createTempDir(), stream);
    Files.createFile(dir.resolve("foobar"));
    Files.deleteIfExists(dir.resolve("foobar"));
    assertTrue(stream.sawMessage());

    // no exception
    Files.deleteIfExists(dir.resolve("foobar"));
  }
  
  /** Test copy */
  public void testCopy() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("copy");
    Path dir = wrap(createTempDir(), stream);
    Files.createFile(dir.resolve("foobar"));
    Files.copy(dir.resolve("foobar"), dir.resolve("baz"));
    assertTrue(stream.sawMessage());

    expectThrows(IOException.class, () -> Files.copy(dir.resolve("nonexistent"), dir.resolve("something")));
  }
  
  /** Test move */
  public void testMove() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("move");
    Path dir = wrap(createTempDir(), stream);
    Files.createFile(dir.resolve("foobar"));
    Files.move(dir.resolve("foobar"), dir.resolve("baz"));
    assertTrue(stream.sawMessage());

    expectThrows(IOException.class, () -> Files.move(dir.resolve("nonexistent"), dir.resolve("something")));
  }
  
  /** Test newOutputStream */
  public void testNewOutputStream() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("newOutputStream");
    Path dir = wrap(createTempDir(), stream);
    OutputStream file = Files.newOutputStream(dir.resolve("output"));
    assertTrue(stream.sawMessage());
    file.close();

    expectThrows(IOException.class, () -> Files.newOutputStream(dir.resolve("output"), StandardOpenOption.CREATE_NEW));
  }
  
  /** Test FileChannel.open */
  public void testFileChannel() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("newFileChannel");
    Path dir = wrap(createTempDir(), stream);
    FileChannel channel = FileChannel.open(dir.resolve("foobar"), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
    assertTrue(stream.sawMessage());
    channel.close();

    expectThrows(IOException.class, () -> FileChannel.open(dir.resolve("foobar"),
        StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE));
  }
  
  /** Test AsynchronousFileChannel.open */
  public void testAsyncFileChannel() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("newAsynchronousFileChannel");
    Path dir = wrap(createTempDir(), stream);
    AsynchronousFileChannel channel = AsynchronousFileChannel.open(dir.resolve("foobar"), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
    assertTrue(stream.sawMessage());
    channel.close();

    expectThrows(IOException.class, () -> AsynchronousFileChannel.open(dir.resolve("foobar"),
        StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE));
  }
  
  /** Test newByteChannel */
  public void testByteChannel() throws IOException {
    InfoStreamListener stream = new InfoStreamListener("newByteChannel");
    Path dir = wrap(createTempDir(), stream);
    SeekableByteChannel channel = Files.newByteChannel(dir.resolve("foobar"), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
    assertTrue(stream.sawMessage());
    channel.close();

    expectThrows(IOException.class, () -> Files.newByteChannel(dir.resolve("foobar"),
        StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE));
  }
  
  /** Test that verbose does not corrupt file not found exceptions */
  public void testVerboseFSNoSuchFileException() {
    Path dir = wrap(createTempDir());
    expectThrows(NoSuchFileException.class, () -> AsynchronousFileChannel.open(dir.resolve("doesNotExist.rip")));
    expectThrows(NoSuchFileException.class, () -> FileChannel.open(dir.resolve("doesNotExist.rip")));
    expectThrows(NoSuchFileException.class, () -> Files.newByteChannel(dir.resolve("stillopen")));
  }
}
