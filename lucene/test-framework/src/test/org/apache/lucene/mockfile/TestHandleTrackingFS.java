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
import java.lang.Object;
import java.lang.Override;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.mockfile.HandleTrackingFS;
import org.apache.lucene.mockfile.LeakFS;

/** Basic tests for HandleTrackingFS */
public class TestHandleTrackingFS extends MockFileSystemTestCase {
  
  @Override
  protected Path wrap(Path path) {
    FileSystem fs = new LeakFS(path.getFileSystem()).getFileSystem(URI.create("file:///"));
    return new FilterPath(path, fs);
  }
  
  /** Test that the delegate gets closed on exception in HandleTrackingFS#onClose */
  public void testOnCloseThrowsException() throws IOException {
    Path path = wrap(createTempDir()); // we are using LeakFS under the hood if we don't get closed the test fails
    FileSystem fs = new HandleTrackingFS("test://", path.getFileSystem()) {
      @Override
      protected void onClose(Path path, Object stream) throws IOException {
        throw new IOException("boom");
      }

      @Override
      protected void onOpen(Path path, Object stream) throws IOException {
        //
      }
    }.getFileSystem(URI.create("file:///"));
    Path dir = new FilterPath(path, fs);

    OutputStream file = Files.newOutputStream(dir.resolve("somefile"));
    file.write(5);
    try {
      file.close();
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }

    SeekableByteChannel channel = Files.newByteChannel(dir.resolve("somefile"));
    try {
      channel.close();
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }

    InputStream stream = Files.newInputStream(dir.resolve("somefile"));
    try {
      stream.close();
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }
    fs.close();

    DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir);
    try {
      dirStream.close();
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }
  }


  /** Test that the delegate gets closed on exception in HandleTrackingFS#onOpen */
  public void testOnOpenThrowsException() throws IOException {
    Path path = wrap(createTempDir()); // we are using LeakFS under the hood if we don't get closed the test fails
    FileSystem fs = new HandleTrackingFS("test://", path.getFileSystem()) {
      @Override
      protected void onClose(Path path, Object stream) throws IOException {
      }

      @Override
      protected void onOpen(Path path, Object stream) throws IOException {
        throw new IOException("boom");
      }
    }.getFileSystem(URI.create("file:///"));
    Path dir = new FilterPath(path, fs);

    try {
      OutputStream file = Files.newOutputStream(dir.resolve("somefile"));
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }

    try {
      SeekableByteChannel channel = Files.newByteChannel(dir.resolve("somefile"));
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }

    try {
      InputStream stream = Files.newInputStream(dir.resolve("somefile"));
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }
    fs.close();

    try {
      DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir);
      fail("expected IOException");
    } catch (IOException ex) {
      // expected
    }
    fs.close();
  }
}
