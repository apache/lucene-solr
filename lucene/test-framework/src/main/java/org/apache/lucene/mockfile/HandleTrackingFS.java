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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.util.IOUtils;

/** 
 * Base class for tracking file handles.
 * <p>
 * This class adds tracking to all streams/channels and 
 * provides two hooks to handle file management:
 * <ul>
 *   <li>{@link #onOpen(Path, Object)}
 *   <li>{@link #onClose(Path, Object)}
 * </ul>
 */
public abstract class HandleTrackingFS extends FilterFileSystemProvider {
  
  /**
   * Create a new instance, identified by {@code scheme} and passing
   * through operations to {@code delegate}. 
   * @param scheme URI scheme for this provider
   * @param delegate delegate filesystem to wrap.
   */
  public HandleTrackingFS(String scheme, FileSystem delegate) {
    super(scheme, delegate);
  }
  
  /**
   * Called when {@code path} is opened via {@code stream}. 
   * @param path Path that was opened
   * @param stream Stream or Channel opened against the path.
   * @throws IOException if an I/O error occurs.
   */
  protected abstract void onOpen(Path path, Object stream) throws IOException;
  
  /**
   * Called when {@code path} is closed via {@code stream}. 
   * @param path Path that was closed
   * @param stream Stream or Channel closed against the path.
   * @throws IOException if an I/O error occurs.
   */
  protected abstract void onClose(Path path, Object stream) throws IOException;

  /**
   * Helper method, to deal with onOpen() throwing exception
   */
  final void callOpenHook(Path path, Closeable stream) throws IOException {
    boolean success = false;
    try {
      onOpen(path, stream);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(stream);
      }
    }
  }
  
  @Override
  public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
    InputStream stream = new FilterInputStream2(super.newInputStream(path, options)) {
      
      boolean closed;
      
      @Override
      public void close() throws IOException {
        try {
          if (!closed) {
            closed = true;
            onClose(path, this);
          }
        } finally {
          super.close();
        }
      }

      @Override
      public String toString() {
        return "InputStream(" + path.toString() + ")";
      }

      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }
    };
    callOpenHook(path, stream);
    return stream;
  }

  @Override
  public OutputStream newOutputStream(final Path path, OpenOption... options) throws IOException {
    OutputStream stream = new FilterOutputStream2(delegate.newOutputStream(toDelegate(path), options)) {
      
      boolean closed;

      @Override
      public void close() throws IOException {
        try {
          if (!closed) {
            closed = true;
            onClose(path, this);
          }
        } finally {
          super.close();
        }
      }
      
      @Override
      public String toString() {
        return "OutputStream(" + path.toString() + ")";
      }
      
      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }
    };
    callOpenHook(path, stream);
    return stream;
  }
  
  @Override
  public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    FileChannel channel = new FilterFileChannel(delegate.newFileChannel(toDelegate(path), options, attrs)) {
      
      boolean closed;
      
      @Override
      protected void implCloseChannel() throws IOException {
        if (!closed) {
          closed = true;
          try {
            onClose(path, this);
          } finally {
            super.implCloseChannel();
          }
        }
      }

      @Override
      public String toString() {
        return "FileChannel(" + path.toString() + ")";
      }
      
      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }
    };
    callOpenHook(path, channel);
    return channel;
  }

  @Override
  public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
    AsynchronousFileChannel channel = new FilterAsynchronousFileChannel(super.newAsynchronousFileChannel(path, options, executor, attrs)) {
      
      boolean closed;
      
      @Override
      public void close() throws IOException {
        try {
          if (!closed) {
            closed = true;
            onClose(path, this);
          }
        } finally {
          super.close();
        }
      }

      @Override
      public String toString() {
        return "AsynchronousFileChannel(" + path.toString() + ")";
      }
      
      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }
    };
    callOpenHook(path, channel);
    return channel;
  }

  @Override
  public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    SeekableByteChannel channel = new FilterSeekableByteChannel(super.newByteChannel(path, options, attrs)) {
      
      boolean closed;
      
      @Override
      public void close() throws IOException {
        try {
          if (!closed) {
            closed = true;
            onClose(path, this);
          }
        } finally {
          super.close();
        }
      }

      @Override
      public String toString() {
        return "SeekableByteChannel(" + path.toString() + ")";
      }
      
      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }
    };
    callOpenHook(path, channel);
    return channel;
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
    Filter<Path> wrappedFilter = new Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        return filter.accept(new FilterPath(entry, fileSystem));
      }
    };
    DirectoryStream<Path> stream = delegate.newDirectoryStream(toDelegate(dir), wrappedFilter);
    stream = new FilterDirectoryStream(stream, fileSystem) {
      
      boolean closed;
      
      @Override
      public void close() throws IOException {
        try {
          if (!closed) {
            closed = true;
            onClose(dir, this);
          }
        } finally {
          super.close();
        }
      }
      
      @Override
      public String toString() {
        return "DirectoryStream(" + dir + ")";
      }
      
      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }
      
      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }
    };
    callOpenHook(dir, stream);
    return stream;
  }
}
