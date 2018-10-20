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
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**  
 * A {@code FilterFileSystem} contains another 
 * {@code FileSystem}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 */
public class FilterFileSystem extends FileSystem {
  
  /**
   * FileSystemProvider that created this FilterFileSystem
   */
  protected final FilterFileSystemProvider parent;
  
  /** 
   * The underlying {@code FileSystem} instance. 
   */
  protected final FileSystem delegate;
  
  /**
   * Construct a {@code FilterFileSystem} based on 
   * the specified base filesystem.
   * <p>
   * Note that base filesystem is closed if this filesystem is closed,
   * however the default filesystem provider will never be closed, it doesn't
   * support that.
   * @param delegate specified base channel.
   */
  public FilterFileSystem(FilterFileSystemProvider parent, FileSystem delegate) {
    this.parent = Objects.requireNonNull(parent);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public FileSystemProvider provider() {
    return parent;
  }

  @Override
  public void close() throws IOException {
    if (delegate == FileSystems.getDefault()) {
      // you can't close the default provider!
      parent.onClose();
    } else {
      try (FileSystem d = delegate) {
        assert d != null; // avoid stupid compiler warning
        parent.onClose();
      }
    }
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public boolean isReadOnly() {
    return delegate.isReadOnly();
  }

  @Override
  public String getSeparator() {
    return delegate.getSeparator();
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    final Iterable<Path> roots = delegate.getRootDirectories();
    return () -> {
      final Iterator<Path> iterator = roots.iterator();
      return new Iterator<Path>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public Path next() {
          return new FilterPath(iterator.next(), FilterFileSystem.this);
        }
        
        @Override
        public void remove() {
          iterator.remove();
        }
      };
    };
  }

  @Override
  public Iterable<FileStore> getFileStores() {
    final Iterable<FileStore> fileStores = delegate.getFileStores();
    return () -> {
      final Iterator<FileStore> iterator = fileStores.iterator();
      return new Iterator<FileStore>() {
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public FileStore next() {
          return new FilterFileStore(iterator.next(), parent.getScheme()) {};
        }
        
        @Override
        public void remove() {
          iterator.remove();
        }
      };
    };
  }

  @Override
  public Set<String> supportedFileAttributeViews() {
    return delegate.supportedFileAttributeViews();
  }

  @Override
  public Path getPath(String first, String... more) {
    return new FilterPath(delegate.getPath(first, more), this);
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    final PathMatcher matcher = delegate.getPathMatcher(syntaxAndPattern);
    return path -> {
      if (path instanceof FilterPath) {
        return matcher.matches(((FilterPath)path).delegate);
      }
      return false;
    };
  }

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    return delegate.getUserPrincipalLookupService();
  }

  @Override
  public WatchService newWatchService() throws IOException {
    return delegate.newWatchService();
  }

  /** Returns the {@code FileSystem} we wrap. */
  public FileSystem getDelegate() {
    return delegate;
  }

  /** Returns the {@code FilterFileSystemProvider} sent to this on init. */
  public FileSystemProvider getParent() {
    return parent;
  }
}
