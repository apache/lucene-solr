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
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**  
 * A {@code FilterFileSystemProvider} contains another 
 * {@code FileSystemProvider}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 */
public abstract class FilterFileSystemProvider extends FileSystemProvider {
  
  /** 
   * The underlying {@code FileSystemProvider}. 
   */
  protected final FileSystemProvider delegate;
  /** 
   * The underlying {@code FileSystem} instance. 
   */
  protected FileSystem fileSystem;
  /** 
   * The URI scheme for this provider.
   */
  protected final String scheme;
  
  /**
   * Construct a {@code FilterFileSystemProvider} indicated by
   * the specified {@code scheme} and wrapping functionality of the
   * provider of the specified base filesystem.
   * @param scheme URI scheme
   * @param delegateInstance specified base filesystem.
   */
  public FilterFileSystemProvider(String scheme, FileSystem delegateInstance) {
    this.scheme = Objects.requireNonNull(scheme);
    Objects.requireNonNull(delegateInstance);
    this.delegate = delegateInstance.provider();
    this.fileSystem = new FilterFileSystem(this, delegateInstance);
  }
  
  /**
   * Construct a {@code FilterFileSystemProvider} indicated by
   * the specified {@code scheme} and wrapping functionality of the
   * provider. You must set the singleton {@code filesystem} yourself.
   * @param scheme URI scheme
   * @param delegate specified base provider.
   */
  public FilterFileSystemProvider(String scheme, FileSystemProvider delegate) {
    this.scheme = Objects.requireNonNull(scheme);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public String getScheme() {
    return scheme;
  }

  @Override
  public FileSystem newFileSystem(URI uri, Map<String,?> env) throws IOException {
    if (fileSystem == null) {
      throw new IllegalStateException("subclass did not initialize singleton filesystem");
    }
    return fileSystem;
  }
  
  @Override
  public FileSystem newFileSystem(Path path, Map<String,?> env) throws IOException {
    if (fileSystem == null) {
      throw new IllegalStateException("subclass did not initialize singleton filesystem");
    }
    return fileSystem;
  }

  @Override
  public FileSystem getFileSystem(URI uri) {
    if (fileSystem == null) {
      throw new IllegalStateException("subclass did not initialize singleton filesystem");
    }
    return fileSystem;
  }

  @Override
  public Path getPath(URI uri) {
    if (fileSystem == null) {
      throw new IllegalStateException("subclass did not initialize singleton filesystem");
    }
    Path path = delegate.getPath(uri);
    return new FilterPath(path, fileSystem);
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    delegate.createDirectory(toDelegate(dir), attrs);
  }

  @Override
  public void delete(Path path) throws IOException {
    delegate.delete(toDelegate(path));
  }

  @Override
  public void copy(Path source, Path target, CopyOption... options) throws IOException {
    delegate.copy(toDelegate(source), toDelegate(target), options);
  }

  @Override
  public void move(Path source, Path target, CopyOption... options) throws IOException {
    delegate.move(toDelegate(source), toDelegate(target), options);
  }

  @Override
  public boolean isSameFile(Path path, Path path2) throws IOException {
    return delegate.isSameFile(toDelegate(path), toDelegate(path2));
  }

  @Override
  public boolean isHidden(Path path) throws IOException {
    return delegate.isHidden(toDelegate(path));
  }

  @Override
  public FileStore getFileStore(Path path) throws IOException {
    return delegate.getFileStore(toDelegate(path));
  }

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    delegate.checkAccess(toDelegate(path), modes);
  }

  @Override
  public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
    return delegate.getFileAttributeView(toDelegate(path), type, options);
  }

  @Override
  public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
    return delegate.readAttributes(toDelegate(path), type, options);
  }

  @Override
  public Map<String,Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
    return delegate.readAttributes(toDelegate(path), attributes, options);
  }

  @Override
  public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
    delegate.setAttribute(toDelegate(path), attribute, value, options);
  }

  @Override
  public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
    return delegate.newInputStream(toDelegate(path), options);
  }

  @Override
  public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
    return delegate.newOutputStream(toDelegate(path), options);
  }

  @Override
  public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    return delegate.newFileChannel(toDelegate(path), options, attrs);
  }

  @Override
  public AsynchronousFileChannel newAsynchronousFileChannel(Path path, Set<? extends OpenOption> options, ExecutorService executor, FileAttribute<?>... attrs) throws IOException {
    return delegate.newAsynchronousFileChannel(toDelegate(path), options, executor, attrs);
  }
  
  @Override
  public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    return delegate.newByteChannel(toDelegate(path), options, attrs);
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(Path dir, final Filter<? super Path> filter) throws IOException {
    Filter<Path> wrappedFilter = new Filter<Path>() {
      @Override
      public boolean accept(Path entry) throws IOException {
        return filter.accept(new FilterPath(entry, fileSystem));
      }
    };
    return new FilterDirectoryStream(delegate.newDirectoryStream(toDelegate(dir), wrappedFilter), fileSystem);
  }

  @Override
  public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException {
    delegate.createSymbolicLink(toDelegate(link), toDelegate(target), attrs);
  }

  @Override
  public void createLink(Path link, Path existing) throws IOException {
    delegate.createLink(toDelegate(link), toDelegate(existing));
  }

  @Override
  public boolean deleteIfExists(Path path) throws IOException {
    return delegate.deleteIfExists(toDelegate(path));
  }

  @Override
  public Path readSymbolicLink(Path link) throws IOException {
    return delegate.readSymbolicLink(toDelegate(link));
  }

  protected Path toDelegate(Path path) {
    if (path instanceof FilterPath) {
      FilterPath fp = (FilterPath) path;
      if (fp.fileSystem != fileSystem) {
        throw new ProviderMismatchException("mismatch, expected: " + fileSystem.provider().getClass() + ", got: " + fp.fileSystem.provider().getClass());
      }
      return fp.delegate;
    } else {
      throw new ProviderMismatchException("mismatch, expected: FilterPath, got: " + path.getClass());
    }
  }
  
  /** 
   * Override to trigger some behavior when the filesystem is closed.
   * <p>
   * This is always called for each FilterFileSystemProvider in the chain.
   */
  protected void onClose() {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + delegate + ")";
  }
}
