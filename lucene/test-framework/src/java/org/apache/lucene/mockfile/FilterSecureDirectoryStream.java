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
import java.nio.channels.SeekableByteChannel;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.util.Iterator;
import java.util.Set;

/**  
 * A {@code FilterSecureDirectoryStream} contains another 
 * {@code SecureDirectoryStream}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 */
public class FilterSecureDirectoryStream<T> implements SecureDirectoryStream<T> {
  
  /** 
   * The underlying {@code SecureDirectoryStream} instance. 
   */
  protected final SecureDirectoryStream<T> delegate;
  
  /**
   * Construct a {@code FilterSecureDirectoryStream} based on 
   * the specified base stream.
   * <p>
   * Note that base stream is closed if this stream is closed.
   * @param delegate specified base stream.
   */
  public FilterSecureDirectoryStream(SecureDirectoryStream<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public SecureDirectoryStream<T> newDirectoryStream(T path, LinkOption... options) throws IOException {
    return delegate.newDirectoryStream(path, options);
  }

  @Override
  public SeekableByteChannel newByteChannel(T path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    return delegate.newByteChannel(path, options, attrs);
  }

  @Override
  public void deleteFile(T path) throws IOException {
    delegate.deleteFile(path);
  }

  @Override
  public void deleteDirectory(T path) throws IOException {
    delegate.deleteDirectory(path);
  }

  @Override
  public void move(T srcpath, SecureDirectoryStream<T> targetdir, T targetpath) throws IOException {
    delegate.move(srcpath, targetdir, targetpath);
  }

  @Override
  public <V extends FileAttributeView> V getFileAttributeView(Class<V> type) {
    return delegate.getFileAttributeView(type);
  }

  @Override
  public <V extends FileAttributeView> V getFileAttributeView(T path, Class<V> type, LinkOption... options) {
    return delegate.getFileAttributeView(path, type, options);
  }
}
