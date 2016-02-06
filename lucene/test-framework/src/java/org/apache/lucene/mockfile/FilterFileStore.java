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
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Objects;

/**  
 * A {@code FilterFileStore} contains another 
 * {@code FileStore}, which it uses as its basic 
 * source of data, possibly transforming the data along the 
 * way or providing additional functionality. 
 */
public class FilterFileStore extends FileStore {
  
  /** 
   * The underlying {@code FileStore} instance. 
   */
  protected final FileStore delegate;
  
  /**
   * URI scheme used for this instance.
   */
  protected final String scheme;
  
  /**
   * Construct a {@code FilterFileStore} based on 
   * the specified base store.
   * @param delegate specified base store.
   * @param scheme URI scheme identifying this instance.
   */
  public FilterFileStore(FileStore delegate, String scheme) {
    this.delegate = Objects.requireNonNull(delegate);
    this.scheme = Objects.requireNonNull(scheme);
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public String type() {
    return delegate.type();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public boolean isReadOnly() {
    return delegate.isReadOnly();
  }

  @Override
  public long getTotalSpace() throws IOException {
    return delegate.getTotalSpace();
  }

  @Override
  public long getUsableSpace() throws IOException {
    return delegate.getUsableSpace();
  }

  @Override
  public long getUnallocatedSpace() throws IOException {
    return delegate.getUnallocatedSpace();
  }

  @Override
  public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
    return delegate.supportsFileAttributeView(type);
  }

  @Override
  public boolean supportsFileAttributeView(String name) {
    return delegate.supportsFileAttributeView(name);
  }

  @Override
  public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
    return delegate.getFileStoreAttributeView(type);
  }

  @Override
  public Object getAttribute(String attribute) throws IOException {
    return delegate.getAttribute(attribute);
  }
}
