package org.apache.lucene.store;

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
import java.util.Collection;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util._TestUtil;

/**
 * Calls check index on close.
 */
// do NOT make any methods in this class synchronized, volatile
// do NOT import anything from the concurrency package.
// no randoms, no nothing.
public class BaseDirectoryWrapper extends Directory {
  /** our in directory */
  protected final Directory delegate;
  
  private boolean checkIndexOnClose = true;
  private boolean crossCheckTermVectorsOnClose = true;

  public BaseDirectoryWrapper(Directory delegate) {
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    if (checkIndexOnClose && DirectoryReader.indexExists(this)) {
      _TestUtil.checkIndex(this, crossCheckTermVectorsOnClose);
    }
    delegate.close();
  }
  
  public boolean isOpen() {
    return isOpen;
  }
  
  /**
   * Set whether or not checkindex should be run
   * on close
   */
  public void setCheckIndexOnClose(boolean value) {
    this.checkIndexOnClose = value;
  }
  
  public boolean getCheckIndexOnClose() {
    return checkIndexOnClose;
  }

  public void setCrossCheckTermVectorsOnClose(boolean value) {
    this.crossCheckTermVectorsOnClose = value;
  }

  public boolean getCrossCheckTermVectorsOnClose() {
    return crossCheckTermVectorsOnClose;
  }

  // directory methods: delegate

  @Override
  public String[] listAll() throws IOException {
    return delegate.listAll();
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return delegate.fileExists(name);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    delegate.deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    return delegate.fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return delegate.createOutput(name, context);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    delegate.sync(names);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return delegate.openInput(name, context);
  }

  @Override
  public Lock makeLock(String name) {
    return delegate.makeLock(name);
  }

  @Override
  public void clearLock(String name) throws IOException {
    delegate.clearLock(name);
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    delegate.setLockFactory(lockFactory);
  }

  @Override
  public LockFactory getLockFactory() {
    return delegate.getLockFactory();
  }

  @Override
  public String getLockID() {
    return delegate.getLockID();
  }

  @Override
  public String toString() {
    return "BaseDirectoryWrapper(" + delegate.toString() + ")";
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    delegate.copy(to, src, dest, context);
  }

  @Override
  public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
    return delegate.createSlicer(name, context);
  }  
}
