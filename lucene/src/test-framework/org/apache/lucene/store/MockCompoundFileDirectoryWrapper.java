package org.apache.lucene.store;

/**
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

public class MockCompoundFileDirectoryWrapper extends CompoundFileDirectory {
  private final MockDirectoryWrapper parent;
  private final CompoundFileDirectory delegate;
  private final String name;
  
  public MockCompoundFileDirectoryWrapper(String name, MockDirectoryWrapper parent, CompoundFileDirectory delegate, boolean forWrite) throws IOException {
    super(parent, name, IOContext.DEFAULT);
    this.name = name;
    this.parent = parent;
    this.delegate = delegate;
    // don't initialize here since we delegate everything - if not initialized a direct call will cause an assert to fail!
    parent.addFileHandle(this, name, !forWrite);
  }
  
  @Override
  public Directory getDirectory() {
    return delegate.getDirectory();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public synchronized void close() throws IOException {
    delegate.close();
    parent.removeOpenFile(this, name);
  }

  @Override
  public synchronized IndexInput openInput(String id, IOContext context) throws IOException {
    return delegate.openInput(id, context);
  }

  @Override
  public String[] listAll() {
    return delegate.listAll();
  }

  @Override
  public boolean fileExists(String name) {
    return delegate.fileExists(name);
  }

  @Override
  public long fileModified(String name) throws IOException {
    return delegate.fileModified(name);
  }

  @Override
  public void deleteFile(String name) {
    delegate.deleteFile(name);
  }

  @Override
  public void renameFile(String from, String to) {
    delegate.renameFile(from, to);
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
    return "MockCompoundFileDirectoryWrapper(" + super.toString() + ")";
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    delegate.copy(to, src, dest, context);
  }

  @Override
  public IndexInput openInputSlice(String id, long offset, long length, int readBufferSize) throws IOException {
    return delegate.openInputSlice(id, offset, length, readBufferSize);
  }

  @Override
  public CompoundFileDirectory createCompoundOutput(String name, IOContext context) throws IOException {
    return delegate.createCompoundOutput(name, context);
  }

  @Override
  public CompoundFileDirectory openCompoundInput(String name, IOContext context)
      throws IOException {
    return delegate.openCompoundInput(name, context);
  }

}
