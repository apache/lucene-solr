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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/** A delegating Directory that records which files were
 *  written to and deleted. */
public final class TrackingDirectoryWrapper extends Directory implements Closeable {

  private final Directory other;
  private final Set<String> createdFileNames = Collections.synchronizedSet(new HashSet<String>());

  public TrackingDirectoryWrapper(Directory other) {
    this.other = other;
  }

  @Override
  public String[] listAll() throws IOException {
    return other.listAll();
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return other.fileExists(name);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    createdFileNames.remove(name);
    other.deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    return other.fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    createdFileNames.add(name);
    return other.createOutput(name, context);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    other.sync(names);
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return other.openInput(name, context);
  }

  @Override
  public Lock makeLock(String name) {
    return other.makeLock(name);
  }

  @Override
  public void clearLock(String name) throws IOException {
    other.clearLock(name);
  }

  @Override
  public void close() throws IOException {
    other.close();
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    other.setLockFactory(lockFactory);
  }

  @Override
  public LockFactory getLockFactory() {
    return other.getLockFactory();
  }

  @Override
  public String getLockID() {
    return other.getLockID();
  }

  @Override
  public String toString() {
    return "TrackingDirectoryWrapper(" + other.toString() + ")";
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    createdFileNames.add(dest);
    other.copy(to, src, dest, context);
  }

  @Override
  public Directory.IndexInputSlicer createSlicer(final String name, final IOContext context) throws IOException {
    return other.createSlicer(name, context);
  }

  // maybe clone before returning.... all callers are
  // cloning anyway....
  public Set<String> getCreatedFiles() {
    return createdFileNames;
  }

  public Directory getDelegate() {
    return other;
  }
}
