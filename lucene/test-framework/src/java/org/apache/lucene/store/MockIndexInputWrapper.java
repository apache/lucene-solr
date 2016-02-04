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
package org.apache.lucene.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Used by MockDirectoryWrapper to create an input stream that
 * keeps track of when it's been closed.
 */

public class MockIndexInputWrapper extends IndexInput {
  private MockDirectoryWrapper dir;
  final String name;
  private IndexInput delegate;
  private boolean isClone;
  private boolean closed;

  /** Construct an empty output buffer. */
  public MockIndexInputWrapper(MockDirectoryWrapper dir, String name, IndexInput delegate) {
    super("MockIndexInputWrapper(name=" + name + " delegate=" + delegate + ")");
    this.name = name;
    this.dir = dir;
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      delegate.close(); // don't mask double-close bugs
      return;
    }
    closed = true;
    
    try (Closeable delegate = this.delegate) {
      // Pending resolution on LUCENE-686 we may want to
      // remove the conditional check so we also track that
      // all clones get closed:
      assert delegate != null;
      if (!isClone) {
        dir.removeIndexInput(this, name);
      }
      dir.maybeThrowDeterministicException();
    }
  }
  
  private void ensureOpen() {
    if (closed) {
      throw new RuntimeException("Abusing closed IndexInput!");
    }
  }

  @Override
  public MockIndexInputWrapper clone() {
    ensureOpen();
    if (dir.verboseClone) {
      new Exception("clone: " + this).printStackTrace(System.out);
    }
    dir.inputCloneCount.incrementAndGet();
    IndexInput iiclone = delegate.clone();
    MockIndexInputWrapper clone = new MockIndexInputWrapper(dir, name, iiclone);
    clone.isClone = true;
    // Pending resolution on LUCENE-686 we may want to
    // uncomment this code so that we also track that all
    // clones get closed:
    /*
    synchronized(dir.openFiles) {
      if (dir.openFiles.containsKey(name)) {
        Integer v = (Integer) dir.openFiles.get(name);
        v = Integer.valueOf(v.intValue()+1);
        dir.openFiles.put(name, v);
      } else {
        throw new RuntimeException("BUG: cloned file was not open?");
      }
    }
    */
    return clone;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    ensureOpen();
    if (dir.verboseClone) {
      new Exception("slice: " + this).printStackTrace(System.out);
    }
    dir.inputCloneCount.incrementAndGet();
    IndexInput slice = delegate.slice(sliceDescription, offset, length);
    MockIndexInputWrapper clone = new MockIndexInputWrapper(dir, sliceDescription, slice);
    clone.isClone = true;
    return clone;
  }

  @Override
  public long getFilePointer() {
    ensureOpen();
    return delegate.getFilePointer();
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    delegate.seek(pos);
  }

  @Override
  public long length() {
    ensureOpen();
    return delegate.length();
  }

  @Override
  public byte readByte() throws IOException {
    ensureOpen();
    return delegate.readByte();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    ensureOpen();
    delegate.readBytes(b, offset, len);
  }

  @Override
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer)
      throws IOException {
    ensureOpen();
    delegate.readBytes(b, offset, len, useBuffer);
  }

  @Override
  public short readShort() throws IOException {
    ensureOpen();
    return delegate.readShort();
  }

  @Override
  public int readInt() throws IOException {
    ensureOpen();
    return delegate.readInt();
  }

  @Override
  public long readLong() throws IOException {
    ensureOpen();
    return delegate.readLong();
  }

  @Override
  public String readString() throws IOException {
    ensureOpen();
    return delegate.readString();
  }

  @Override
  public Map<String,String> readStringStringMap() throws IOException {
    ensureOpen();
    return delegate.readStringStringMap();
  }

  @Override
  public int readVInt() throws IOException {
    ensureOpen();
    return delegate.readVInt();
  }

  @Override
  public long readVLong() throws IOException {
    ensureOpen();
    return delegate.readVLong();
  }

  @Override
  public int readZInt() throws IOException {
    ensureOpen();
    return delegate.readZInt();
  }

  @Override
  public long readZLong() throws IOException {
    ensureOpen();
    return delegate.readZLong();
  }

  @Override
  public Set<String> readStringSet() throws IOException {
    ensureOpen();
    return delegate.readStringSet();
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    ensureOpen();
    super.skipBytes(numBytes);
  }

  @Override
  public String toString() {
    return "MockIndexInputWrapper(" + delegate + ")";
  }
}

