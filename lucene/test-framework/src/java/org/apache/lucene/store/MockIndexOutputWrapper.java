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

import org.apache.lucene.util.LuceneTestCase;

/**
 * Used by MockRAMDirectory to create an output stream that
 * will throw an IOException on fake disk full, track max
 * disk space actually used, and maybe throw random
 * IOExceptions.
 */

public class MockIndexOutputWrapper extends IndexOutput {
  private MockDirectoryWrapper dir;
  private final IndexOutput delegate;
  private boolean first=true;
  final String name;
  
  byte[] singleByte = new byte[1];

  /** Construct an empty output buffer. */
  public MockIndexOutputWrapper(MockDirectoryWrapper dir, IndexOutput delegate, String name) {
    super("MockIndexOutputWrapper(" + delegate + ")");
    this.dir = dir;
    this.name = name;
    this.delegate = delegate;
  }

  private void checkCrashed() throws IOException {
    // If MockRAMDir crashed since we were opened, then don't write anything
    if (dir.crashed) {
      throw new IOException("MockRAMDirectory was crashed; cannot write to " + name);
    }
  }
  
  private void checkDiskFull(byte[] b, int offset, DataInput in, long len) throws IOException {
    long freeSpace = dir.maxSize == 0 ? 0 : dir.maxSize - dir.sizeInBytes();
    long realUsage = 0;

    // Enforce disk full:
    if (dir.maxSize != 0 && freeSpace <= len) {
      // Compute the real disk free.  This will greatly slow
      // down our test but makes it more accurate:
      realUsage = dir.getRecomputedActualSizeInBytes();
      freeSpace = dir.maxSize - realUsage;
    }

    if (dir.maxSize != 0 && freeSpace <= len) {
      if (freeSpace > 0) {
        realUsage += freeSpace;
        if (b != null) {
          delegate.writeBytes(b, offset, (int) freeSpace);
        } else {
          delegate.copyBytes(in, (int) freeSpace);
        }
      }
      if (realUsage > dir.maxUsedSize) {
        dir.maxUsedSize = realUsage;
      }
      String message = "fake disk full at " + dir.getRecomputedActualSizeInBytes() + " bytes when writing " + name + " (file length=" + delegate.getFilePointer();
      if (freeSpace > 0) {
        message += "; wrote " + freeSpace + " of " + len + " bytes";
      }
      message += ")";
      if (LuceneTestCase.VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": MDW: now throw fake disk full");
        new Throwable().printStackTrace(System.out);
      }
      throw new IOException(message);
    }
  }
  
  private boolean closed;
  
  @Override
  public void close() throws IOException {
    if (closed) {
      delegate.close(); // don't mask double-close bugs
      return;
    }
    closed = true;
    
    try (Closeable delegate = this.delegate) {
      assert delegate != null;
      dir.maybeThrowDeterministicException();
    } finally {
      dir.removeIndexOutput(this, name);
      if (dir.trackDiskUsage) {
        // Now compute actual disk usage & track the maxUsedSize
        // in the MockDirectoryWrapper:
        long size = dir.getRecomputedActualSizeInBytes();
        if (size > dir.maxUsedSize) {
          dir.maxUsedSize = size;
        }
      }
    }
  }
  
  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public void writeByte(byte b) throws IOException {
    singleByte[0] = b;
    writeBytes(singleByte, 0, 1);
  }
  
  @Override
  public void writeBytes(byte[] b, int offset, int len) throws IOException {
    ensureOpen();
    checkCrashed();
    checkDiskFull(b, offset, null, len);
    
    if (dir.randomState.nextInt(200) == 0) {
      final int half = len/2;
      delegate.writeBytes(b, offset, half);
      Thread.yield();
      delegate.writeBytes(b, offset+half, len-half);
    } else {
      delegate.writeBytes(b, offset, len);
    }

    dir.maybeThrowDeterministicException();

    if (first) {
      // Maybe throw random exception; only do this on first
      // write to a new file:
      first = false;
      dir.maybeThrowIOException(name);
    }
  }

  @Override
  public long getFilePointer() {
    return delegate.getFilePointer();
  }

  @Override
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    ensureOpen();
    checkCrashed();
    checkDiskFull(null, 0, input, numBytes);
    
    delegate.copyBytes(input, numBytes);
    dir.maybeThrowDeterministicException();
  }

  @Override
  public long getChecksum() throws IOException {
    return delegate.getChecksum();
  }

  @Override
  public String toString() {
    return "MockIndexOutputWrapper(" + delegate + ")";
  }
}
