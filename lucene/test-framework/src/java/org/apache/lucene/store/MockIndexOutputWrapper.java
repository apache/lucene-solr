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
    this.dir = dir;
    this.name = name;
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    try {
      dir.maybeThrowDeterministicException();
    } finally {
      delegate.close();
      if (dir.trackDiskUsage) {
        // Now compute actual disk usage & track the maxUsedSize
        // in the MockDirectoryWrapper:
        long size = dir.getRecomputedActualSizeInBytes();
        if (size > dir.maxUsedSize) {
          dir.maxUsedSize = size;
        }
      }
      dir.removeIndexOutput(this, name);
    }
  }

  @Override
  public void flush() throws IOException {
    dir.maybeThrowDeterministicException();
    delegate.flush();
  }

  @Override
  public void writeByte(byte b) throws IOException {
    singleByte[0] = b;
    writeBytes(singleByte, 0, 1);
  }
  
  @Override
  public void writeBytes(byte[] b, int offset, int len) throws IOException {
    long freeSpace = dir.maxSize == 0 ? 0 : dir.maxSize - dir.sizeInBytes();
    long realUsage = 0;
    // If MockRAMDir crashed since we were opened, then
    // don't write anything:
    if (dir.crashed)
      throw new IOException("MockRAMDirectory was crashed; cannot write to " + name);

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
        delegate.writeBytes(b, offset, (int) freeSpace);
      }
      if (realUsage > dir.maxUsedSize) {
        dir.maxUsedSize = realUsage;
      }
      String message = "fake disk full at " + dir.getRecomputedActualSizeInBytes() + " bytes when writing " + name + " (file length=" + delegate.length();
      if (freeSpace > 0) {
        message += "; wrote " + freeSpace + " of " + len + " bytes";
      }
      message += ")";
      if (LuceneTestCase.VERBOSE) {
        System.out.println(Thread.currentThread().getName() + ": MDW: now throw fake disk full");
        new Throwable().printStackTrace(System.out);
      }
      throw new IOException(message);
    } else {
      if (dir.randomState.nextInt(200) == 0) {
        final int half = len/2;
        delegate.writeBytes(b, offset, half);
        Thread.yield();
        delegate.writeBytes(b, offset+half, len-half);
      } else {
        delegate.writeBytes(b, offset, len);
      }
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
  public void seek(long pos) throws IOException {
    delegate.seek(pos);
  }

  @Override
  public long length() throws IOException {
    return delegate.length();
  }

  @Override
  public void setLength(long length) throws IOException {
    delegate.setLength(length);
  }

  @Override
  public void copyBytes(DataInput input, long numBytes) throws IOException {
    delegate.copyBytes(input, numBytes);
    // TODO: we may need to check disk full here as well
    dir.maybeThrowDeterministicException();
  }

  @Override
  public String toString() {
    return "MockIndexOutputWrapper(" + delegate + ")";
  }
}
