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

/**
 * Used by MockRAMDirectory to create an output stream that
 * will throw an IOException on fake disk full, track max
 * disk space actually used, and maybe throw random
 * IOExceptions.
 */

public class MockRAMOutputStream extends RAMOutputStream {
  private MockRAMDirectory dir;
  private boolean first=true;
  
  byte[] singleByte = new byte[1];

  /** Construct an empty output buffer. */
  public MockRAMOutputStream(MockRAMDirectory dir, RAMFile f) {
    super(f);
    this.dir = dir;
  }

  public void close() throws IOException {
    super.close();

    // Now compute actual disk usage & track the maxUsedSize
    // in the MockRAMDirectory:
    long size = dir.getRecomputedActualSizeInBytes();
    if (size > dir.maxUsedSize) {
      dir.maxUsedSize = size;
    }
  }

  public void flush() throws IOException {
    dir.maybeThrowDeterministicException();
    super.flush();
  }

  public void writeByte(byte b) throws IOException {
    singleByte[0] = b;
    writeBytes(singleByte, 0, 1);
  }
  
  public void writeBytes(byte[] b, int offset, int len) throws IOException {
    long freeSpace = dir.maxSize - dir.sizeInBytes();
    long realUsage = 0;

    // If MockRAMDir crashed since we were opened, then
    // don't write anything:
    if (dir.crashed)
      throw new IOException("MockRAMDirectory was crashed");

    // Enforce disk full:
    if (dir.maxSize != 0 && freeSpace <= len) {
      // Compute the real disk free.  This will greatly slow
      // down our test but makes it more accurate:
      realUsage = dir.getRecomputedActualSizeInBytes();
      freeSpace = dir.maxSize - realUsage;
    }

    if (dir.maxSize != 0 && freeSpace <= len) {
      if (freeSpace > 0 && freeSpace < len) {
        realUsage += freeSpace;
        super.writeBytes(b, offset, (int) freeSpace);
      }
      if (realUsage > dir.maxUsedSize) {
        dir.maxUsedSize = realUsage;
      }
      throw new IOException("fake disk full at " + dir.getRecomputedActualSizeInBytes() + " bytes");
    } else {
      super.writeBytes(b, offset, len);
    }

    dir.maybeThrowDeterministicException();

    if (first) {
      // Maybe throw random exception; only do this on first
      // write to a new file:
      first = false;
      dir.maybeThrowIOException();
    }
  }
}
