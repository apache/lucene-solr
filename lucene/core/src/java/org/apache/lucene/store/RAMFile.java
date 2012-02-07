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

import java.util.ArrayList;

/** @lucene.internal */
public class RAMFile {
  protected ArrayList<byte[]> buffers = new ArrayList<byte[]>();
  long length;
  RAMDirectory directory;
  protected long sizeInBytes;

  // File used as buffer, in no RAMDirectory
  public RAMFile() {}
  
  RAMFile(RAMDirectory directory) {
    this.directory = directory;
  }

  // For non-stream access from thread that might be concurrent with writing
  public synchronized long getLength() {
    return length;
  }

  protected synchronized void setLength(long length) {
    this.length = length;
  }

  protected final byte[] addBuffer(int size) {
    byte[] buffer = newBuffer(size);
    synchronized(this) {
      buffers.add(buffer);
      sizeInBytes += size;
    }

    if (directory != null) {
      directory.sizeInBytes.getAndAdd(size);
    }
    return buffer;
  }

  protected final synchronized byte[] getBuffer(int index) {
    return buffers.get(index);
  }

  protected final synchronized int numBuffers() {
    return buffers.size();
  }

  /**
   * Expert: allocate a new buffer. 
   * Subclasses can allocate differently. 
   * @param size size of allocated buffer.
   * @return allocated buffer.
   */
  protected byte[] newBuffer(int size) {
    return new byte[size];
  }

  public synchronized long getSizeInBytes() {
    return sizeInBytes;
  }
  
}
