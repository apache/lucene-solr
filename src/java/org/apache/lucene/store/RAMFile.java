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
import java.io.Serializable;

class RAMFile implements Serializable {

  private static final long serialVersionUID = 1l;

  private ArrayList buffers = new ArrayList();
  long length;
  RAMDirectory directory;
  long sizeInBytes;                  // Only maintained if in a directory; updates synchronized on directory

  // This is publicly modifiable via Directory.touchFile(), so direct access not supported
  private long lastModified = System.currentTimeMillis();

  // File used as buffer, in no RAMDirectory
  RAMFile() {}
  
  RAMFile(RAMDirectory directory) {
    this.directory = directory;
  }

  // For non-stream access from thread that might be concurrent with writing
  synchronized long getLength() {
    return length;
  }

  synchronized void setLength(long length) {
    this.length = length;
  }

  // For non-stream access from thread that might be concurrent with writing
  synchronized long getLastModified() {
    return lastModified;
  }

  synchronized void setLastModified(long lastModified) {
    this.lastModified = lastModified;
  }

  final synchronized byte[] addBuffer(int size) {
    byte[] buffer = newBuffer(size);
    if (directory!=null)
      synchronized (directory) {             // Ensure addition of buffer and adjustment to directory size are atomic wrt directory
        buffers.add(buffer);
        directory.sizeInBytes += size;
        sizeInBytes += size;
      }
    else
      buffers.add(buffer);
    return buffer;
  }

  final synchronized byte[] getBuffer(int index) {
    return (byte[]) buffers.get(index);
  }

  final synchronized int numBuffers() {
    return buffers.size();
  }

  /**
   * Expert: allocate a new buffer. 
   * Subclasses can allocate differently. 
   * @param size size of allocated buffer.
   * @return allocated buffer.
   */
  byte[] newBuffer(int size) {
    return new byte[size];
  }

  // Only valid if in a directory
  long getSizeInBytes() {
    synchronized (directory) {
      return sizeInBytes;
    }
  }
  
}
