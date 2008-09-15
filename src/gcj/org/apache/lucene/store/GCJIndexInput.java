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
import gnu.gcj.RawData;

/** Native file-based {@link IndexInput} implementation, using GCJ.
 *
 */
public class GCJIndexInput extends IndexInput {

  private String file;
  private int fd;
  private long fileLength;
  public RawData data;
  public RawData pointer;
  private boolean isClone;

  public GCJIndexInput(String file) throws IOException {
    this.file = file;
    open();
  }

  private native void open() throws IOException;

  public native byte readByte() throws IOException;

  public native void readBytes(byte[] b, int offset, int len)
    throws IOException;

  public native int readVInt() throws IOException;

  public native long getFilePointer();

  public native void seek(long pos) throws IOException;

  public long length() { return fileLength; }

  public Object clone() {
    GCJIndexInput clone = (GCJIndexInput)super.clone();
    clone.isClone = true;
    return clone;
  }

  public void close() throws IOException {
    if (!isClone)
      doClose();
  }
  private native void doClose() throws IOException;
    
}


