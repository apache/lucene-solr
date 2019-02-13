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

package org.apache.solr.common.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A byte[] backed String
 */
public interface Utf8CharSequence extends CharSequence , Comparable, Cloneable {

  /**
   * Write the bytes into a buffer. The objective is to avoid the local bytes being exposed to
   * other classes if the implementation is expected to be immutable. It writes as many bytes as
   * possible into the buffer and then return how many bytes were written. It's the responsibility
   * of the caller to call this method repeatedly and ensure that everything is completely written
   *
   * @param start  position from which to start writing
   * @param buffer the buffer to which to write to
   * @param pos    position to start writing
   * @return no:of bytes written
   */
  int write(int start, byte[] buffer, int pos);

  /**
   * The size of utf8 bytes
   *
   * @return the size
   */
  int size();

  byte byteAt(int idx);

  @Override
  default int compareTo(Object o) {
    if(o == null) return 1;
    return toString().compareTo(o.toString());
  }

  /**
   * Creates  a byte[] and copy to it first before writing it out to the output
   *
   * @param os The sink
   */
  default void write(OutputStream os) throws IOException {
    byte[] buf = new byte[1024];
    int start = 0;
    int totalWritten = 0;
    for (; ; ) {
      if (totalWritten >= size()) break;
      int sz = write(start, buf, 0);
      totalWritten += sz;
      if (sz > 0) os.write(buf, 0, sz);
      start += sz;
    }
  }

  Utf8CharSequence clone();

}
