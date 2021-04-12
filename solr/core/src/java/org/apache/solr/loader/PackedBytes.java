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

package org.apache.solr.loader;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;

public class PackedBytes {
  public byte[] buf;
  public int pos = 0;
  private static final int INCR = 1024;

  public PackedBytes() {
    this.buf = new byte[INCR];
  }

  public void clear() {
    pos = 0;
  }

  public PackedBytes(byte[] buf) {
    this.buf = buf;
  }

  public BytesRef add(ByteArrayUtf8CharSequence utf8) {
    return add(utf8.getBuf(), utf8.offset(), utf8.size());
  }

  public BytesRef add(byte[] buf, int offset, int len) {
    ensureCapacity(len);
    System.arraycopy(buf, offset, this.buf, pos, len);
    pos += len;
    return new BytesRef(buf, pos - len, len);
  }

  private void ensureCapacity(int len) {
    if (buf.length < pos + len) return;
    byte[] newBytes = new byte[pos + len + INCR];
    System.arraycopy(buf, 0, newBytes, 0, pos);
    buf = newBytes;
  }

}
