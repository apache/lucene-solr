package org.apache.lucene.codecs.compressing;

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

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;

/**
 * A {@link DataOutput} that can be used to build a byte[].
 */
final class GrowableByteArrayDataOutput extends DataOutput {

  byte[] bytes;
  int length;

  GrowableByteArrayDataOutput(int cp) {
    this.bytes = new byte[ArrayUtil.oversize(cp, 1)];
    this.length = 0;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    if (length >= bytes.length) {
      bytes = ArrayUtil.grow(bytes);
    }
    bytes[length++] = b;
  }

  @Override
  public void writeBytes(byte[] b, int off, int len) throws IOException {
    final int newLength = length + len;
    if (newLength > bytes.length) {
      bytes = ArrayUtil.grow(bytes, newLength);
    }
    System.arraycopy(b, off, bytes, length, len);
    length = newLength;
  }

}
