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

public class StringBytes {
  byte[] bytes;

  /**
   * Offset of first valid byte.
   */
  int offset;

  /**
   * Length of used bytes.
   */
  int length;
  private int hash;

  public StringBytes(byte[] bytes, int offset, int length) {
    reset(bytes, offset, length);
  }

  StringBytes reset(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
    hash = bytes == null ? 0 : Hash.murmurhash3_x86_32(bytes, offset, length, 0);
    return this;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other instanceof StringBytes) {
      return this.bytesEquals((StringBytes) other);
    }
    return false;
  }

  boolean bytesEquals(StringBytes other) {
    assert other != null;
    if (length == other.length) {
      int otherUpto = other.offset;
      final byte[] otherBytes = other.bytes;
      final int end = offset + length;
      for (int upto = offset; upto < end; upto++, otherUpto++) {
        if (bytes[upto] != otherBytes[otherUpto]) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
