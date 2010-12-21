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

package org.apache.solr.util;

import org.apache.lucene.util.BytesRef;
import org.apache.noggit.CharArr;


public class ByteUtils {
 /** A binary term consisting of a number of 0xff bytes, likely to be bigger than other terms
   *  one would normally encounter, and definitely bigger than any UTF-8 terms */
  public static final BytesRef bigTerm = new BytesRef(
      new byte[] {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1}
  );
  
  /** Converts utf8 to utf16 and returns the number of 16 bit Java chars written.
   * Full characters are read, even if this reads past the length passed (and can result in
   * an ArrayOutOfBoundsException if invalid UTF8 is passed).  Explicit checks for valid UTF8 are not performed.
   * The char[] out should probably have enough room to hold the worst case of each byte becoming a Java char.
   */
  public static int UTF8toUTF16(byte[] utf8, int offset, int len, char[] out, int out_offset) {
    int out_start = out_offset;
    final int limit = offset + len;
    while (offset < limit) {
      int b = utf8[offset++]&0xff;

      if (b < 0xc0) {
        assert b < 0x80;
        out[out_offset++] = (char)b;
      } else if (b < 0xe0) {
        out[out_offset++] = (char)(((b&0x1f)<<6) + (utf8[offset++]&0x3f));
      } else if (b < 0xf0) {
        out[out_offset++] = (char)(((b&0xf)<<12) + ((utf8[offset]&0x3f)<<6) + (utf8[offset+1]&0x3f));
        offset += 2;
      } else {
        assert b < 0xf8;
        int ch = ((b&0x7)<<18) + ((utf8[offset]&0x3f)<<12) + ((utf8[offset+1]&0x3f)<<6) + (utf8[offset+2]&0x3f);
        offset += 3;
        if (ch < 0xffff) {
          out[out_offset++] = (char)ch;
        } else {
          int chHalf = ch - 0x0010000;
          out[out_offset++] = (char) ((chHalf >> 10) + 0xD800);
          out[out_offset++] = (char) ((chHalf & 0x3FFL) + 0xDC00);          
        }
      }
    }

    return out_offset - out_start;
  }

  /** Convert UTF8 bytes into UTF16 characters. */
  public static void UTF8toUTF16(BytesRef utf8, CharArr out) {
    // TODO: do in chunks if the input is large
    out.reserve(utf8.length);
    int n = UTF8toUTF16(utf8.bytes, utf8.offset, utf8.length, out.getArray(), out.getEnd());
    out.setEnd(out.getEnd() + n);
  }

  /** Convert UTF8 bytes into a String */
  public static String UTF8toUTF16(BytesRef utf8) {
    char[] out = new char[utf8.length];
    int n = UTF8toUTF16(utf8.bytes, utf8.offset, utf8.length, out, 0);
    return new String(out,0,n);
  }
}
