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

import org.noggit.CharArr;


public class ByteUtils {

  /** Maximum number of UTF8 bytes per UTF16 character. */
  public static final int MAX_UTF8_BYTES_PER_CHAR = 3;

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
  public static void UTF8toUTF16(byte[] utf8, int offset, int len, CharArr out) {
    // TODO: do in chunks if the input is large
    out.reserve(len);
    int n = UTF8toUTF16(utf8, offset, len, out.getArray(), out.getEnd());
    out.setEnd(out.getEnd() + n);
  }

  /** Convert UTF8 bytes into a String */
  public static String UTF8toUTF16(byte[] utf8, int offset, int len) {
    char[] out = new char[len];
    int n = UTF8toUTF16(utf8, offset, len, out, 0);
    return new String(out,0,n);
  }



  /** Writes UTF8 into the byte array, starting at offset.  The caller should ensure that
   * there is enough space for the worst-case scenario.
   * @return the number of bytes written
   */
  public static int UTF16toUTF8(CharSequence s, int offset, int len, byte[] result, int resultOffset) {
    final int end = offset + len;

    int upto = resultOffset;
    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        result[upto++] = (byte) code;
      else if (code < 0x800) {
        result[upto++] = (byte) (0xC0 | (code >> 6));
        result[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        result[upto++] = (byte)(0xE0 | (code >> 12));
        result[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        result[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            result[upto++] = (byte)(0xF0 | (utf32 >> 18));
            result[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            result[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            result[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        result[upto++] = (byte) 0xEF;
        result[upto++] = (byte) 0xBF;
        result[upto++] = (byte) 0xBD;
      }
    }

    return upto - resultOffset;
  }

  /** Writes UTF8 into the given OutputStream by first writing to the given scratch array
   * and then writing the contents of the scratch array to the OutputStream. The given scratch byte array
   * is used to buffer intermediate data before it is written to the output stream.
   *
   * @return the number of bytes written
   */
  public static int writeUTF16toUTF8(CharSequence s, int offset, int len, OutputStream fos, byte[] scratch) throws IOException {
    final int end = offset + len;

    int upto = 0, totalBytes = 0;
    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (upto > scratch.length - 4)  {
        // a code point may take upto 4 bytes and we don't have enough space, so reset
        totalBytes += upto;
        if(fos == null) throw new IOException("buffer over flow");
        fos.write(scratch, 0, upto);
        upto = 0;
      }

      if (code < 0x80)
        scratch[upto++] = (byte) code;
      else if (code < 0x800) {
        scratch[upto++] = (byte) (0xC0 | (code >> 6));
        scratch[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        scratch[upto++] = (byte)(0xE0 | (code >> 12));
        scratch[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        scratch[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            scratch[upto++] = (byte)(0xF0 | (utf32 >> 18));
            scratch[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            scratch[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            scratch[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        scratch[upto++] = (byte) 0xEF;
        scratch[upto++] = (byte) 0xBF;
        scratch[upto++] = (byte) 0xBD;
      }
    }

    totalBytes += upto;
    if(fos != null) fos.write(scratch, 0, upto);

    return totalBytes;
  }

  /**
   * Calculates the number of UTF8 bytes necessary to write a UTF16 string.
   *
   * @return the number of bytes written
   */
  public static int calcUTF16toUTF8Length(CharSequence s, int offset, int len) {
    final int end = offset + len;

    int res = 0;
    for (int i = offset; i < end; i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        res++;
      else if (code < 0x800) {
        res += 2;
      } else if (code < 0xD800 || code > 0xDFFF) {
        res += 3;
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end - 1)) {
          int utf32 = (int) s.charAt(i + 1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            i++;
            res += 4;
            continue;
          }
        }
        res += 3;
      }
    }

    return res;
  }

}
