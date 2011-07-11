package org.apache.lucene.util;

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


/*
 * Some of this code came from the excellent Unicode
 * conversion examples from:
 *
 *   http://www.unicode.org/Public/PROGRAMS/CVTUTF
 *
 * Full Copyright for that code follows:
*/

/*
 * Copyright 2001-2004 Unicode, Inc.
 * 
 * Disclaimer
 * 
 * This source code is provided as is by Unicode, Inc. No claims are
 * made as to fitness for any particular purpose. No warranties of any
 * kind are expressed or implied. The recipient agrees to determine
 * applicability of information provided. If this file has been
 * purchased on magnetic or optical media from Unicode, Inc., the
 * sole remedy for any claim will be exchange of defective media
 * within 90 days of receipt.
 * 
 * Limitations on Rights to Redistribute This Code
 * 
 * Unicode, Inc. hereby grants the right to freely use the information
 * supplied in this file in the creation of products supporting the
 * Unicode Standard, and to make copies of this file in any form
 * for internal or external distribution as long as this notice
 * remains attached.
 */

/*
 * Additional code came from the IBM ICU library.
 *
 *  http://www.icu-project.org
 *
 * Full Copyright for that code follows.
 */

/*
 * Copyright (C) 1999-2010, International Business Machines
 * Corporation and others.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * provided that the above copyright notice(s) and this permission notice appear
 * in all copies of the Software and that both the above copyright notice(s) and
 * this permission notice appear in supporting documentation.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE
 * LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES, OR
 * ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder shall not
 * be used in advertising or otherwise to promote the sale, use or other
 * dealings in this Software without prior written authorization of the
 * copyright holder.
 */

/**
 * Class to encode java's UTF16 char[] into UTF8 byte[]
 * without always allocating a new byte[] as
 * String.getBytes("UTF-8") does.
 *
 * @lucene.internal
 */

public final class UnicodeUtil {

  private UnicodeUtil() {} // no instance

  public static final int UNI_SUR_HIGH_START = 0xD800;
  public static final int UNI_SUR_HIGH_END = 0xDBFF;
  public static final int UNI_SUR_LOW_START = 0xDC00;
  public static final int UNI_SUR_LOW_END = 0xDFFF;
  public static final int UNI_REPLACEMENT_CHAR = 0xFFFD;

  private static final long UNI_MAX_BMP = 0x0000FFFF;

  private static final int HALF_BASE = 0x0010000;
  private static final long HALF_SHIFT = 10;
  private static final long HALF_MASK = 0x3FFL;

  private static final int SURROGATE_OFFSET = 
    Character.MIN_SUPPLEMENTARY_CODE_POINT - 
    (UNI_SUR_HIGH_START << HALF_SHIFT) - UNI_SUR_LOW_START;

  /**
   * @lucene.internal
   */
  public static final class UTF8Result {
    public byte[] result = new byte[10];
    public int length;

    public void setLength(int newLength) {
      if (result.length < newLength) {
        result = ArrayUtil.grow(result, newLength);
      }
      length = newLength;
    }
  }

  /**
   * @lucene.internal
   */
  public static final class UTF16Result {
    public char[] result = new char[10];
    public int[] offsets = new int[10];
    public int length;

    public void setLength(int newLength) {
      if (result.length < newLength) {
        result = ArrayUtil.grow(result, newLength);
      }
      length = newLength;
    }

    public void copyText(UTF16Result other) {
      setLength(other.length);
      System.arraycopy(other.result, 0, result, 0, length);
    }
  }

  /** Encode characters from a char[] source, starting at
   *  offset for length chars.  Returns a hash of the resulting bytes.  After encoding, result.offset will always be 0. */
  public static int UTF16toUTF8WithHash(final char[] source, final int offset, final int length, BytesRef result) {
    int hash = 0;
    int upto = 0;
    int i = offset;
    final int end = offset + length;
    byte[] out = result.bytes;
    // Pre-allocate for worst case 4-for-1
    final int maxLen = length * 4;
    if (out.length < maxLen)
      out = result.bytes = new byte[ArrayUtil.oversize(maxLen, 1)];
    result.offset = 0;

    while(i < end) {
      
      final int code = (int) source[i++];

      if (code < 0x80) {
        hash = 31*hash + (out[upto++] = (byte) code);
      } else if (code < 0x800) {
        hash = 31*hash + (out[upto++] = (byte) (0xC0 | (code >> 6)));
        hash = 31*hash + (out[upto++] = (byte)(0x80 | (code & 0x3F)));
      } else if (code < 0xD800 || code > 0xDFFF) {
        hash = 31*hash + (out[upto++] = (byte)(0xE0 | (code >> 12)));
        hash = 31*hash + (out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F)));
        hash = 31*hash + (out[upto++] = (byte)(0x80 | (code & 0x3F)));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && i < end) {
          int utf32 = (int) source[i];
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            hash = 31*hash + (out[upto++] = (byte)(0xF0 | (utf32 >> 18)));
            hash = 31*hash + (out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F)));
            hash = 31*hash + (out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F)));
            hash = 31*hash + (out[upto++] = (byte)(0x80 | (utf32 & 0x3F)));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        hash = 31*hash + (out[upto++] = (byte) 0xEF);
        hash = 31*hash + (out[upto++] = (byte) 0xBF);
        hash = 31*hash + (out[upto++] = (byte) 0xBD);
      }
    }
    //assert matches(source, offset, length, out, upto);
    result.length = upto;
    return hash;
  }

  /** Encode characters from a char[] source, starting at
   *  offset and stopping when the character 0xffff is seen.
   *  Returns the number of bytes written to bytesOut. */
  public static void UTF16toUTF8(final char[] source, final int offset, UTF8Result result) {

    int upto = 0;
    int i = offset;
    byte[] out = result.result;

    while(true) {
      
      final int code = (int) source[i++];

      if (upto+4 > out.length) {
        out = result.result = ArrayUtil.grow(out, upto+4);
      }
      if (code < 0x80)
        out[upto++] = (byte) code;
      else if (code < 0x800) {
        out[upto++] = (byte) (0xC0 | (code >> 6));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        if (code == 0xffff)
          // END
          break;
        out[upto++] = (byte)(0xE0 | (code >> 12));
        out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && source[i] != 0xffff) {
          int utf32 = (int) source[i];
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            out[upto++] = (byte)(0xF0 | (utf32 >> 18));
            out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            out[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        out[upto++] = (byte) 0xEF;
        out[upto++] = (byte) 0xBF;
        out[upto++] = (byte) 0xBD;
      }
    }
    //assert matches(source, offset, i-offset-1, out, upto);
    result.length = upto;
  }

  /** Encode characters from a char[] source, starting at
   *  offset for length chars.  Returns the number of bytes
   *  written to bytesOut. */
  public static void UTF16toUTF8(final char[] source, final int offset, final int length, UTF8Result result) {

    int upto = 0;
    int i = offset;
    final int end = offset + length;
    byte[] out = result.result;

    while(i < end) {
      
      final int code = (int) source[i++];

      if (upto+4 > out.length) {
        out = result.result = ArrayUtil.grow(out, upto+4);
      }
      if (code < 0x80)
        out[upto++] = (byte) code;
      else if (code < 0x800) {
        out[upto++] = (byte) (0xC0 | (code >> 6));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        out[upto++] = (byte)(0xE0 | (code >> 12));
        out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && i < end && source[i] != 0xffff) {
          int utf32 = (int) source[i];
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            out[upto++] = (byte)(0xF0 | (utf32 >> 18));
            out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            out[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        out[upto++] = (byte) 0xEF;
        out[upto++] = (byte) 0xBF;
        out[upto++] = (byte) 0xBD;
      }
    }
    //assert matches(source, offset, length, out, upto);
    result.length = upto;
  }

  /** Encode characters from this String, starting at offset
   *  for length characters.  Returns the number of bytes
   *  written to bytesOut. */
  public static void UTF16toUTF8(final String s, final int offset, final int length, UTF8Result result) {
    final int end = offset + length;

    byte[] out = result.result;

    int upto = 0;
    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (upto+4 > out.length) {
        out = result.result = ArrayUtil.grow(out, upto+4);
      }
      if (code < 0x80)
        out[upto++] = (byte) code;
      else if (code < 0x800) {
        out[upto++] = (byte) (0xC0 | (code >> 6));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        out[upto++] = (byte)(0xE0 | (code >> 12));
        out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
            i++;
            out[upto++] = (byte)(0xF0 | (utf32 >> 18));
            out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            out[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        out[upto++] = (byte) 0xEF;
        out[upto++] = (byte) 0xBF;
        out[upto++] = (byte) 0xBD;
      }
    }
    //assert matches(s, offset, length, out, upto);
    result.length = upto;
  }

  /** Encode characters from this String, starting at offset
   *  for length characters. After encoding, result.offset will always be 0.
   */
  public static void UTF16toUTF8(final CharSequence s, final int offset, final int length, BytesRef result) {
    final int end = offset + length;

    byte[] out = result.bytes;
    result.offset = 0;
    // Pre-allocate for worst case 4-for-1
    final int maxLen = length * 4;
    if (out.length < maxLen)
      out = result.bytes = new byte[maxLen];

    int upto = 0;
    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        out[upto++] = (byte) code;
      else if (code < 0x800) {
        out[upto++] = (byte) (0xC0 | (code >> 6));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        out[upto++] = (byte)(0xE0 | (code >> 12));
        out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            out[upto++] = (byte)(0xF0 | (utf32 >> 18));
            out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            out[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        out[upto++] = (byte) 0xEF;
        out[upto++] = (byte) 0xBF;
        out[upto++] = (byte) 0xBD;
      }
    }
    //assert matches(s, offset, length, out, upto);
    result.length = upto;
  }

  /** Encode characters from a char[] source, starting at
   *  offset for length chars. After encoding, result.offset will always be 0.
   */
  public static void UTF16toUTF8(final char[] source, final int offset, final int length, BytesRef result) {

    int upto = 0;
    int i = offset;
    final int end = offset + length;
    byte[] out = result.bytes;
    // Pre-allocate for worst case 4-for-1
    final int maxLen = length * 4;
    if (out.length < maxLen)
      out = result.bytes = new byte[maxLen];
    result.offset = 0;

    while(i < end) {
      
      final int code = (int) source[i++];

      if (code < 0x80)
        out[upto++] = (byte) code;
      else if (code < 0x800) {
        out[upto++] = (byte) (0xC0 | (code >> 6));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        out[upto++] = (byte)(0xE0 | (code >> 12));
        out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && i < end) {
          int utf32 = (int) source[i];
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            out[upto++] = (byte)(0xF0 | (utf32 >> 18));
            out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            out[upto++] = (byte)(0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // replace unpaired surrogate or out-of-order low surrogate
        // with substitution character
        out[upto++] = (byte) 0xEF;
        out[upto++] = (byte) 0xBF;
        out[upto++] = (byte) 0xBD;
      }
    }
    //assert matches(source, offset, length, out, upto);
    result.length = upto;
  }

  /** Convert UTF8 bytes into UTF16 characters.  If offset
   *  is non-zero, conversion starts at that starting point
   *  in utf8, re-using the results from the previous call
   *  up until offset. */
  public static void UTF8toUTF16(final byte[] utf8, final int offset, final int length, final UTF16Result result) {

    final int end = offset + length;
    char[] out = result.result;
    if (result.offsets.length <= end) {
      result.offsets = ArrayUtil.grow(result.offsets, end+1);
    }
    final int[] offsets = result.offsets;

    // If incremental decoding fell in the middle of a
    // single unicode character, rollback to its start:
    int upto = offset;
    while(offsets[upto] == -1)
      upto--;

    int outUpto = offsets[upto];

    // Pre-allocate for worst case 1-for-1
    if (outUpto+length >= out.length) {
      out = result.result = ArrayUtil.grow(out, outUpto+length+1);
    }

    while (upto < end) {

      final int b = utf8[upto]&0xff;
      final int ch;

      offsets[upto++] = outUpto;

      if (b < 0xc0) {
        assert b < 0x80;
        ch = b;
      } else if (b < 0xe0) {
        ch = ((b&0x1f)<<6) + (utf8[upto]&0x3f);
        offsets[upto++] = -1;
      } else if (b < 0xf0) {
        ch = ((b&0xf)<<12) + ((utf8[upto]&0x3f)<<6) + (utf8[upto+1]&0x3f);
        offsets[upto++] = -1;
        offsets[upto++] = -1;
      } else {
        assert b < 0xf8;
        ch = ((b&0x7)<<18) + ((utf8[upto]&0x3f)<<12) + ((utf8[upto+1]&0x3f)<<6) + (utf8[upto+2]&0x3f);
        offsets[upto++] = -1;
        offsets[upto++] = -1;
        offsets[upto++] = -1;
      }

      if (ch <= UNI_MAX_BMP) {
        // target is a character <= 0xFFFF
        out[outUpto++] = (char) ch;
      } else {
        // target is a character in range 0xFFFF - 0x10FFFF
        final int chHalf = ch - HALF_BASE;
        out[outUpto++] = (char) ((chHalf >> HALF_SHIFT) + UNI_SUR_HIGH_START);
        out[outUpto++] = (char) ((chHalf & HALF_MASK) + UNI_SUR_LOW_START);
      }
    }

    offsets[upto] = outUpto;
    result.length = outUpto;
  }

  // Only called from assert
  /*
  private static boolean matches(char[] source, int offset, int length, byte[] result, int upto) {
    try {
      String s1 = new String(source, offset, length);
      String s2 = new String(result, 0, upto, "UTF-8");
      if (!s1.equals(s2)) {
        //System.out.println("DIFF: s1 len=" + s1.length());
        //for(int i=0;i<s1.length();i++)
        //  System.out.println("    " + i + ": " + (int) s1.charAt(i));
        //System.out.println("s2 len=" + s2.length());
        //for(int i=0;i<s2.length();i++)
        //  System.out.println("    " + i + ": " + (int) s2.charAt(i));

        // If the input string was invalid, then the
        // difference is OK
        if (!validUTF16String(s1))
          return true;

        return false;
      }
      return s1.equals(s2);
    } catch (UnsupportedEncodingException uee) {
      return false;
    }
  }

  // Only called from assert
  private static boolean matches(String source, int offset, int length, byte[] result, int upto) {
    try {
      String s1 = source.substring(offset, offset+length);
      String s2 = new String(result, 0, upto, "UTF-8");
      if (!s1.equals(s2)) {
        // Allow a difference if s1 is not valid UTF-16

        //System.out.println("DIFF: s1 len=" + s1.length());
        //for(int i=0;i<s1.length();i++)
        //  System.out.println("    " + i + ": " + (int) s1.charAt(i));
        //System.out.println("  s2 len=" + s2.length());
        //for(int i=0;i<s2.length();i++)
        //  System.out.println("    " + i + ": " + (int) s2.charAt(i));

        // If the input string was invalid, then the
        // difference is OK
        if (!validUTF16String(s1))
          return true;

        return false;
      }
      return s1.equals(s2);
    } catch (UnsupportedEncodingException uee) {
      return false;
    }
  }

  public static final boolean validUTF16String(String s) {
    final int size = s.length();
    for(int i=0;i<size;i++) {
      char ch = s.charAt(i);
      if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) {
        if (i < size-1) {
          i++;
          char nextCH = s.charAt(i);
          if (nextCH >= UNI_SUR_LOW_START && nextCH <= UNI_SUR_LOW_END) {
            // Valid surrogate pair
          } else
            // Unmatched high surrogate
            return false;
        } else
          // Unmatched high surrogate
          return false;
      } else if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END)
        // Unmatched low surrogate
        return false;
    }

    return true;
  }

  public static final boolean validUTF16String(char[] s, int size) {
    for(int i=0;i<size;i++) {
      char ch = s[i];
      if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) {
        if (i < size-1) {
          i++;
          char nextCH = s[i];
          if (nextCH >= UNI_SUR_LOW_START && nextCH <= UNI_SUR_LOW_END) {
            // Valid surrogate pair
          } else
            return false;
        } else
          return false;
      } else if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END)
        // Unmatched low surrogate
        return false;
    }

    return true;
  }
  */

  /** Shift value for lead surrogate to form a supplementary character. */
  private static final int LEAD_SURROGATE_SHIFT_ = 10;
  /** Mask to retrieve the significant value from a trail surrogate.*/
  private static final int TRAIL_SURROGATE_MASK_ = 0x3FF;
  /** Trail surrogate minimum value */
  private static final int TRAIL_SURROGATE_MIN_VALUE = 0xDC00;
  /** Lead surrogate minimum value */
  private static final int LEAD_SURROGATE_MIN_VALUE = 0xD800;
  /** The minimum value for Supplementary code points */
  private static final int SUPPLEMENTARY_MIN_VALUE = 0x10000;
  /** Value that all lead surrogate starts with */
  private static final int LEAD_SURROGATE_OFFSET_ = LEAD_SURROGATE_MIN_VALUE
          - (SUPPLEMENTARY_MIN_VALUE >> LEAD_SURROGATE_SHIFT_);

  /**
   * Cover JDK 1.5 API. Create a String from an array of codePoints.
   *
   * @param codePoints The code array
   * @param offset The start of the text in the code point array
   * @param count The number of code points
   * @return a String representing the code points between offset and count
   * @throws IllegalArgumentException If an invalid code point is encountered
   * @throws IndexOutOfBoundsException If the offset or count are out of bounds.
   */
  public static String newString(int[] codePoints, int offset, int count) {
      if (count < 0) {
          throw new IllegalArgumentException();
      }
      char[] chars = new char[count];
      int w = 0;
      for (int r = offset, e = offset + count; r < e; ++r) {
          int cp = codePoints[r];
          if (cp < 0 || cp > 0x10ffff) {
              throw new IllegalArgumentException();
          }
          while (true) {
              try {
                  if (cp < 0x010000) {
                      chars[w] = (char) cp;
                      w++;
                  } else {
                      chars[w] = (char) (LEAD_SURROGATE_OFFSET_ + (cp >> LEAD_SURROGATE_SHIFT_));
                      chars[w + 1] = (char) (TRAIL_SURROGATE_MIN_VALUE + (cp & TRAIL_SURROGATE_MASK_));
                      w += 2;
                  }
                  break;
              } catch (IndexOutOfBoundsException ex) {
                  int newlen = (int) (Math.ceil((double) codePoints.length * (w + 2)
                          / (r - offset + 1)));
                  char[] temp = new char[newlen];
                  System.arraycopy(chars, 0, temp, 0, w);
                  chars = temp;
              }
          }
      }
      return new String(chars, 0, w);
  }
  
  /**
   * Interprets the given byte array as UTF-8 and converts to UTF-16. The {@link CharsRef} will be extended if 
   * it doesn't provide enough space to hold the worst case of each byte becoming a UTF-16 codepoint.
   * <p>
   * NOTE: Full characters are read, even if this reads past the length passed (and
   * can result in an ArrayOutOfBoundsException if invalid UTF-8 is passed).
   * Explicit checks for valid UTF-8 are not performed. 
   */
  public static void UTF8toUTF16(byte[] utf8, int offset, int length, CharsRef chars) {
    int out_offset = chars.offset = 0;
    final char[] out = chars.chars =  ArrayUtil.grow(chars.chars, length);
    final int limit = offset + length;
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
        if (ch < UNI_MAX_BMP) {
          out[out_offset++] = (char)ch;
        } else {
          int chHalf = ch - 0x0010000;
          out[out_offset++] = (char) ((chHalf >> 10) + 0xD800);
          out[out_offset++] = (char) ((chHalf & HALF_MASK) + 0xDC00);          
        }
      }
    }
    chars.length = out_offset - chars.offset;
  }

  /**
   * Utility method for {@link #UTF8toUTF16(byte[], int, int, CharsRef)}
   * @see #UTF8toUTF16(byte[], int, int, CharsRef)
   */
  public static void UTF8toUTF16(BytesRef bytesRef, CharsRef chars) {
    UTF8toUTF16(bytesRef.bytes, bytesRef.offset, bytesRef.length, chars);
  }
}
