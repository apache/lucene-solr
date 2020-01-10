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
package org.noggit;


public class JSONUtil {
  public static final char[] TRUE_CHARS = new char[]{'t', 'r', 'u', 'e'};
  public static final char[] FALSE_CHARS = new char[]{'f', 'a', 'l', 's', 'e'};
  public static final char[] NULL_CHARS = new char[]{'n', 'u', 'l', 'l'};
  public static final char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  public static final char VALUE_SEPARATOR = ',';
  public static final char NAME_SEPARATOR = ':';
  public static final char OBJECT_START = '{';
  public static final char OBJECT_END = '}';
  public static final char ARRAY_START = '[';
  public static final char ARRAY_END = ']';

  public static String toJSON(Object o) {
    CharArr out = new CharArr();
    new JSONWriter(out).write(o);
    return out.toString();
  }

  /**
   * @param o          The object to convert to JSON
   * @param indentSize The number of space characters to use as an indent (default 2). 0=newlines but no spaces, -1=no indent at all.
   */
  public static String toJSON(Object o, int indentSize) {
    CharArr out = new CharArr();
    new JSONWriter(out, indentSize).write(o);
    return out.toString();
  }

  public static void writeNumber(int number, CharArr out) {
    out.write(Integer.toString(number));
  }

  public static void writeNumber(long number, CharArr out) {
    out.write(Long.toString(number));
  }

  public static void writeNumber(float number, CharArr out) {
    out.write(Float.toString(number));
  }

  public static void writeNumber(double number, CharArr out) {
    out.write(Double.toString(number));
  }

  public static void writeString(CharArr val, CharArr out) {
    writeString(val.getArray(), val.getStart(), val.getEnd(), out);
  }

  public static void writeString(char[] val, int start, int end, CharArr out) {
    out.write('"');
    writeStringPart(val, start, end, out);
    out.write('"');
  }

  public static void writeString(String val, int start, int end, CharArr out) {
    out.write('"');
    writeStringPart(val, start, end, out);
    out.write('"');
  }

  public static void writeString(CharSequence val, int start, int end, CharArr out) {
    out.write('"');
    writeStringPart(val, start, end, out);
    out.write('"');
  }

  public static void writeStringPart(char[] val, int start, int end, CharArr out) {
    for (int i = start; i < end; i++) {
      char ch = val[i];
      // When ch>=1f, (ch*146087937)&0xd6a01f80) is 0 only for characters that need escaping: " \\ u2028 u2029
      // and has 7 false positives: 204a 4051 802f c022 c044 e04a e04b
      if (ch > 0x1f && ((ch * 146087937) & 0xd6a01f80) != 0) {
        out.write(ch);
      } else {
        writeChar(ch, out);
      }
    }
  }

  public static void writeChar(char ch, CharArr out) {
    switch (ch) {
      case '"':
      case '\\':
        out.write('\\');
        out.write(ch);
        break;
      case '\r':
        out.write('\\');
        out.write('r');
        break;
      case '\n':
        out.write('\\');
        out.write('n');
        break;
      case '\t':
        out.write('\\');
        out.write('t');
        break;
      case '\b':
        out.write('\\');
        out.write('b');
        break;
      case '\f':
        out.write('\\');
        out.write('f');
        break;
      // case '/':
      case '\u2028':  // valid JSON, but not valid json script
      case '\u2029':
        unicodeEscape(ch, out);
        break;
      default:
        if (ch <= 0x1F) {
          unicodeEscape(ch, out);
        } else {
          out.write(ch);
        }
    }
  }


  public static void writeStringPart(String chars, int start, int end, CharArr out) {
    // TODO: write in chunks?

    int toWrite = end - start;
    char[] arr = out.getArray();
    int pos = out.getEnd();
    int space = arr.length - pos;
    if (space < toWrite) {
      writeStringPart((CharSequence) chars, start, end, out);
      return;
    }

    // get chars directly from String into output array
    chars.getChars(start, end, arr, pos);

    int endInOut = pos + toWrite;
    out.setEnd(endInOut);
    for (int i = pos; i < endInOut; i++) {
      char ch = arr[i];

      // When ch>=1f, (ch*146087937)&0xd6a01f80) is 0 only for characters that need escaping: " \\ u2028 u2029
      // and has 7 false positives: 204a 4051 802f c022 c044 e04a e04b
      if (ch <= 0x1f || ((ch * 146087937) & 0xd6a01f80) == 0) {
        // We hit a char that needs escaping. do the rest char by char.
        out.setEnd(i);
        writeStringPart((CharSequence) chars, start + (i - pos), end, out);
        return;
      }
    }
  }

  public static void writeStringPart(CharSequence chars, int start, int end, CharArr out) {
    for (int i = start; i < end; i++) {
      char ch = chars.charAt(i);
      // When ch>=1f, (ch*146087937)&0xd6a01f80) is 0 only for characters that need escaping: " \\ u2028 u2029
      // and has 7 false positives: 204a 4051 802f c022 c044 e04a e04b
      if (ch > 0x1f && ((ch * 146087937) & 0xd6a01f80) != 0) {
        out.write(ch);
      } else {
        writeChar(ch, out);
      }
    }
  }


  public static void unicodeEscape(int ch, CharArr out) {
    out.write('\\');
    out.write('u');
    out.write(HEX_CHARS[ch >>> 12]);
    out.write(HEX_CHARS[(ch >>> 8) & 0xf]);
    out.write(HEX_CHARS[(ch >>> 4) & 0xf]);
    out.write(HEX_CHARS[ch & 0xf]);
  }

  public static void writeNull(CharArr out) {
    out.write(NULL_CHARS);
  }

  public static void writeBoolean(boolean val, CharArr out) {
    out.write(val ? TRUE_CHARS : FALSE_CHARS);
  }

}