package org.apache.lucene.analysis.ko.utils;

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


// nocommit: parts of this class seems to borrowed from commons-lang, add correct attribution!
class StringEscapeUtil {
  
  /**
   * <p>
   * Unescapes any Java literals found in the <code>String</code>. For example,
   * it will turn a sequence of <code>'\'</code> and <code>'n'</code> into a
   * newline character, unless the <code>'\'</code> is preceded by another
   * <code>'\'</code>.
   * </p>
   * 
   * @param str
   *          the <code>String</code> to unescape, may be null
   * @return a new unescaped <code>String</code>, <code>null</code> if null
   *         string input
   */
  public static String unescapeJava(String str) {
    StringBuilder out = new StringBuilder(str.length());
    int sz = str.length();
    StringBuilder unicode = new StringBuilder(4);
    boolean hadSlash = false;
    boolean inUnicode = false;
    for (int i = 0; i < sz; i++) {
      char ch = str.charAt(i);
      if (inUnicode) {
        // if in unicode, then we're reading unicode
        // values in somehow
        unicode.append(ch);
        if (unicode.length() == 4) {
          // unicode now contains the four hex digits
          // which represents our unicode character
          int value = Integer.parseInt(unicode.toString(), 16);
          out.append((char) value);
          unicode.setLength(0);
          inUnicode = false;
          hadSlash = false;
        }
        continue;
      }
      if (hadSlash) {
        // handle an escaped value
        hadSlash = false;
        switch (ch) {
          case '\\':
            out.append('\\');
            break;
          case '\'':
            out.append('\'');
            break;
          case '\"':
            out.append('"');
            break;
          case 'r':
            out.append('\r');
            break;
          case 'f':
            out.append('\f');
            break;
          case 't':
            out.append('\t');
            break;
          case 'n':
            out.append('\n');
            break;
          case 'b':
            out.append('\b');
            break;
          case 'u': {
            // uh-oh, we're in unicode country....
            inUnicode = true;
            break;
          }
          default:
            out.append(ch);
            break;
        }
        continue;
      } else if (ch == '\\') {
        hadSlash = true;
        continue;
      }
      out.append(ch);
    }
    if (hadSlash) {
      // then we're in the weird case of a \ at the end of the
      // string, let's output it anyway.
      out.append('\\');
    }
    return out.toString();
  }
  
}
