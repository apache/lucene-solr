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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public class StringEscapeUtil {

  /**
   * <p>Unescapes any Java literals found in the <code>String</code>.
   * For example, it will turn a sequence of <code>'\'</code> and
   * <code>'n'</code> into a newline character, unless the <code>'\'</code>
   * is preceded by another <code>'\'</code>.</p>
   * 
   * @param str  the <code>String</code> to unescape, may be null
   * @return a new unescaped <code>String</code>, <code>null</code> if null string input
   */
  public static String unescapeJava(String str) {
    if (str == null) {
      return null;
    }
    try {
      StringWriter writer = new StringWriter(str.length());
      unescapeJava(writer, str);
      return writer.toString();
    } catch (IOException ioe) {
      // this should never ever happen while writing to a StringWriter
      throw new UnhandledException(ioe);
    }
  }

  /**
   * <p>Unescapes any Java literals found in the <code>String</code> to a
   * <code>Writer</code>.</p>
   *
   * <p>For example, it will turn a sequence of <code>'\'</code> and
   * <code>'n'</code> into a newline character, unless the <code>'\'</code>
   * is preceded by another <code>'\'</code>.</p>
   * 
   * <p>A <code>null</code> string input has no effect.</p>
   * 
   * @param out  the <code>Writer</code> used to output unescaped characters
   * @param str  the <code>String</code> to unescape, may be null
   * @throws IllegalArgumentException if the Writer is <code>null</code>
   * @throws IOException if error occurs on underlying Writer
   */
  public static void unescapeJava(Writer out, String str) throws IOException {
    if (out == null) {
      throw new IllegalArgumentException("The Writer must not be null");
    }
    if (str == null) {
      return;
    }
    int sz = str.length();
    StrBuilder unicode = new StrBuilder(4);
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
          try {
            int value = Integer.parseInt(unicode.toString(), 16);
            out.write((char) value);
            unicode.setLength(0);
            inUnicode = false;
            hadSlash = false;
          } catch (NumberFormatException nfe) {
            throw new UnhandledException("Unable to parse unicode value: " + unicode, nfe);
          }
        }
        continue;
      }
      if (hadSlash) {
        // handle an escaped value
        hadSlash = false;
        switch (ch) {
          case '\\':
            out.write('\\');
            break;
          case '\'':
            out.write('\'');
            break;
          case '\"':
            out.write('"');
            break;
          case 'r':
            out.write('\r');
            break;
          case 'f':
            out.write('\f');
            break;
          case 't':
            out.write('\t');
            break;
          case 'n':
            out.write('\n');
            break;
          case 'b':
            out.write('\b');
            break;
          case 'u': {
            // uh-oh, we're in unicode country....
            inUnicode = true;
            break;
          }
          default :
            out.write(ch);
            break;
        }
        continue;
      } else if (ch == '\\') {
        hadSlash = true;
        continue;
      }
      out.write(ch);
    }
    if (hadSlash) {
      // then we're in the weird case of a \ at the end of the
      // string, let's output it anyway.
      out.write('\\');
    }
  }
}
