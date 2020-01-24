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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.solr.common.SolrException;

/**
 *
 */
public class StrUtils {
  public static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6',
      '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  public static List<String> splitSmart(String s, char separator) {
    ArrayList<String> lst = new ArrayList<>(4);
    splitSmart(s, separator, lst);
    return lst;

  }

  static final String DELIM_CHARS = "/:;.,%#";

  public static List<String> split(String s, char sep) {
    if (DELIM_CHARS.indexOf(s.charAt(0)) > -1) {
      sep = s.charAt(0);
    }
    return splitSmart(s, sep, true);

  }

  public static List<String> splitSmart(String s, char separator, boolean trimEmpty) {
    List<String> l = splitSmart(s, separator);
    if (trimEmpty) {
      if (l.size() > 0 && l.get(0).isEmpty()) l.remove(0);
    }
    return l;
  }

  /**
   * Split a string based on a separator, but don't split if it's inside
   * a string.  Assume '\' escapes the next char both inside and
   * outside strings.
   */
  public static void splitSmart(String s, char separator, List<String> lst) {
    int pos = 0, start = 0, end = s.length();
    char inString = 0;
    char ch = 0;
    while (pos < end) {
      char prevChar = ch;
      ch = s.charAt(pos++);
      if (ch == '\\') {    // skip escaped chars
        pos++;
      } else if (inString != 0 && ch == inString) {
        inString = 0;
      } else if (ch == '\'' || ch == '"') {
        // If char is directly preceeded by a number or letter
        // then don't treat it as the start of a string.
        // Examples: 50" TV, or can't
        if (!Character.isLetterOrDigit(prevChar)) {
          inString = ch;
        }
      } else if (ch == separator && inString == 0) {
        lst.add(s.substring(start, pos - 1));
        start = pos;
      }
    }
    if (start < end) {
      lst.add(s.substring(start, end));
    }

    /***
     if (SolrCore.log.isLoggable(Level.FINEST)) {
     SolrCore.log.trace("splitCommand=" + lst);
     }
     ***/

  }

  /**
   * Splits a backslash escaped string on the separator.
   * <p>
   * Current backslash escaping supported:
   * <br> \n \t \r \b \f are escaped the same as a Java String
   * <br> Other characters following a backslash are produced verbatim (\c =&gt; c)
   *
   * @param s         the string to split
   * @param separator the separator to split on
   * @param decode    decode backslash escaping
   * @return not null
   */
  public static List<String> splitSmart(String s, String separator, boolean decode) {
    ArrayList<String> lst = new ArrayList<>(2);
    StringBuilder sb = new StringBuilder();
    int pos = 0, end = s.length();
    while (pos < end) {
      if (s.startsWith(separator, pos)) {
        if (sb.length() > 0) {
          lst.add(sb.toString());
          sb = new StringBuilder();
        }
        pos += separator.length();
        continue;
      }

      char ch = s.charAt(pos++);
      if (ch == '\\') {
        if (!decode) sb.append(ch);
        if (pos >= end) break;  // ERROR, or let it go?
        ch = s.charAt(pos++);
        if (decode) {
          switch (ch) {
            case 'n':
              ch = '\n';
              break;
            case 't':
              ch = '\t';
              break;
            case 'r':
              ch = '\r';
              break;
            case 'b':
              ch = '\b';
              break;
            case 'f':
              ch = '\f';
              break;
          }
        }
      }

      sb.append(ch);
    }

    if (sb.length() > 0) {
      lst.add(sb.toString());
    }

    return lst;
  }

  /**
   * Splits file names separated by comma character.
   * File names can contain comma characters escaped by backslash '\'
   *
   * @param fileNames the string containing file names
   * @return a list of file names with the escaping backslashed removed
   */
  public static List<String> splitFileNames(String fileNames) {
    if (fileNames == null)
      return Collections.<String>emptyList();

    List<String> result = new ArrayList<>();
    for (String file : fileNames.split("(?<!\\\\),")) {
      result.add(file.replaceAll("\\\\(?=,)", ""));
    }

    return result;
  }

  /**
   * Creates a backslash escaped string, joining all the items.
   *
   * @see #escapeTextWithSeparator
   */
  public static String join(Collection<?> items, char separator) {
    if (items == null) return "";
    StringBuilder sb = new StringBuilder(items.size() << 3);
    boolean first = true;
    for (Object o : items) {
      String item = String.valueOf(o);
      if (first) {
        first = false;
      } else {
        sb.append(separator);
      }
      appendEscapedTextToBuilder(sb, item, separator);
    }
    return sb.toString();
  }


  public static List<String> splitWS(String s, boolean decode) {
    ArrayList<String> lst = new ArrayList<>(2);
    StringBuilder sb = new StringBuilder();
    int pos = 0, end = s.length();
    while (pos < end) {
      char ch = s.charAt(pos++);
      if (Character.isWhitespace(ch)) {
        if (sb.length() > 0) {
          lst.add(sb.toString());
          sb = new StringBuilder();
        }
        continue;
      }

      if (ch == '\\') {
        if (!decode) sb.append(ch);
        if (pos >= end) break;  // ERROR, or let it go?
        ch = s.charAt(pos++);
        if (decode) {
          switch (ch) {
            case 'n':
              ch = '\n';
              break;
            case 't':
              ch = '\t';
              break;
            case 'r':
              ch = '\r';
              break;
            case 'b':
              ch = '\b';
              break;
            case 'f':
              ch = '\f';
              break;
          }
        }
      }

      sb.append(ch);
    }

    if (sb.length() > 0) {
      lst.add(sb.toString());
    }

    return lst;
  }

  public static List<String> toLower(List<String> strings) {
    ArrayList<String> ret = new ArrayList<>(strings.size());
    for (String str : strings) {
      ret.add(str.toLowerCase(Locale.ROOT));
    }
    return ret;
  }


  /**
   * Return if a string starts with '1', 't', or 'T'
   * and return false otherwise.
   */
  public static boolean parseBoolean(String s) {
    char ch = s.length() > 0 ? s.charAt(0) : 0;
    return (ch == '1' || ch == 't' || ch == 'T');
  }

  /**
   * how to transform a String into a boolean... more flexible than
   * Boolean.parseBoolean() to enable easier integration with html forms.
   */
  public static boolean parseBool(String s) {
    if (s != null) {
      if (s.startsWith("true") || s.startsWith("on") || s.startsWith("yes")) {
        return true;
      }
      if (s.startsWith("false") || s.startsWith("off") || s.equals("no")) {
        return false;
      }
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid boolean value: " + s);
  }

  /**
   * {@link NullPointerException} and {@link SolrException} free version of {@link #parseBool(String)}
   *
   * @return parsed boolean value (or def, if s is null or invalid)
   */
  public static boolean parseBool(String s, boolean def) {
    if (s != null) {
      if (s.startsWith("true") || s.startsWith("on") || s.startsWith("yes")) {
        return true;
      }
      if (s.startsWith("false") || s.startsWith("off") || s.equals("no")) {
        return false;
      }
    }
    return def;
  }

  /**
   * URLEncodes a value, replacing only enough chars so that
   * the URL may be unambiguously pasted back into a browser.
   * <p>
   * Characters with a numeric value less than 32 are encoded.
   * &amp;,=,%,+,space are encoded.
   */
  public static void partialURLEncodeVal(Appendable dest, String val) throws IOException {
    for (int i = 0; i < val.length(); i++) {
      char ch = val.charAt(i);
      if (ch < 32) {
        dest.append('%');
        if (ch < 0x10) dest.append('0');
        dest.append(Integer.toHexString(ch));
      } else {
        switch (ch) {
          case ' ':
            dest.append('+');
            break;
          case '&':
            dest.append("%26");
            break;
          case '%':
            dest.append("%25");
            break;
          case '=':
            dest.append("%3D");
            break;
          case '+':
            dest.append("%2B");
            break;
          default:
            dest.append(ch);
            break;
        }
      }
    }
  }

  /**
   * Creates a new copy of the string with the separator backslash escaped.
   *
   * @see #join
   */
  public static String escapeTextWithSeparator(String item, char separator) {
    StringBuilder sb = new StringBuilder(item.length() * 2);
    appendEscapedTextToBuilder(sb, item, separator);
    return sb.toString();
  }

  /**
   * writes chars from item to out, backslash escaping as needed based on separator --
   * but does not append the separator itself
   */
  public static void appendEscapedTextToBuilder(StringBuilder out,
                                                String item,
                                                char separator) {
    for (int i = 0; i < item.length(); i++) {
      char ch = item.charAt(i);
      if (ch == '\\' || ch == separator) {
        out.append('\\');
      }
      out.append(ch);
    }
  }

  /**
   * Format using {@link MessageFormat} but with the ROOT locale
   */
  public static String formatString(String pattern, Object... args) {
    return new MessageFormat(pattern, Locale.ROOT).format(args);
  }
}
