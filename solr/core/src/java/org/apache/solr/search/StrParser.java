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
package org.apache.solr.search;

import java.util.Locale;

/**
   * Simple class to help with parsing a string.
   * <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
   */
  public class StrParser {
    public String val;
    public int pos;
    public int end;

    public StrParser(String val) {
      this(val, 0, val.length());
    }

    public StrParser(String val, int start, int end) {
      this.val = val;
      this.pos = start;
      this.end = end;
    }

    public void eatws() {
      while (pos < end && Character.isWhitespace(val.charAt(pos))) pos++;
    }

    public char ch() {
      return pos < end ? val.charAt(pos) : 0;
    }

    public void skip(int nChars) {
      pos = Math.max(pos + nChars, end);
    }

    public boolean opt(String s) {
      eatws();
      int slen = s.length();
      if (val.regionMatches(pos, s, 0, slen)) {
        pos += slen;
        return true;
      }
      return false;
    }

    public boolean opt(char ch) {
      eatws();
      if (pos < end && val.charAt(pos) == ch) {
        pos++;
        return true;
      }
      return false;
    }


    public void expect(String s) throws SyntaxError {
      eatws();
      int slen = s.length();
      if (val.regionMatches(pos, s, 0, slen)) {
        pos += slen;
      } else {
        throw new SyntaxError("Expected '" + s + "' at position " + pos + " in '" + val + "'");
      }
    }

    public float getFloat() {
      eatws();
      char[] arr = new char[end - pos];
      int i;
      for (i = 0; i < arr.length; i++) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9')
                || ch == '+' || ch == '-'
                || ch == '.' || ch == 'e' || ch == 'E'
                ) {
          pos++;
          arr[i] = ch;
        } else {
          break;
        }
      }

      return Float.parseFloat(new String(arr, 0, i));
    }

    public Number getNumber() {
      eatws();
      int start = pos;
      boolean flt = false;

      while (pos < end) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9') || ch == '+' || ch == '-') {
          pos++;
        } else if (ch == '.' || ch =='e' || ch=='E') {
          flt = true;
          pos++;
        } else {
          break;
        }
      }

      String v = val.substring(start,pos);
      if (flt) {
        return Double.parseDouble(v);
      } else {
        return Long.parseLong(v);
      }
    }

    public double getDouble() {
      eatws();
      char[] arr = new char[end - pos];
      int i;
      for (i = 0; i < arr.length; i++) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9')
                || ch == '+' || ch == '-'
                || ch == '.' || ch == 'e' || ch == 'E'
                ) {
          pos++;
          arr[i] = ch;
        } else {
          break;
        }
      }

      return Double.parseDouble(new String(arr, 0, i));
    }

    public int getInt() {
      eatws();
      char[] arr = new char[end - pos];
      int i;
      for (i = 0; i < arr.length; i++) {
        char ch = val.charAt(pos);
        if ((ch >= '0' && ch <= '9')
                || ch == '+' || ch == '-'
                ) {
          pos++;
          arr[i] = ch;
        } else {
          break;
        }
      }

      return Integer.parseInt(new String(arr, 0, i));
    }


    public String getId() throws SyntaxError {
      return getId("Expected identifier");
    }

    public String getId(String errMessage) throws SyntaxError {
      eatws();
      int id_start = pos;
      char ch;
      if (pos < end && (ch = val.charAt(pos)) != '$' && Character.isJavaIdentifierStart(ch)) {
        pos++;
        while (pos < end) {
          ch = val.charAt(pos);
//          if (!Character.isJavaIdentifierPart(ch) && ch != '.' && ch != ':') {
          if (!Character.isJavaIdentifierPart(ch) && ch != '.') {
            break;
          }
          pos++;
        }
        return val.substring(id_start, pos);
      }

      if (errMessage != null) {
        throw new SyntaxError(errMessage + " at pos " + pos + " str='" + val + "'");
      }
      return null;
    }

    public String getGlobbedId(String errMessage) throws SyntaxError {
      eatws();
      int id_start = pos;
      char ch;
      if (pos < end && (ch = val.charAt(pos)) != '$' && (Character.isJavaIdentifierStart(ch) || ch=='?' || ch=='*')) {
        pos++;
        while (pos < end) {
          ch = val.charAt(pos);
          if (!(Character.isJavaIdentifierPart(ch) || ch=='?' || ch=='*') && ch != '.') {
            break;
          }
          pos++;
        }
        return val.substring(id_start, pos);
      }

      if (errMessage != null) {
        throw new SyntaxError(errMessage + " at pos " + pos + " str='" + val + "'");
      }
      return null;
    }

    /**
     * Skips leading whitespace and returns whatever sequence of non 
     * whitespace it can find (or hte empty string)
     */
    public String getSimpleString() {
      eatws();
      int startPos = pos;
      char ch;
      while (pos < end) {
        ch = val.charAt(pos);
        if (Character.isWhitespace(ch)) break;
        pos++;
      }
      return val.substring(startPos, pos);
    }

    /**
     * Sort direction or null if current position does not indicate a 
     * sort direction. (True is desc, False is asc).  
     * Position is advanced to after the comma (or end) when result is non null 
     */
    public Boolean getSortDirection() throws SyntaxError {
      final int startPos = pos;
      final String order = getId(null);

      Boolean top = null;

      if (null != order) {
        final String orderLowerCase = order.toLowerCase(Locale.ROOT);
        if ("desc".equals(orderLowerCase) || "top".equals(orderLowerCase)) {
          top = true;
        } else if ("asc".equals(orderLowerCase) || "bottom".equals(orderLowerCase)) {
          top = false;
        }

        // it's not a legal direction if more stuff comes after it
        eatws();
        final char c = ch();
        if (0 == c) {
          // :NOOP
        } else if (',' == c) {
          pos++;
        } else {
          top = null;
        }
      }

      if (null == top) pos = startPos; // no direction, reset
      return top;
    }

    // return null if not a string
    public String getQuotedString() throws SyntaxError {
      eatws();
      char delim = peekChar();
      if (!(delim == '\"' || delim == '\'')) {
        return null;
      }
      int val_start = ++pos;
      StringBuilder sb = new StringBuilder(); // needed for escaping
      for (; ;) {
        if (pos >= end) {
          throw new SyntaxError("Missing end quote for string at pos " + (val_start - 1) + " str='" + val + "'");
        }
        char ch = val.charAt(pos);
        if (ch == '\\') {
          pos++;
          if (pos >= end) break;
          ch = val.charAt(pos);
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
            case 'u':
              if (pos + 4 >= end) {
                throw new SyntaxError("bad unicode escape \\uxxxx at pos" + (val_start - 1) + " str='" + val + "'");
              }
              ch = (char) Integer.parseInt(val.substring(pos + 1, pos + 5), 16);
              pos += 4;
              break;
          }
        } else if (ch == delim) {
          pos++;  // skip over the quote
          break;
        }
        sb.append(ch);
        pos++;
      }

      return sb.toString();
    }

    // next non-whitespace char
    public char peek() {
      eatws();
      return pos < end ? val.charAt(pos) : 0;
    }

    // next char
    public char peekChar() {
      return pos < end ? val.charAt(pos) : 0;
    }

    @Override
    public String toString() {
      return "'" + val + "'" + ", pos=" + pos;
    }

  }
