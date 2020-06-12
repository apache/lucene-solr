/*
 *  Copyright 2006- Yonik Seeley
 *
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

import java.io.IOException;
import java.io.Reader;


public class JSONParser {

  /**
   * Event indicating a JSON string value, including member names of objects
   */
  public static final int STRING = 1;
  /**
   * Event indicating a JSON number value which fits into a signed 64 bit integer
   */
  public static final int LONG = 2;
  /**
   * Event indicating a JSON number value which has a fractional part or an exponent
   * and with string length &lt;= 23 chars not including sign.  This covers
   * all representations of normal values for Double.toString().
   */
  public static final int NUMBER = 3;
  /**
   * Event indicating a JSON number value that was not produced by toString of any
   * Java primitive numerics such as Double or Long.  It is either
   * an integer outside the range of a 64 bit signed integer, or a floating
   * point value with a string representation of more than 23 chars.
   */
  public static final int BIGNUMBER = 4;
  /**
   * Event indicating a JSON boolean
   */
  public static final int BOOLEAN = 5;
  /**
   * Event indicating a JSON null
   */
  public static final int NULL = 6;
  /**
   * Event indicating the start of a JSON object
   */
  public static final int OBJECT_START = 7;
  /**
   * Event indicating the end of a JSON object
   */
  public static final int OBJECT_END = 8;
  /**
   * Event indicating the start of a JSON array
   */
  public static final int ARRAY_START = 9;
  /**
   * Event indicating the end of a JSON array
   */
  public static final int ARRAY_END = 10;
  /**
   * Event indicating the end of input has been reached
   */
  public static final int EOF = 11;


  /**
   * Flags to control parsing behavior
   */
  public static final int ALLOW_COMMENTS = 1 << 0;
  public static final int ALLOW_SINGLE_QUOTES = 1 << 1;
  public static final int ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER = 1 << 2;
  public static final int ALLOW_UNQUOTED_KEYS = 1 << 3;
  public static final int ALLOW_UNQUOTED_STRING_VALUES = 1 << 4;
  /**
   * ALLOW_EXTRA_COMMAS causes any number of extra commas in arrays and objects to be ignored
   * Note that a trailing comma in [] would be [,] (hence calling the feature "trailing" commas
   * is either limiting or misleading.  Since trailing commas is fundamentally incompatible with any future
   * "fill-in-missing-values-with-null", it was decided to extend this feature to handle any
   * number of extra commas.
   */
  public static final int ALLOW_EXTRA_COMMAS = 1 << 5;
  public static final int ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT = 1 << 6;
  public static final int OPTIONAL_OUTER_BRACES = 1 << 7;

  public static final int FLAGS_STRICT = 0;
  public static final int FLAGS_DEFAULT = ALLOW_COMMENTS | ALLOW_SINGLE_QUOTES | ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER | ALLOW_UNQUOTED_KEYS | ALLOW_UNQUOTED_STRING_VALUES | ALLOW_EXTRA_COMMAS;

  public static class ParseException extends RuntimeException {
    public ParseException(String msg) {
      super(msg);
    }
  }

  public static String getEventString(int e) {
    switch (e) {
      case STRING:
        return "STRING";
      case LONG:
        return "LONG";
      case NUMBER:
        return "NUMBER";
      case BIGNUMBER:
        return "BIGNUMBER";
      case BOOLEAN:
        return "BOOLEAN";
      case NULL:
        return "NULL";
      case OBJECT_START:
        return "OBJECT_START";
      case OBJECT_END:
        return "OBJECT_END";
      case ARRAY_START:
        return "ARRAY_START";
      case ARRAY_END:
        return "ARRAY_END";
      case EOF:
        return "EOF";
    }
    return "Unknown: " + e;
  }

  private static final CharArr devNull = new CharArr.NullCharArr();

  protected int flags = FLAGS_DEFAULT;

  protected final char[] buf;  // input buffer with JSON text in it
  protected int start;         // current position in the buffer
  protected int end;           // end position in the buffer (one past last valid index)
  protected final Reader in;   // optional reader to obtain data from
  protected boolean eof = false; // true if the end of the stream was reached.
  protected long gpos;          // global position = gpos + start

  protected int event;         // last event read

  protected int stringTerm;    // The terminator for the last string we read: single quote, double quote, or 0 for unterminated.

  protected boolean missingOpeningBrace = false;

  public JSONParser(Reader in) {
    this(in, new char[8192]);
    // 8192 matches the default buffer size of a BufferedReader so double
    // buffering of the data is avoided.
  }

  public JSONParser(Reader in, char[] buffer) {
    this.in = in;
    this.buf = buffer;
  }

  // idea - if someone passes us a CharArrayReader, we could
  // directly use that buffer as it's protected.

  public JSONParser(char[] data, int start, int end) {
    this.in = null;
    this.buf = data;
    this.start = start;
    this.end = end;
  }

  public JSONParser(String data) {
    this(data, 0, data.length());
  }

  public JSONParser(String data, int start, int end) {
    this.in = null;
    this.start = start;
    this.end = end;
    this.buf = new char[end - start];
    data.getChars(start, end, buf, 0);
  }

  public int getFlags() {
    return flags;
  }

  public int setFlags(int flags) {
    int oldFlags = flags;
    this.flags = flags;
    return oldFlags;
  }

  // temporary output buffer
  private final CharArr out = new CharArr(64);

  // We need to keep some state in order to (at a minimum) know if
  // we should skip ',' or ':'.
  private byte[] stack = new byte[16];
  private int ptr = 0;     // pointer into the stack of parser states
  private byte state = 0;  // current parser state

  // parser states stored in the stack
  private static final byte DID_OBJSTART = 1;  // '{' just read
  private static final byte DID_ARRSTART = 2;  // '[' just read
  private static final byte DID_ARRELEM = 3;   // array element just read
  private static final byte DID_MEMNAME = 4;   // object member name (map key) just read
  private static final byte DID_MEMVAL = 5;    // object member value (map val) just read

  // info about value that was just read (or is in the middle of being read)
  private int valstate;

  // push current parser state (use at start of new container)
  private final void push() {
    if (ptr >= stack.length) {
      // doubling here is probably overkill, but anything that needs to double more than
      // once (32 levels deep) is very atypical anyway.
      byte[] newstack = new byte[stack.length << 1];
      System.arraycopy(stack, 0, newstack, 0, stack.length);
      stack = newstack;
    }
    stack[ptr++] = state;
  }

  // pop  parser state (use at end of container)
  private final void pop() {
    if (--ptr < 0) {
      throw err("Unbalanced container");
    } else {
      state = stack[ptr];
    }
  }

  protected void fill() throws IOException {
    if (in != null) {
      gpos += end;
      start = 0;
      int num = in.read(buf, 0, buf.length);
      end = num >= 0 ? num : 0;
    }
    if (start >= end) eof = true;
  }

  private void getMore() throws IOException {
    fill();
    if (start >= end) {
      throw err(null);
    }
  }

  protected int getChar() throws IOException {
    if (start >= end) {
      fill();
      if (start >= end) return -1;
    }
    return buf[start++];
  }

  /**
   * Returns true if the given character is considered to be whitespace.
   * One difference between Java's Character.isWhitespace() is that this method
   * considers a hard space (non-breaking space, or nbsp) to be whitespace.
   */
  protected static final boolean isWhitespace(int ch) {
    return (Character.isWhitespace(ch) || ch == 0x00a0);
  }

  private static final long WS_MASK = (1L << ' ') | (1L << '\t') | (1L << '\r') | (1L << '\n') | (1L << '#') | (1L << '/') | (0x01); // set 1 bit so 0xA0 will be flagged as whitespace

  protected int getCharNWS() throws IOException {
    for (; ; ) {
      int ch = getChar();
      // getCharNWS is normally called in the context of expecting certain JSON special characters
      // such as ":}"],"
      // all of these characters are below 64 (including comment chars '/' and '#', so we can make this the fast path
      // even w/o checking the range first.  We'll only get some false-positives while using bare strings (chars "IJMc")
      if (((WS_MASK >> ch) & 0x01) == 0) {
        return ch;
      } else if (ch <= ' ') {   // this will only be true if one of the whitespace bits was set
        continue;
      } else if (ch == '/') {
        getSlashComment();
      } else if (ch == '#') {
        getNewlineComment();
      } else if (!isWhitespace(ch)) { // we'll only reach here with certain bare strings, errors, or strange whitespace like 0xa0
        return ch;
      }

      /***
       // getCharNWS is normally called in the context of expecting certain JSON special characters
       // such as ":}"],"
       // all of these characters are below 64 (including comment chars '/' and '#', so we can make this the fast path
       if (ch < 64) {
       if (((WS_MASK >> ch) & 0x01) == 0) return ch;
       if (ch <= ' ') continue;  // whitespace below a normal space
       if (ch=='/') {
       getSlashComment();
       } else if (ch=='#') {
       getNewlineComment();
       }
       } else if (!isWhitespace(ch)) {  // check for higher whitespace like 0xA0
       return ch;
       }
       ***/

      /** older code
       switch (ch) {
       case ' ' :
       case '\t' :
       case '\r' :
       case '\n' :
       continue outer;
       case '#' :
       getNewlineComment();
       continue outer;
       case '/' :
       getSlashComment();
       continue outer;
       default:
       return ch;
       }
       **/
    }
  }

  protected int getCharNWS(int ch) throws IOException {
    for (; ; ) {
      // getCharNWS is normally called in the context of expecting certain JSON special characters
      // such as ":}"],"
      // all of these characters are below 64 (including comment chars '/' and '#', so we can make this the fast path
      // even w/o checking the range first.  We'll only get some false-positives while using bare strings (chars "IJMc")
      if (((WS_MASK >> ch) & 0x01) == 0) {
        return ch;
      } else if (ch <= ' ') {   // this will only be true if one of the whitespace bits was set
        // whitespace... get new char at bottom of loop
      } else if (ch == '/') {
        getSlashComment();
      } else if (ch == '#') {
        getNewlineComment();
      } else if (!isWhitespace(ch)) { // we'll only reach here with certain bare strings, errors, or strange whitespace like 0xa0
        return ch;
      }
      ch = getChar();
    }
  }

  protected int getCharExpected(int expected) throws IOException {
    for (; ; ) {
      int ch = getChar();
      if (ch == expected) return expected;
      if (ch == ' ') continue;
      return getCharNWS(ch);
    }
  }

  protected void getNewlineComment() throws IOException {
    // read a # or a //, so go until newline
    for (; ; ) {
      int ch = getChar();
      // don't worry about DOS /r/n... we'll stop on the \r and let the rest of the whitespace
      // eater consume the \n
      if (ch == '\n' || ch == '\r' || ch == -1) {
        return;
      }
    }
  }

  protected void getSlashComment() throws IOException {
    int ch = getChar();
    if (ch == '/') {
      getNewlineComment();
      return;
    }

    if (ch != '*') {
      throw err("Invalid comment: expected //, /*, or #");
    }

    ch = getChar();
    for (; ; ) {
      if (ch == '*') {
        ch = getChar();
        if (ch == '/') {
          return;
        } else if (ch == '*') {
          // handle cases of *******/
          continue;
        }
      }
      if (ch == -1) {
        return;
      }
      ch = getChar();
    }
  }


  protected boolean matchBareWord(char[] arr) throws IOException {
    for (int i = 1; i < arr.length; i++) {
      int ch = getChar();
      if (ch != arr[i]) {
        if ((flags & ALLOW_UNQUOTED_STRING_VALUES) == 0) {
          throw err("Expected " + new String(arr));
        } else {
          stringTerm = 0;
          out.reset();
          out.write(arr, 0, i);
          if (!eof) {
            start--;
          }
          return false;
        }
      }
    }

    // if we don't allow bare strings, we don't need to check that the string actually terminates... just
    // let things fail as the parser tries to continue
    if ((flags & ALLOW_UNQUOTED_STRING_VALUES) == 0) {
      return true;
    }

    // check that the string actually terminates... for example trueX should return false
    int ch = getChar();
    if (eof) {
      return true;
    } else if (!isUnquotedStringChar(ch)) {
      start--;
      return true;
    }

    // we encountered something like "trueX" when matching "true"
    stringTerm = 0;
    out.reset();
    out.unsafeWrite(arr, 0, arr.length);
    out.unsafeWrite(ch);
    return false;
  }

  protected ParseException err(String msg) {
    // We can't tell if EOF was hit by comparing start<=end
    // because the illegal char could have been the last in the buffer
    // or in the stream.  To deal with this, the "eof" var was introduced
    if (!eof && start > 0) start--;  // backup one char
    String chs = "char=" + ((start >= end) ? "(EOF)" : "" + buf[start]);
    String pos = "position=" + (gpos + start);
    String tot = chs + ',' + pos + getContext();
    if (msg == null) {
      if (start >= end) msg = "Unexpected EOF";
      else msg = "JSON Parse Error";
    }
    return new ParseException(msg + ": " + tot);
  }

  private String getContext() {
    String context = "";
    if (start >= 0) {
      context += " AFTER='" + errEscape(Math.max(start - 60, 0), start + 1) + "'";
    }
    if (start < end) {
      context += " BEFORE='" + errEscape(start + 1, start + 40) + "'";
    }
    return context;
  }

  private String errEscape(int a, int b) {
    b = Math.min(b, end);
    if (a >= b) return "";
    return new String(buf, a, b - a).replaceAll("\\s+", " ");
  }


  private boolean bool; // boolean value read
  private long lval;    // long value read
  private int nstate;   // current state while reading a number
  private static final int HAS_FRACTION = 0x01;  // nstate flag, '.' already read
  private static final int HAS_EXPONENT = 0x02;  // nstate flag, '[eE][+-]?[0-9]' already read

  /**
   * Returns the long read... only significant if valstate==LONG after
   * this call.  firstChar should be the first numeric digit read.
   */
  private long readNumber(int firstChar, boolean isNeg) throws IOException {
    out.unsafeWrite(firstChar);   // unsafe OK since we know output is big enough
    // We build up the number in the negative plane since it's larger (by one) than
    // the positive plane.
    long v = '0' - firstChar;
    // can't overflow a long in 18 decimal digits (i.e. 17 additional after the first).
    // we also need 22 additional to handle double so we'll handle in 2 separate loops.
    int i;
    for (i = 0; i < 17; i++) {
      int ch = getChar();
      // TODO: is this switch faster as an if-then-else?
      switch (ch) {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
          v = v * 10 - (ch - '0');
          out.unsafeWrite(ch);
          continue;
        case '.':
          out.unsafeWrite('.');
          valstate = readFrac(out, 22 - i);
          return 0;
        case 'e':
        case 'E':
          out.unsafeWrite(ch);
          nstate = 0;
          valstate = readExp(out, 22 - i);
          return 0;
        default:
          // return the number, relying on nextEvent() to return an error
          // for invalid chars following the number.
          if (ch != -1) --start;   // push back last char if not EOF

          valstate = LONG;
          return isNeg ? v : -v;
      }
    }

    // after this, we could overflow a long and need to do extra checking
    boolean overflow = false;
    long maxval = isNeg ? Long.MIN_VALUE : -Long.MAX_VALUE;

    for (; i < 22; i++) {
      int ch = getChar();
      switch (ch) {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
          if (v < (0x8000000000000000L / 10)) overflow = true;  // can't multiply by 10 w/o overflowing
          v *= 10;
          int digit = ch - '0';
          if (v < maxval + digit) overflow = true; // can't add digit w/o overflowing
          v -= digit;
          out.unsafeWrite(ch);
          continue;
        case '.':
          out.unsafeWrite('.');
          valstate = readFrac(out, 22 - i);
          return 0;
        case 'e':
        case 'E':
          out.unsafeWrite(ch);
          nstate = 0;
          valstate = readExp(out, 22 - i);
          return 0;
        default:
          // return the number, relying on nextEvent() to return an error
          // for invalid chars following the number.
          if (ch != -1) --start;   // push back last char if not EOF

          valstate = overflow ? BIGNUMBER : LONG;
          return isNeg ? v : -v;
      }
    }


    nstate = 0;
    valstate = BIGNUMBER;
    return 0;
  }


  // read digits right of decimal point
  private int readFrac(CharArr arr, int lim) throws IOException {
    nstate = HAS_FRACTION;  // deliberate set instead of '|'
    while (--lim >= 0) {
      int ch = getChar();
      if (ch >= '0' && ch <= '9') {
        arr.write(ch);
      } else if (ch == 'e' || ch == 'E') {
        arr.write(ch);
        return readExp(arr, lim);
      } else {
        if (ch != -1) start--; // back up
        return NUMBER;
      }
    }
    return BIGNUMBER;
  }


  // call after 'e' or 'E' has been seen to read the rest of the exponent
  private int readExp(CharArr arr, int lim) throws IOException {
    nstate |= HAS_EXPONENT;
    int ch = getChar();
    lim--;

    if (ch == '+' || ch == '-') {
      arr.write(ch);
      ch = getChar();
      lim--;
    }

    // make sure at least one digit is read.
    if (ch < '0' || ch > '9') {
      throw err("missing exponent number");
    }
    arr.write(ch);

    return readExpDigits(arr, lim);
  }

  // continuation of readExpStart
  private int readExpDigits(CharArr arr, int lim) throws IOException {
    while (--lim >= 0) {
      int ch = getChar();
      if (ch >= '0' && ch <= '9') {
        arr.write(ch);
      } else {
        if (ch != -1) start--; // back up
        return NUMBER;
      }
    }
    return BIGNUMBER;
  }

  private void continueNumber(CharArr arr) throws IOException {
    if (arr != out) arr.write(out);

    if ((nstate & HAS_EXPONENT) != 0) {
      readExpDigits(arr, Integer.MAX_VALUE);
      return;
    }
    if (nstate != 0) {
      readFrac(arr, Integer.MAX_VALUE);
      return;
    }

    for (; ; ) {
      int ch = getChar();
      if (ch >= '0' && ch <= '9') {
        arr.write(ch);
      } else if (ch == '.') {
        arr.write(ch);
        readFrac(arr, Integer.MAX_VALUE);
        return;
      } else if (ch == 'e' || ch == 'E') {
        arr.write(ch);
        readExp(arr, Integer.MAX_VALUE);
        return;
      } else {
        if (ch != -1) start--;
        return;
      }
    }
  }


  private int hexval(int hexdig) {
    if (hexdig >= '0' && hexdig <= '9') {
      return hexdig - '0';
    } else if (hexdig >= 'A' && hexdig <= 'F') {
      return hexdig + (10 - 'A');
    } else if (hexdig >= 'a' && hexdig <= 'f') {
      return hexdig + (10 - 'a');
    }
    throw err("invalid hex digit");
  }

  // backslash has already been read when this is called
  private char readEscapedChar() throws IOException {
    int ch = getChar();
    switch (ch) {
      case '"':
        return '"';
      case '\'':
        return '\'';
      case '\\':
        return '\\';
      case '/':
        return '/';
      case 'n':
        return '\n';
      case 'r':
        return '\r';
      case 't':
        return '\t';
      case 'f':
        return '\f';
      case 'b':
        return '\b';
      case 'u':
        return (char) (
            (hexval(getChar()) << 12)
                | (hexval(getChar()) << 8)
                | (hexval(getChar()) << 4)
                | (hexval(getChar())));
    }
    if ((flags & ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER) != 0 && ch != EOF) {
      return (char) ch;
    }
    throw err("Invalid character escape");
  }

  // a dummy buffer we can use to point at other buffers
  private final CharArr tmp = new CharArr(null, 0, 0);

  private CharArr readStringChars() throws IOException {
    if (stringTerm == 0) {
      // "out" will already contain the first part of the bare string, so don't reset it
      readStringBare(out);
      return out;
    }

    char terminator = (char) stringTerm;
    int i;
    for (i = start; i < end; i++) {
      char c = buf[i];
      if (c == terminator) {
        tmp.set(buf, start, i);  // directly use input buffer
        start = i + 1; // advance past last '"'
        return tmp;
      } else if (c == '\\') {
        break;
      }
    }
    out.reset();
    readStringChars2(out, i);
    return out;
  }


  // middle is the pointer to the middle of a buffer to start scanning for a non-string
  // character ('"' or "/").  start<=middle<end
  // this should be faster for strings with fewer escapes, but probably slower for many escapes.
  private void readStringChars2(CharArr arr, int middle) throws IOException {
    if (stringTerm == 0) {
      readStringBare(arr);
      return;
    }

    char terminator = (char) stringTerm;

    for (; ; ) {
      if (middle >= end) {
        arr.write(buf, start, middle - start);
        start = middle;
        getMore();
        middle = start;
      }
      int ch = buf[middle++];
      if (ch == terminator) {
        int len = middle - start - 1;
        if (len > 0) arr.write(buf, start, len);
        start = middle;
        return;
      } else if (ch == '\\') {
        int len = middle - start - 1;
        if (len > 0) arr.write(buf, start, len);
        start = middle;
        arr.write(readEscapedChar());
        middle = start;
      }
    }
  }

  private void readStringBare(CharArr arr) throws IOException {
    if (arr != out) {
      arr.append(out);
    }

    for (; ; ) {
      int ch = getChar();
      if (!isUnquotedStringChar(ch)) {
        if (ch == -1) break;
        if (ch == '\\') {
          arr.write(readEscapedChar());
          continue;
        }
        start--;
        break;
      }

      if (ch == '\\') {
        arr.write(readEscapedChar());
        continue;
      }

      arr.write(ch);
    }
  }


  // isName==true if this is a field name (as opposed to a value)
  protected void handleNonDoubleQuoteString(int ch, boolean isName) throws IOException {
    if (ch == '\'') {
      stringTerm = ch;
      if ((flags & ALLOW_SINGLE_QUOTES) == 0) {
        throw err("Single quoted strings not allowed");
      }
    } else {
      if (isName && (flags & ALLOW_UNQUOTED_KEYS) == 0
          || !isName && (flags & ALLOW_UNQUOTED_STRING_VALUES) == 0
          || eof) {
        if (isName) {
          throw err("Expected quoted string");
        } else {
          throw err(null);
        }
      }

      if (!isUnquotedStringStart(ch)) {
        throw err(null);
      }

      stringTerm = 0;  // signal for unquoted string
      out.reset();
      out.unsafeWrite(ch);
    }
  }

  private static boolean isUnquotedStringStart(int ch) {
    return Character.isJavaIdentifierStart(ch);
  }

  // What characters are allowed to continue an unquoted string
  // once we know we are in one.
  private static boolean isUnquotedStringChar(int ch) {
    return Character.isJavaIdentifierPart(ch)
        || ch == '.'
        || ch == '-'
        || ch == '/';

    // would checking for a-z first speed up the common case?

    // possibly much more liberal unquoted string handling...
    /***
     switch (ch) {
     case -1:
     case ' ':
     case '\t':
     case '\r':
     case '\n':
     case '}':
     case ']':
     case ',':
     case ':':
     case '=':   // reserved for future use
     case '\\':  // check for backslash should come after this function call
     return false;
     }
     return true;
     ***/
  }


  /*** alternate implementation
   // middle is the pointer to the middle of a buffer to start scanning for a non-string
   // character ('"' or "/").  start<=middle<end
   private void readStringChars2a(CharArr arr, int middle) throws IOException {
   int ch=0;
   for(;;) {
   // find the next non-string char
   for (; middle<end; middle++) {
   ch = buf[middle];
   if (ch=='"' || ch=='\\') break;
   }

   arr.write(buf,start,middle-start);
   if (middle>=end) {
   getMore();
   middle=start;
   } else {
   start = middle+1;   // set buffer pointer to correct spot
   if (ch=='"') {
   valstate=0;
   return;
   } else if (ch=='\\') {
   arr.write(readEscapedChar());
   if (start>=end) getMore();
   middle=start;
   }
   }
   }
   }
   ***/


  // return the next event when parser is in a neutral state (no
  // map separators or array element separators to read
  private int next(int ch) throws IOException {
    // TODO: try my own form of indirect jump... look up char class and index directly into handling implementation?
    for (; ; ) {
      switch (ch) {
        case ' ': // this is not the exclusive list of whitespace chars... the rest are handled in default:
        case '\t':
        case '\r':
        case '\n':
          ch = getCharNWS(); // calling getCharNWS here seems faster than letting the switch handle it
          break;
        case '"':
          stringTerm = '"';
          valstate = STRING;
          return STRING;
        case '\'':
          if ((flags & ALLOW_SINGLE_QUOTES) == 0) {
            throw err("Single quoted strings not allowed");
          }
          stringTerm = '\'';
          valstate = STRING;
          return STRING;
        case '{':
          push();
          state = DID_OBJSTART;
          return OBJECT_START;
        case '[':
          push();
          state = DID_ARRSTART;
          return ARRAY_START;
        case '0':
          out.reset();
          //special case '0'?  If next char isn't '.' val=0
          ch = getChar();
          if (ch == '.') {
            start--;
            ch = '0';
            readNumber('0', false);
            return valstate;
          } else if (ch > '9' || ch < '0') {
            out.unsafeWrite('0');
            if (ch != -1) start--;
            lval = 0;
            valstate = LONG;
            return LONG;
          } else {
            throw err("Leading zeros not allowed");
          }
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
          out.reset();
          lval = readNumber(ch, false);
          return valstate;
        case '-':
          out.reset();
          out.unsafeWrite('-');
          ch = getChar();
          if (ch < '0' || ch > '9') throw err("expected digit after '-'");
          lval = readNumber(ch, true);
          return valstate;
        case 't':
          // TODO: test performance of this non-branching inline version.
          // if ((('r'-getChar())|('u'-getChar())|('e'-getChar())) != 0) throw err("");
          if (matchBareWord(JSONUtil.TRUE_CHARS)) {
            bool = true;
            valstate = BOOLEAN;
            return valstate;
          } else {
            valstate = STRING;
            return STRING;
          }
        case 'f':
          if (matchBareWord(JSONUtil.FALSE_CHARS)) {
            bool = false;
            valstate = BOOLEAN;
            return valstate;
          } else {
            valstate = STRING;
            return STRING;
          }
        case 'n':
          if (matchBareWord(JSONUtil.NULL_CHARS)) {
            valstate = NULL;
            return valstate;
          } else {
            valstate = STRING;
            return STRING;
          }
        case '/':
          getSlashComment();
          ch = getChar();
          break;
        case '#':
          getNewlineComment();
          ch = getChar();
          break;
        case ']':  // This only happens with a trailing comma (or an error)
          if (state != DID_ARRELEM || (flags & ALLOW_EXTRA_COMMAS) == 0) {
            throw err("Unexpected array closer ]");
          }
          pop();
          return event = ARRAY_END;
        case '}':  // This only happens with a trailing comma (or an error)
          if (state != DID_MEMVAL || (flags & ALLOW_EXTRA_COMMAS) == 0) {
            throw err("Unexpected object closer }");
          }
          pop();
          return event = ARRAY_END;
        case ',': // This only happens with input like [1,]
          if ((state != DID_ARRELEM && state != DID_MEMVAL) || (flags & ALLOW_EXTRA_COMMAS) == 0) {
            throw err("Unexpected comma");
          }
          ch = getChar();
          break;
        case -1:
          if (getLevel() > 0) throw err("Premature EOF");
          return EOF;
        default:
          // Handle unusual unicode whitespace like no-break space (0xA0)
          if (isWhitespace(ch)) {
            ch = getChar();  // getCharNWS() would also work
            break;
          }
          handleNonDoubleQuoteString(ch, false);
          valstate = STRING;
          return STRING;
        // throw err(null);
      }

    }
  }

  @Override
  public String toString() {
    return "start=" + start + ",end=" + end + ",state=" + state + "valstate=" + valstate;
  }


  /**
   * Returns the next event encountered in the JSON stream, one of
   * <ul>
   * <li>{@link #STRING}</li>
   * <li>{@link #LONG}</li>
   * <li>{@link #NUMBER}</li>
   * <li>{@link #BIGNUMBER}</li>
   * <li>{@link #BOOLEAN}</li>
   * <li>{@link #NULL}</li>
   * <li>{@link #OBJECT_START}</li>
   * <li>{@link #OBJECT_END}</li>
   * <li>{@link #OBJECT_END}</li>
   * <li>{@link #ARRAY_START}</li>
   * <li>{@link #ARRAY_END}</li>
   * <li>{@link #EOF}</li>
   * </ul>
   */
  public int nextEvent() throws IOException {
    if (valstate != 0) {
      if (valstate == STRING) {
        readStringChars2(devNull, start);
      } else if (valstate == BIGNUMBER) {
        continueNumber(devNull);
      }
      valstate = 0;
    }

    int ch;
    outer:
    for (; ; ) {
      switch (state) {
        case 0:
          event = next(getChar());
          if (event == STRING && (flags & OPTIONAL_OUTER_BRACES) != 0) {
            if (start > 0) start--;
            missingOpeningBrace = true;
            stringTerm = 0;
            valstate = 0;
            event = next('{');
          }
          return event;
        case DID_OBJSTART:
          ch = getCharExpected('"');
          if (ch == '}') {
            pop();
            return event = OBJECT_END;
          }
          if (ch == '"') {
            stringTerm = ch;
          } else if (ch == ',' && (flags & ALLOW_EXTRA_COMMAS) != 0) {
            continue outer;
          } else {
            handleNonDoubleQuoteString(ch, true);
          }
          state = DID_MEMNAME;
          valstate = STRING;
          return event = STRING;
        case DID_MEMNAME:
          ch = getCharExpected(':');
          if (ch != ':') {
            if ((ch == '{' || ch == '[') && (flags & ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT) != 0) {
              start--;
            } else {
              throw err("Expected key,value separator ':'");
            }
          }
          state = DID_MEMVAL;  // set state first because it might be pushed...
          return event = next(getChar());
        case DID_MEMVAL:
          ch = getCharExpected(',');
          if (ch == '}') {
            pop();
            return event = OBJECT_END;
          } else if (ch != ',') {
            if ((flags & ALLOW_EXTRA_COMMAS) != 0 && (ch == '\'' || ch == '"' || Character.isLetter(ch))) {
              start--;
            } else if (missingOpeningBrace && ch == -1 && (flags & OPTIONAL_OUTER_BRACES) != 0) {
              missingOpeningBrace = false;
              pop();
              return event = OBJECT_END;
            } else throw err("Expected ',' or '}'");
          }
          ch = getCharExpected('"');
          if (ch == '"') {
            stringTerm = ch;
          } else if ((ch == ',' || ch == '}') && (flags & ALLOW_EXTRA_COMMAS) != 0) {
            if (ch == ',') continue outer;
            pop();
            return event = OBJECT_END;
          } else {
            handleNonDoubleQuoteString(ch, true);
          }
          state = DID_MEMNAME;
          valstate = STRING;
          return event = STRING;
        case DID_ARRSTART:
          ch = getCharNWS();
          if (ch == ']') {
            pop();
            return event = ARRAY_END;
          }
          state = DID_ARRELEM;  // set state first, might be pushed...
          return event = next(ch);
        case DID_ARRELEM:
          ch = getCharExpected(',');
          if (ch == ',') {
            // state = DID_ARRELEM;  // redundant
            return event = next(getChar());
          } else if (ch == ']') {
            pop();
            return event = ARRAY_END;
          } else {
            if ((ch == '{' || ch == '[') && (flags & ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT) != 0) {
              return event = next(ch);
            } else {
              throw err("Expected ',' or ']'");
            }
          }
      }
    } // end for(;;)
  }

  public int lastEvent() {
    return event;
  }

  public boolean wasKey() {
    return state == DID_MEMNAME;
  }


  private void goTo(int what) throws IOException {
    if (valstate == what) {
      valstate = 0;
      return;
    }
    if (valstate == 0) {
      /*int ev = */
      nextEvent();      // TODO
      if (valstate != what) {
        throw err("type mismatch");
      }
      valstate = 0;
    } else {
      throw err("type mismatch");
    }
  }

  /**
   * Returns the JSON string value, decoding any escaped characters.
   */
  public String getString() throws IOException {
    return getStringChars().toString();
  }

  /**
   * Returns the characters of a JSON string value, decoding any escaped characters.
   * The underlying buffer of the returned <code>CharArr</code> should *not* be
   * modified as it may be shared with the input buffer.
   * The returned <code>CharArr</code> will only be valid up until
   * the next JSONParser method is called.  Any required data should be
   * read before that point.
   */
  public CharArr getStringChars() throws IOException {
    goTo(STRING);
    return readStringChars();
  }

  /**
   * Reads a JSON string into the output, decoding any escaped characters.
   */
  public void getString(CharArr output) throws IOException {
    goTo(STRING);
    readStringChars2(output, start);
  }

  /**
   * Reads a number from the input stream and parses it as a long, only if
   * the value will in fact fit into a signed 64 bit integer.
   */
  public long getLong() throws IOException {
    goTo(LONG);
    return lval;
  }

  /**
   * Reads a number from the input stream and parses it as a double
   */
  public double getDouble() throws IOException {
    return Double.parseDouble(getNumberChars().toString());
  }

  /**
   * Returns the characters of a JSON numeric value.
   * <p>The underlying buffer of the returned <code>CharArr</code> should *not* be
   * modified as it may be shared with the input buffer.
   * <p>The returned <code>CharArr</code> will only be valid up until
   * the next JSONParser method is called.  Any required data should be
   * read before that point.
   */
  public CharArr getNumberChars() throws IOException {
    int ev = 0;
    if (valstate == 0) ev = nextEvent();

    if (valstate == LONG || valstate == NUMBER) {
      valstate = 0;
      return out;
    } else if (valstate == BIGNUMBER) {
      continueNumber(out);
      valstate = 0;
      return out;
    } else {
      throw err("Unexpected " + ev);
    }
  }

  /**
   * Reads a JSON numeric value into the output.
   */
  public void getNumberChars(CharArr output) throws IOException {
    int ev = 0;
    if (valstate == 0) ev = nextEvent();
    if (valstate == LONG || valstate == NUMBER) output.write(this.out);
    else if (valstate == BIGNUMBER) {
      continueNumber(output);
    } else {
      throw err("Unexpected " + ev);
    }
    valstate = 0;
  }

  /**
   * Reads a boolean value
   */
  public boolean getBoolean() throws IOException {
    goTo(BOOLEAN);
    return bool;
  }

  /**
   * Reads a null value
   */
  public void getNull() throws IOException {
    goTo(NULL);
  }

  /**
   * @return the current nesting level, the number of parent objects or arrays.
   */
  public int getLevel() {
    return ptr;
  }

  public long getPosition() {
    return gpos + start;
  }
}