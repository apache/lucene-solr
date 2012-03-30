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

package org.apache.noggit;

import java.io.IOException;
import java.io.Reader;

/**
 * @author yonik
 * @version $Id: JSONParser.java 1099557 2011-05-04 18:54:26Z yonik $
 */

public class JSONParser {

  /** Event indicating a JSON string value, including member names of objects */
  public static final int STRING=1;
  /** Event indicating a JSON number value which fits into a signed 64 bit integer */
  public static final int LONG=2;
  /** Event indicating a JSON number value which has a fractional part or an exponent
   * and with string length <= 23 chars not including sign.  This covers
   * all representations of normal values for Double.toString().
   */
  public static final int NUMBER=3;
  /** Event indicating a JSON number value that was not produced by toString of any
   * Java primitive numerics such as Double or Long.  It is either
   * an integer outside the range of a 64 bit signed integer, or a floating
   * point value with a string representation of more than 23 chars.
    */
  public static final int BIGNUMBER=4;
  /** Event indicating a JSON boolean */
  public static final int BOOLEAN=5;
  /** Event indicating a JSON null */
  public static final int NULL=6;
  /** Event indicating the start of a JSON object */
  public static final int OBJECT_START=7;
  /** Event indicating the end of a JSON object */
  public static final int OBJECT_END=8;
  /** Event indicating the start of a JSON array */
  public static final int ARRAY_START=9;
  /** Event indicating the end of a JSON array */
  public static final int ARRAY_END=10;
  /** Event indicating the end of input has been reached */
  public static final int EOF=11;

  public static class ParseException extends RuntimeException {
    public ParseException(String msg) {
      super(msg);
    }
  }

  public static String getEventString( int e )
  {
    switch( e )
    {
    case STRING: return "STRING";
    case LONG: return "LONG";
    case NUMBER: return "NUMBER";
    case BIGNUMBER: return "BIGNUMBER";
    case BOOLEAN: return "BOOLEAN";
    case NULL: return "NULL";
    case OBJECT_START: return "OBJECT_START";
    case OBJECT_END: return "OBJECT_END";
    case ARRAY_START: return "ARRAY_START";
    case ARRAY_END: return "ARRAY_END";
    case EOF: return "EOF";
    }
    return "Unknown: "+e;
  }

  private static final CharArr devNull = new NullCharArr();


  final char[] buf;  // input buffer with JSON text in it
  int start;         // current position in the buffer
  int end;           // end position in the buffer (one past last valid index)
  final Reader in;   // optional reader to obtain data from
  boolean eof=false; // true if the end of the stream was reached.
  long gpos;          // global position = gpos + start

  int event;         // last event read

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
    this.buf = new char[end-start];
    data.getChars(start,end,buf,0);
  }

  // temporary output buffer
  private final CharArr out = new CharArr(64);

  // We need to keep some state in order to (at a minimum) know if
  // we should skip ',' or ':'.
  private byte[] stack = new byte[16];
  private int ptr=0;     // pointer into the stack of parser states
  private byte state=0;  // current parser state

  // parser states stored in the stack
  private static final byte DID_OBJSTART =1;  // '{' just read
  private static final byte DID_ARRSTART =2;  // '[' just read
  private static final byte DID_ARRELEM =3;   // array element just read
  private static final byte DID_MEMNAME =4;   // object member name (map key) just read
  private static final byte DID_MEMVAL =5;    // object member value (map val) just read

  // info about value that was just read (or is in the middle of being read)
  private int valstate;

  // push current parser state (use at start of new container)
  private final void push() {
    if (ptr >= stack.length) {
      // doubling here is probably overkill, but anything that needs to double more than
      // once (32 levels deep) is very atypical anyway.
      byte[] newstack = new byte[stack.length<<1];
      System.arraycopy(stack,0,newstack,0,stack.length);
      stack = newstack;
    }
    stack[ptr++] = state;
  }

  // pop  parser state (use at end of container)
  private final void pop() {
    if (--ptr<0) {
      throw err("Unbalanced container");
    } else {
      state = stack[ptr];
    }
  }

  protected void fill() throws IOException {
    if (in!=null) {
      gpos += end;
      start=0;
      int num = in.read(buf,0,buf.length);
      end = num>=0 ? num : 0;
    }
    if (start>=end) eof=true;
  }

  private void getMore() throws IOException {
    fill();
    if (start>=end) {
      throw err(null);
    }
  }

  protected int getChar() throws IOException {
    if (start>=end) {
      fill();
      if (start>=end) return -1;
    }
    return buf[start++];
  }

  private int getCharNWS() throws IOException {
    for (;;) {
      int ch = getChar();
      if (!(ch==' ' || ch=='\t' || ch=='\n' || ch=='\r')) return ch;
    }
  }

  private void expect(char[] arr) throws IOException {
    for (int i=1; i<arr.length; i++) {
      int ch = getChar();
      if (ch != arr[i]) {
        throw err("Expected " + new String(arr));
      }
    }
  }

  private ParseException err(String msg) {
    // We can't tell if EOF was hit by comparing start<=end
    // because the illegal char could have been the last in the buffer
    // or in the stream.  To deal with this, the "eof" var was introduced
    if (!eof && start>0) start--;  // backup one char
    String chs = "char=" + ((start>=end) ? "(EOF)" : "" + (char)buf[start]);
    String pos = "position=" + (gpos+start);
    String tot = chs + ',' + pos + getContext();
    if (msg==null) {
      if (start>=end) msg = "Unexpected EOF";
      else msg="JSON Parse Error";
    }
    return new ParseException(msg + ": " + tot);
  }

  private String getContext() {
    String context = "";
    if (start>=0) {
      context += " BEFORE='" + errEscape(Math.max(start-60,0), start+1) + "'";
    }
    if (start<end) {
      context += " AFTER='" + errEscape(start+1, start+40) + "'";
    }
    return context;
  }

  private String errEscape(int a, int b) {
    b = Math.min(b, end);
    if (a>=b) return "";
    return new String(buf, a, b-a).replaceAll("\\s+"," ");
  }


  private boolean bool; // boolean value read
  private long lval;    // long value read
  private int nstate;   // current state while reading a number
  private static final int HAS_FRACTION = 0x01;  // nstate flag, '.' already read
  private static final int HAS_EXPONENT = 0x02;  // nstate flag, '[eE][+-]?[0-9]' already read

  /** Returns the long read... only significant if valstate==LONG after
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
    for (i=0; i<17; i++) {
      int ch = getChar();
      // TODO: is this switch faster as an if-then-else?
      switch(ch) {
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
          v = v*10 - (ch-'0');
          out.unsafeWrite(ch);
          continue;
        case '.':
          out.unsafeWrite('.');
          valstate = readFrac(out,22-i);
          return 0;
        case 'e':
        case 'E':
          out.unsafeWrite(ch);
          nstate=0;
          valstate = readExp(out,22-i);
          return 0;
        default:
          // return the number, relying on nextEvent() to return an error
          // for invalid chars following the number.
          if (ch!=-1) --start;   // push back last char if not EOF

          valstate = LONG;
          return isNeg ? v : -v;
      }
    }

    // after this, we could overflow a long and need to do extra checking
    boolean overflow = false;
    long maxval = isNeg ? Long.MIN_VALUE : -Long.MAX_VALUE;

    for (; i<22; i++) {
      int ch = getChar();
      switch(ch) {
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
          if (v < (0x8000000000000000L/10)) overflow=true;  // can't multiply by 10 w/o overflowing
          v *= 10;
          int digit = ch - '0';
          if (v < maxval + digit) overflow=true; // can't add digit w/o overflowing
          v -= digit;
          out.unsafeWrite(ch);
          continue;
        case '.':
          out.unsafeWrite('.');
          valstate = readFrac(out,22-i);
          return 0;
        case 'e':
        case 'E':
          out.unsafeWrite(ch);
          nstate=0;
          valstate = readExp(out,22-i);
          return 0;
        default:
          // return the number, relying on nextEvent() to return an error
          // for invalid chars following the number.
          if (ch!=-1) --start;   // push back last char if not EOF

          valstate = overflow ? BIGNUMBER : LONG;
          return isNeg ? v : -v;
      }
    }


    nstate=0;
    valstate = BIGNUMBER;
    return 0;
  }

  
  // read digits right of decimal point
  private int readFrac(CharArr arr, int lim) throws IOException {
    nstate = HAS_FRACTION;  // deliberate set instead of '|'
    while(--lim>=0) {
      int ch = getChar();
      if (ch>='0' && ch<='9') {
        arr.write(ch);
      } else if (ch=='e' || ch=='E') {
        arr.write(ch);
        return readExp(arr,lim);
      } else {
        if (ch!=-1) start--; // back up
        return NUMBER;
      }
    }
    return BIGNUMBER;
  }


  // call after 'e' or 'E' has been seen to read the rest of the exponent
  private int readExp(CharArr arr, int lim) throws IOException {
    nstate |= HAS_EXPONENT;
    int ch = getChar(); lim--;

    if (ch=='+' || ch=='-') {
      arr.write(ch);
      ch = getChar(); lim--;
    }

    // make sure at least one digit is read.
    if (ch<'0' || ch>'9') {
      throw err("missing exponent number");
    }
    arr.write(ch);

    return readExpDigits(arr,lim);
  }

  // continuation of readExpStart
  private int readExpDigits(CharArr arr, int lim) throws IOException {
    while (--lim>=0) {
      int ch = getChar();
      if (ch>='0' && ch<='9') {
        arr.write(ch);
      } else {
        if (ch!=-1) start--; // back up
        return NUMBER;
      }
    }
    return BIGNUMBER;
  }

  private void continueNumber(CharArr arr) throws IOException {
    if (arr != out) arr.write(out);

    if ((nstate & HAS_EXPONENT)!=0){
      readExpDigits(arr, Integer.MAX_VALUE);
      return;
    }
    if (nstate != 0) {
      readFrac(arr, Integer.MAX_VALUE);
      return;
    }

    for(;;) {
      int ch = getChar();
      if (ch>='0' && ch <='9') {
        arr.write(ch);
      } else if (ch=='.') {
        arr.write(ch);
        readFrac(arr,Integer.MAX_VALUE);
        return;
      } else if (ch=='e' || ch=='E') {
        arr.write(ch);
        readExp(arr,Integer.MAX_VALUE);
        return;
      } else {
        if (ch!=-1) start--;
        return;
      }
    }
  }


  private int hexval(int hexdig) {
    if (hexdig>='0' && hexdig <='9') {
      return hexdig-'0';
    } else if (hexdig>='A' && hexdig <='F') {
      return hexdig+(10-'A');
    } else if (hexdig>='a' && hexdig <='f') {
      return hexdig+(10-'a');
    }
    throw err("invalid hex digit");
  }

  // backslash has already been read when this is called
  private char readEscapedChar() throws IOException {
    switch (getChar()) {
      case '"' : return '"';
      case '\\' : return '\\';
      case '/' : return '/';
      case 'n' : return '\n';
      case 'r' : return '\r';
      case 't' : return '\t';
      case 'f' : return '\f';
      case 'b' : return '\b';
      case 'u' :
        return (char)(
               (hexval(getChar()) << 12)
             | (hexval(getChar()) << 8)
             | (hexval(getChar()) << 4)
             | (hexval(getChar())));
    }
    throw err("Invalid character escape in string");
  }

  // a dummy buffer we can use to point at other buffers
  private final CharArr tmp = new CharArr(null,0,0);

  private CharArr readStringChars() throws IOException {
     char c=0;
     int i;
     for (i=start; i<end; i++) {
      c = buf[i];
      if (c=='"') {
        tmp.set(buf,start,i);  // directly use input buffer
        start=i+1; // advance past last '"'
        return tmp;
      } else if (c=='\\') {
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
    for (;;) {
      if (middle>=end) {
        arr.write(buf,start,middle-start);
        start=middle;
        getMore();
        middle=start;
      }
      int ch = buf[middle++];
      if (ch=='"') {
        int len = middle-start-1;        
        if (len>0) arr.write(buf,start,len);
        start=middle;
        return;
      } else if (ch=='\\') {
        int len = middle-start-1;
        if (len>0) arr.write(buf,start,len);
        start=middle;
        arr.write(readEscapedChar());
        middle=start;
      }
    }
  }


  /*** alternate implelentation
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
    for(;;) {
      switch (ch) {
        case ' ':
        case '\t': break;
        case '\r':
        case '\n': break;  // try and keep track of linecounts?
        case '"' :
          valstate = STRING;
          return STRING;
        case '{' :
          push();
          state= DID_OBJSTART;
          return OBJECT_START;
        case '[':
          push();
          state=DID_ARRSTART;
          return ARRAY_START;
        case '0' :
          out.reset();
          //special case '0'?  If next char isn't '.' val=0
          ch=getChar();
          if (ch=='.') {
            start--; ch='0';
            readNumber('0',false);
            return valstate;
          } else if (ch>'9' || ch<'0') {
            out.unsafeWrite('0');
            if (ch!=-1) start--;
            lval = 0;
            valstate=LONG;
            return LONG;
          } else {
            throw err("Leading zeros not allowed");
          }
        case '1' :
        case '2' :
        case '3' :
        case '4' :
        case '5' :
        case '6' :
        case '7' :
        case '8' :
        case '9' :
          out.reset();
          lval = readNumber(ch,false);
          return valstate;
        case '-' :
          out.reset();
          out.unsafeWrite('-');
          ch = getChar();
          if (ch<'0' || ch>'9') throw err("expected digit after '-'");
          lval = readNumber(ch,true);
          return valstate;
        case 't':
          valstate=BOOLEAN;
          // TODO: test performance of this non-branching inline version.
          // if ((('r'-getChar())|('u'-getChar())|('e'-getChar())) != 0) err("");
          expect(JSONUtil.TRUE_CHARS);
          bool=true;
          return BOOLEAN;
        case 'f':
          valstate=BOOLEAN;
          expect(JSONUtil.FALSE_CHARS);
          bool=false;
          return BOOLEAN;
        case 'n':
          valstate=NULL;
          expect(JSONUtil.NULL_CHARS);
          return NULL;
        case -1:
          if (getLevel()>0) throw err("Premature EOF");
          return EOF;
        default: throw err(null);
      }

      ch = getChar();
    }
  }

  public String toString() {
    return "start="+start+",end="+end+",state="+state+"valstate="+valstate;
  }


  /** Returns the next event encountered in the JSON stream, one of
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
    if (valstate==STRING) {
      readStringChars2(devNull,start);
    }
    else if (valstate==BIGNUMBER) {
      continueNumber(devNull);
    }

    valstate=0;

    int ch;   // TODO: factor out getCharNWS() to here and check speed
    switch (state) {
      case 0:
        return event = next(getCharNWS());
      case DID_OBJSTART:
        ch = getCharNWS();
        if (ch=='}') {
          pop();
          return event = OBJECT_END;
        }
        if (ch != '"') {
          throw err("Expected string");
        }
        state = DID_MEMNAME;
        valstate = STRING;
        return event = STRING;
      case DID_MEMNAME:
        ch = getCharNWS();
        if (ch!=':') {
          throw err("Expected key,value separator ':'");
        }
        state = DID_MEMVAL;  // set state first because it might be pushed...
        return event = next(getChar());
      case DID_MEMVAL:
        ch = getCharNWS();
        if (ch=='}') {
          pop();
          return event = OBJECT_END;
        } else if (ch!=',') {
          throw err("Expected ',' or '}'");
        }
        ch = getCharNWS();
        if (ch != '"') {
          throw err("Expected string");
        }
        state = DID_MEMNAME;
        valstate = STRING;
        return event = STRING;
      case DID_ARRSTART:
        ch = getCharNWS();
        if (ch==']') {
          pop();
          return event = ARRAY_END;
        }
        state = DID_ARRELEM;  // set state first, might be pushed...
        return event = next(ch);
      case DID_ARRELEM:
        ch = getCharNWS();
        if (ch==']') {
          pop();
          return event = ARRAY_END;
        } else if (ch!=',') {
          throw err("Expected ',' or ']'");
        }
        // state = DID_ARRELEM;
        return event = next(getChar());
    }
    return 0;
  }

  public int lastEvent() {
    return event;
  }

  public boolean wasKey()
  {
    return state == DID_MEMNAME;
  }
  

  private void goTo(int what) throws IOException {
    if (valstate==what) { valstate=0; return; }
    if (valstate==0) {
      int ev = nextEvent();      // TODO
      if (valstate!=what) {
        throw err("type mismatch");
      }
      valstate=0;
    }
    else {
      throw err("type mismatch");
    }
  }

  /** Returns the JSON string value, decoding any escaped characters. */
  public String getString() throws IOException {
    return getStringChars().toString();
  }

  /** Returns the characters of a JSON string value, decoding any escaped characters.
   * <p/>The underlying buffer of the returned <code>CharArr</code> should *not* be
   * modified as it may be shared with the input buffer.
   * <p/>The returned <code>CharArr</code> will only be valid up until
   * the next JSONParser method is called.  Any required data should be
   * read before that point.
   */
  public CharArr getStringChars() throws IOException {
    goTo(STRING);
    return readStringChars();
  }

  /** Reads a JSON string into the output, decoding any escaped characters. */
  public void getString(CharArr output) throws IOException {
    goTo(STRING);
    readStringChars2(output,start);
  }

  /** Reads a number from the input stream and parses it as a long, only if
   * the value will in fact fit into a signed 64 bit integer. */
  public long getLong() throws IOException {
    goTo(LONG);
    return lval;
  }

  /** Reads a number from the input stream and parses it as a double */
  public double getDouble() throws IOException {
    return Double.parseDouble(getNumberChars().toString());
  }

  /** Returns the characters of a JSON numeric value.
   * <p/>The underlying buffer of the returned <code>CharArr</code> should *not* be
   * modified as it may be shared with the input buffer.
   * <p/>The returned <code>CharArr</code> will only be valid up until
   * the next JSONParser method is called.  Any required data should be
   * read before that point.
   */  
  public CharArr getNumberChars() throws IOException {
    int ev=0;
    if (valstate==0) ev = nextEvent();

    if (valstate == LONG || valstate == NUMBER) {
      valstate=0;
      return out;
    }
    else if (valstate==BIGNUMBER) {
      continueNumber(out);
      valstate=0;
      return out;
    } else {
      throw err("Unexpected " + ev);
    }
  }

  /** Reads a JSON numeric value into the output. */
  public void getNumberChars(CharArr output) throws IOException {
    int ev=0;
    if (valstate==0) ev=nextEvent();
    if (valstate == LONG || valstate == NUMBER) output.write(this.out);
    else if (valstate==BIGNUMBER) {
      continueNumber(output);
    } else {
      throw err("Unexpected " + ev);
    }
    valstate=0;
  }

  /** Reads a boolean value */  
  public boolean getBoolean() throws IOException {
    goTo(BOOLEAN);
    return bool;
  }

  /** Reads a null value */
  public void getNull() throws IOException {
    goTo(NULL);
  }

  /**
   * @return the current nesting level, the number of parent objects or arrays.
   */
  public int getLevel() {
    return ptr;
  }
  
  public long getPosition()
  {
    return gpos+start;
  }
}
