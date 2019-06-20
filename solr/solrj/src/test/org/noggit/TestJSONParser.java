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

import java.io.IOException;
import java.io.StringReader;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class TestJSONParser extends SolrTestCaseJ4 {

  // these are to aid in debugging if an unexpected error occurs
  static int parserType;
  static int bufferSize;
  static String parserInput;
  static JSONParser lastParser;

  static int flags = JSONParser.FLAGS_DEFAULT;  // the default

  public static String lastParser() {
    return "parserType=" + parserType
        + (parserType==1 ? " bufferSize=" + bufferSize : "")
        + " parserInput='" + parserInput + "'" + "flags : " + lastParser.flags;
  }

  public static JSONParser getParser(String s) {
    return getParser(s, random().nextInt(2), -1);
  }

  public static JSONParser getParser(String s, int type, int bufSize) {
    parserInput = s;
    parserType = type;

    JSONParser parser=null;
    switch (type) {
      case 0:
        // test directly using input buffer
        parser = new JSONParser(s.toCharArray(),0,s.length());
        break;
      case 1:
        // test using Reader...
        // small input buffers can help find bugs on boundary conditions

        if (bufSize < 1) bufSize = random().nextInt(25) + 1;
        bufferSize = bufSize;// record in case there is an error
        parser = new JSONParser(new StringReader(s), new char[bufSize]);
        break;
    }
    if (parser == null) return null;

    lastParser = parser;

    if (flags != JSONParser.FLAGS_DEFAULT) {
      parser.setFlags(flags);
    }

    return parser;
  }

  /** for debugging purposes
   public void testSpecific() throws Exception {
   JSONParser parser = getParser("[0",1,1);
   for (;;) {
   int ev = parser.nextEvent();
   if (ev == JSONParser.EOF) {
   break;
   } else {
   System.out.println("got " + JSONParser.getEventString(ev));
   }
   }
   }
   **/

  public static byte[] events = new byte[256];
  static {
    events['{'] = JSONParser.OBJECT_START;
    events['}'] = JSONParser.OBJECT_END;
    events['['] = JSONParser.ARRAY_START;
    events[']'] = JSONParser.ARRAY_END;
    events['s'] = JSONParser.STRING;
    events['b'] = JSONParser.BOOLEAN;
    events['l'] = JSONParser.LONG;
    events['n'] = JSONParser.NUMBER;
    events['N'] = JSONParser.BIGNUMBER;
    events['0'] = JSONParser.NULL;
    events['e'] = JSONParser.EOF;
  }

  // match parser states with the expected states
  public static void parse(JSONParser p, String input, String expected) throws IOException {
    expected += "e";
    for (int i=0; i<expected.length(); i++) {
      int ev = p.nextEvent();
      int expect = events[expected.charAt(i)];
      if (ev != expect) {
        fail("Expected " + expect + ", got " + ev
            + "\n\tINPUT=" + input
            + "\n\tEXPECTED=" + expected
            + "\n\tAT=" + i + " (" + expected.charAt(i) + ")");
      }
    }
  }

  public static void parse(String input, String expected) throws IOException {
    String in = input;
    if ((flags & JSONParser.ALLOW_SINGLE_QUOTES)==0 || random().nextBoolean()) {
      in = in.replace('\'', '"');
    }

    for (int i=0; i<Integer.MAX_VALUE; i++) {
      JSONParser p = getParser(in,i,-1);
      if (p==null) break;
      parse(p,in,expected);
    }

    testCorruption(input, 100000);

  }

  public static void testCorruption(String input, int iter) {
    char[] arr = new char[input.length()];

    for (int i=0; i<iter; i++) {
      input.getChars(0, arr.length, arr, 0);
      int changes = random().nextInt(arr.length>>1) + 1;
      for (int j=0; j<changes; j++) {
        char ch;
        switch (random().nextInt(31)) {
          case 0: ch = 0; break;
          case 1: ch = '['; break;
          case 2: ch = ']'; break;
          case 3: ch = '{'; break;
          case 4: ch = '}'; break;
          case 5: ch = '"'; break;
          case 6: ch = '\''; break;
          case 7: ch = ' '; break;
          case 8: ch = '\r'; break;
          case 9: ch = '\n'; break;
          case 10:ch = '\t'; break;
          case 11:ch = ','; break;
          case 12:ch = ':'; break;
          case 13:ch = '.'; break;
          case 14:ch = 'a'; break;
          case 15:ch = 'e'; break;
          case 16:ch = '0'; break;
          case 17:ch = '1'; break;
          case 18:ch = '+'; break;
          case 19:ch = '-'; break;
          case 20:ch = 't'; break;
          case 21:ch = 'f'; break;
          case 22:ch = 'n'; break;
          case 23:ch = '/'; break;
          case 24:ch = '\\'; break;
          case 25:ch = 'u'; break;
          case 26:ch = '\u00a0'; break;
          default:ch = (char) random().nextInt(256);
        }

        arr[random().nextInt(arr.length)] = ch;
      }


      JSONParser parser = getParser(new String(arr));
      parser.setFlags( random().nextInt() );  // set random parser flags

      int ret = 0;
      try {
        for (;;) {
          int ev = parser.nextEvent();
          if (random().nextBoolean()) {
            // see if we can read the event
            switch (ev) {
              case JSONParser.STRING: ret += parser.getString().length(); break;
              case JSONParser.BOOLEAN: ret += parser.getBoolean() ? 1 : 2; break;
              case JSONParser.BIGNUMBER: ret += parser.getNumberChars().length(); break;
              case JSONParser.NUMBER: ret += parser.getDouble(); break;
              case JSONParser.LONG: ret += parser.getLong(); break;
              default: ret += ev;
            }
          }

          if (ev == JSONParser.EOF) break;
        }
      } catch (IOException ex) {
        // shouldn't happen
        System.out.println(ret);  // use ret
      } catch (JSONParser.ParseException ex) {
        // OK
      } catch (Throwable ex) {
        ex.printStackTrace();
        System.out.println(lastParser());
        throw new RuntimeException(ex);
      }
    }
  }



  public static class Num {
    public String digits;
    public Num(String digits) {
      this.digits = digits;
    }
    @Override
    public String toString() { return new String("NUMBERSTRING("+digits+")"); }
    @Override
    public boolean equals(Object o) {
      return (getClass() == o.getClass() && digits.equals(((Num) o).digits));
    }
    @Override
    public int hashCode() {
      return digits.hashCode();
    }
  }

  public static class BigNum extends Num {
    @Override
    public String toString() { return new String("BIGNUM("+digits+")"); }
    public BigNum(String digits) { super(digits); }
  }

  // Oh, what I wouldn't give for Java5 varargs and autoboxing
  public static Long o(int l) { return (long) l; }
  public static Long o(long l) { return l; }
  public static Double o(double d) { return d; }
  public static Boolean o(boolean b) { return b; }
  public static Num n(String digits) { return new Num(digits); }
  public static Num bn(String digits) { return new BigNum(digits); }
  public static Object t = Boolean.TRUE;
  public static Object f = Boolean.FALSE;
  public static Object a = new Object(){@Override
  public String toString() {return "ARRAY_START";}};
  public static Object A = new Object(){@Override
  public String toString() {return "ARRAY_END";}};
  public static Object m = new Object(){@Override
  public String toString() {return "OBJECT_START";}};
  public static Object M = new Object(){@Override
  public String toString() {return "OBJECT_END";}};
  public static Object N = new Object(){@Override
  public String toString() {return "NULL";}};
  public static Object e = new Object(){@Override
  public String toString() {return "EOF";}};

  // match parser states with the expected states
  public static void parse(JSONParser p, String input, Object[] expected) throws IOException {
    for (int i=0; i<expected.length; i++) {
      int ev = p.nextEvent();
      Object exp = expected[i];
      Object got = null;

      switch(ev) {
        case JSONParser.ARRAY_START: got=a; break;
        case JSONParser.ARRAY_END: got=A; break;
        case JSONParser.OBJECT_START: got=m; break;
        case JSONParser.OBJECT_END: got=M; break;
        case JSONParser.LONG: got=o(p.getLong()); break;
        case JSONParser.NUMBER:
          if (exp instanceof Double) {
            got = o(p.getDouble());
          } else {
            got = n(p.getNumberChars().toString());
          }
          break;
        case JSONParser.BIGNUMBER: got=bn(p.getNumberChars().toString()); break;
        case JSONParser.NULL: got=N; p.getNull(); break; // optional
        case JSONParser.BOOLEAN: got=o(p.getBoolean()); break;
        case JSONParser.EOF: got=e; break;
        case JSONParser.STRING: got=p.getString(); break;
        default: got="Unexpected Event Number " + ev;
      }

      if (!(exp==got || exp.equals(got))) {
        fail("Fail: String='" + input + "'" + "\n\tINPUT=" + got + "\n\tEXPECTED=" + exp + "\n\tAT RULE " + i);
      }
    }
  }


  public static void parse(String input, Object[] expected) throws IOException {
    parse(input, (flags & JSONParser.ALLOW_SINGLE_QUOTES)==0 || random().nextBoolean(), expected);
  }

  public static void parse(String input, boolean changeSingleQuote, Object[] expected) throws IOException {
    String in = input;
    if (changeSingleQuote) {
      in = in.replace('\'', '"');
    }
    for (int i=0; i<Integer.MAX_VALUE; i++) {
      JSONParser p = getParser(in,i,-1);
      if (p == null) break;
      parse(p,in,expected);
    }
  }


  public static void err(String input) throws IOException {
    try {
      JSONParser p = getParser(input);
      while (p.nextEvent() != JSONParser.EOF) {}
    } catch (Exception e) {
      return;
    }
    fail("Input should failed:'" + input + "'");
  }

  @Test
  public void testNull() throws IOException {
    flags = JSONParser.FLAGS_STRICT;
    err("nul");
    err("n");
    err("nullz");
    err("[nullz]");
    flags = JSONParser.FLAGS_DEFAULT;

    parse("[null]","[0]");
    parse("{'hi':null}",new Object[]{m,"hi",N,M,e});
  }

  @Test
  public void testBool() throws IOException {
    flags = JSONParser.FLAGS_STRICT;
    err("[True]");
    err("[False]");
    err("[TRUE]");
    err("[FALSE]");
    err("[truex]");
    err("[falsex]");
    err("[tru]");
    err("[fals]");
    err("[tru");
    err("[fals");
    err("t");
    err("f");
    flags = JSONParser.FLAGS_DEFAULT;

    parse("[false,true, false , true ]",new Object[]{a,f,t,f,t,A,e});
  }

  @Test
  public void testString() throws IOException {
    // NOTE: single quotes are converted to double quotes by this
    // testsuite!
    err("[']");
    err("[',]");
    err("{'}");
    err("{',}");

    err("['\\u111']");
    err("['\\u11']");
    err("['\\u1']");
    err("['\\']");


    flags = JSONParser.FLAGS_STRICT;
    err("['\\ ']");  // escape of non-special char
    err("['\\U1111']");  // escape of non-special char
    flags = JSONParser.FLAGS_DEFAULT;

    parse("['\\ ']", new Object[]{a, " ", A, e});  // escape of non-special char
    parse("['\\U1111']", new Object[]{a, "U1111", A, e});  // escape of non-special char



    parse("['']",new Object[]{a,"",A,e});
    parse("['\\\\']",new Object[]{a,"\\",A,e});
    parse("['X\\\\']",new Object[]{a,"X\\",A,e});
    parse("['\\\\X']",new Object[]{a,"\\X",A,e});
    parse("[\"\\\"\"]",new Object[]{a,"\"",A,e});

    parse("['\\'']", true, new Object[]{a,"\"",A,e});
    parse("['\\'']", false, new Object[]{a,"'",A,e});


    String esc="\\n\\r\\tX\\b\\f\\/\\\\X\\\"";
    String exp="\n\r\tX\b\f/\\X\"";
    parse("['" + esc + "']",new Object[]{a,exp,A,e});
    parse("['" + esc+esc+esc+esc+esc + "']",new Object[]{a,exp+exp+exp+exp+exp,A,e});

    esc="\\u004A";
    exp="\u004A";
    parse("['" + esc + "']",new Object[]{a,exp,A,e});

    esc="\\u0000\\u1111\\u2222\\u12AF\\u12BC\\u19DE";
    exp="\u0000\u1111\u2222\u12AF\u12BC\u19DE";
    parse("['" + esc + "']",new Object[]{a,exp,A,e});

  }

  @Test
  public void testNumbers() throws IOException {
    flags = JSONParser.FLAGS_STRICT;

    err("[00]");
    err("[003]");
    err("[00.3]");
    err("[1e1.1]");
    err("[+1]");
    err("[NaN]");
    err("[Infinity]");
    err("[--1]");

    flags = JSONParser.FLAGS_DEFAULT;

    String lmin    = "-9223372036854775808";
    String lminNot = "-9223372036854775809";
    String lmax    = "9223372036854775807";
    String lmaxNot = "9223372036854775808";

    String bignum="12345678987654321357975312468642099775533112244668800152637485960987654321";

    parse("[0,1,-1,543,-876]", new Object[]{a,o(0),o(1),o(-1),o(543),o(-876),A,e});
    parse("[-0]",new Object[]{a,o(0),A,e});


    parse("["+lmin +"," + lmax+"]",
        new Object[]{a,o(Long.MIN_VALUE),o(Long.MAX_VALUE),A,e});

    parse("["+bignum+"]", new Object[]{a,bn(bignum),A,e});
    parse("["+"-"+bignum+"]", new Object[]{a,bn("-"+bignum),A,e});

    parse("["+lminNot+"]",new Object[]{a,bn(lminNot),A,e});
    parse("["+lmaxNot+"]",new Object[]{a,bn(lmaxNot),A,e});

    parse("["+lminNot + "," + lmaxNot + "]",
        new Object[]{a,bn(lminNot),bn(lmaxNot),A,e});

    // bignum many digits on either side of decimal
    String t = bignum + "." + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    err("[" + t+".1" + "]"); // extra decimal
    err("[" + "-"+t+".1" + "]");

    // bignum exponent w/o fraction
    t = "1" + "e+" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    t = "1" + "E+" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    t = "1" + "e" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    t = "1" + "E" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    t = "1" + "e-" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    t = "1" + "E-" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});

    t = bignum + "e+" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    t = bignum + "E-" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    t = bignum + "e" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});

    t = bignum + "." + bignum + "e" + bignum;
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});

    err("[1E]");
    err("[1E-]");
    err("[1E+]");
    err("[1E+.3]");
    err("[1E+0.3]");
    err("[1E+1e+3]");
    err("["+bignum+"e"+"]");
    err("["+bignum+"e-"+"]");
    err("["+bignum+"e+"+"]");
    err("["+bignum+"."+bignum+"."+bignum+"]");


    double[] vals = new double[] {0,0.1,1.1,
        Double.MAX_VALUE,
        Double.MIN_VALUE,
        2.2250738585072014E-308, /* Double.MIN_NORMAL */
    };
    for (int i=0; i<vals.length; i++) {
      double d = vals[i];
      parse("["+d+","+-d+"]", new Object[]{a,o(d),o(-d),A,e});
    }

    // MIN_NORMAL has the max number of digits (23), so check that
    // adding an extra digit causes BIGNUM to be returned.
    t = "2.2250738585072014E-308" + "0";
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});
    // check it works with a leading zero too
    t = "0.2250738585072014E-308" + "0";
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});

    // check that overflow detection is working properly w/ numbers that don't cause a wrap to negatives
    // when multiplied by 10
    t = "1910151821265210155" + "0";
    parse("["+t+","+"-"+t+"]", new Object[]{a,bn(t),bn("-"+t),A,e});

    for (int i=0; i<1000000; i++) {
      long val = random().nextLong();
      String sval = Long.toString(val);
      JSONParser parser = getParser("["+val+"]");
      parser.nextEvent();
      assertTrue(parser.nextEvent() == JSONParser.LONG);
      if (random().nextBoolean()) {
        assertEquals(val, parser.getLong());
      } else {
        CharArr chars = parser.getNumberChars();
        assertEquals(sval, chars.toString());
      }
    }

  }

  @Test
  public void testArray() throws IOException {
    parse("[]","[]");
    parse("[ ]","[]");
    parse(" \r\n\t[\r\t\n ]\r\n\t ","[]");

    parse("[0]","[l]");
    parse("['0']","[s]");
    parse("[0,'0',0.1]","[lsn]");

    parse("[[[[[]]]]]","[[[[[]]]]]");
    parse("[[[[[0]]]]]","[[[[[l]]]]]");

    err("]");
    err("[");
    err("[[]");
    err("[]]");
    err("[}");
    err("{]");
    err("['a':'b']");

    flags=JSONParser.FLAGS_STRICT;
    err("[,]");         // test that extra commas fail
    err("[[],]");
    err("['a',]");
    err("['a',]");
    flags=JSONParser.FLAGS_DEFAULT;

    parse("[,]","[]");  // test extra commas
    parse("[,,]","[]");
    parse("[,,,]","[]");
    parse("[[],]","[[]]");
    parse("[[,],]","[[]]");
    parse("[[,,],,]","[[]]");
    parse("[,[,,],,]","[[]]");
    parse("[,5,[,,5],,]","[l[l]]");

  }

  @Test
  public void testObject() throws IOException {
    parse("{}","{}");
    parse("{}","{}");
    parse(" \r\n\t{\r\t\n }\r\n\t ","{}");

    parse("{'':null}","{s0}");

    err("}");
    err("[}]");
    err("{");
    err("[{]");
    err("{{}");
    err("[{{}]");
    err("{}}");
    err("[{}}]");
    err("{1}");
    err("[{1}]");
    err("{'a'}");
    err("{'a','b'}");
    err("{[]:'b'}");
    err("{{'a':'b'}:'c'}");
    err("{'a','b'}}");

    // bare strings allow these to pass
    flags=JSONParser.FLAGS_STRICT;
    err("{null:'b'}");
    err("{true:'b'}");
    err("{false:'b'}");
    err("{,}");         // test that extra commas fail
    err("{{},}");
    err("{'a':'b',}");
    flags=JSONParser.FLAGS_DEFAULT;

    parse("{}", new Object[]{m,M,e});
    parse("{,}", new Object[]{m,M,e});
    parse("{,,}", new Object[]{m,M,e});
    parse("{'a':{},}", new Object[]{m,"a",m,M,M,e});
    parse("{'a':{},,}", new Object[]{m,"a",m,M,M,e});
    parse("{,'a':{,},,}", new Object[]{m,"a",m,M,M,e});
    parse("{'a':'b'}", new Object[]{m,"a","b",M,e});
    parse("{'a':5}", new Object[]{m,"a",o(5),M,e});
    parse("{'a':null}", new Object[]{m,"a",N,M,e});
    parse("{'a':[]}", new Object[]{m,"a",a,A,M,e});
    parse("{'a':{'b':'c'}}", new Object[]{m,"a",m,"b","c",M,M,e});

    String big = "Now is the time for all good men to come to the aid of their country!";
    String bigger = big+big+big+big+big;
    parse("{'"+bigger+"':'"+bigger+"','a':'b'}", new Object[]{m,bigger,bigger,"a","b",M,e});


    flags=JSONParser.ALLOW_UNQUOTED_KEYS;
    parse("{a:'b'}", new Object[]{m,"a","b",M,e});
    parse("{null:'b'}", new Object[]{m,"null","b",M,e});
    parse("{true: 'b'}", new Object[]{m,"true","b",M,e});
    parse("{ false :'b'}", new Object[]{m,"false","b",M,e});
    parse("{null:null, true : true , false : false , x:'y',a:'b'}", new Object[]{m,"null",N,"true",t,"false",f,"x","y","a","b",M,e});
    flags = JSONParser.FLAGS_DEFAULT | JSONParser.ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT;
    parse("{'a'{'b':'c'}}", new Object[]{m, "a", m, "b", "c", M, M, e});
    parse("{'a': [{'b':'c'} {'b':'c'}]}", new Object[]{m, "a",a, m, "b", "c", M, m, "b", "c", M,A, M, e});
    parse("{'a' [{'b':'c'} {'b':'c'}]}", new Object[]{m, "a", a, m, "b", "c", M, m, "b", "c", M, A, M, e});
    parse("{'a':[['b']['c']]}", new Object[]{m, "a", a, a, "b", A, a, "c", A, A, M, e});
    parse("{'a': {'b':'c'} 'd': {'e':'f'}}", new Object[]{m, "a", m, "b", "c",M,  "d", m,"e","f", M, M, e});
    parse("{'a': {'b':'c'} d: {'e':'f'}}", new Object[]{m, "a", m, "b", "c",M,  "d", m,"e","f", M, M, e});

    flags = JSONParser.FLAGS_DEFAULT | JSONParser.ALLOW_MISSING_COLON_COMMA_BEFORE_OBJECT | JSONParser.OPTIONAL_OUTER_BRACES;
    parse("'a':{'b':'c'}", new Object[]{m, "a", m, "b", "c", M, M, e});
    parse("'a':{'b':'c'}", true, new Object[]{m, "a", m, "b", "c", M, M, e});
    parse("a:'b'", new Object[]{m, "a", "b", M, e});


    flags = JSONParser.FLAGS_DEFAULT;

  }

  @Test
  public void testBareString() throws Exception {
    flags=JSONParser.ALLOW_UNQUOTED_STRING_VALUES | JSONParser.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER;
    String[] strings = new String[] {"t","f","n","a","tru","fals","nul","abc","trueX","falseXY","nullZ","truetrue", "$true", "a.bc.def","a_b-c/d"};

    for (String s : strings) {
      parse(s, new Object[]{s, e});
      parse("[" + s + "]", new Object[]{a, s, A, e});
      parse("[ " + s + ", "+s +" ]", new Object[]{a, s, s, A, e});
      parse("[" + s + ","+s +"]",    new Object[]{a, s, s, A, e});
      parse("\u00a0[\u00a0\r\n\t\u00a0" + s + "\u00a0,\u00a0\u00a0"+s +"\u00a0]\u00a0",    new Object[]{a, s, s, A, e});
    }

    flags |= JSONParser.ALLOW_UNQUOTED_KEYS;
    for (String s : strings) {
      parse("{" + s + ":" + s + "}", new Object[]{m, s, s, M, e});
      parse("{ " + s + " \t\n\r:\t\n\r " + s + "\t\n\r}", new Object[]{m, s, s, M, e});
    }

    parse("{true:true, false:false, null:null}",new Object[]{m,"true",t,"false",f,"null",N,M,e});

    flags=JSONParser.FLAGS_DEFAULT;
  }


  @Test
  public void testAPI() throws IOException {
    JSONParser p = new JSONParser("[1,2]");
    assertEquals(JSONParser.ARRAY_START, p.nextEvent());
    // no nextEvent for the next objects...
    assertEquals(1,p.getLong());
    assertEquals(2,p.getLong());
  }


  @Test
  public void testComments() throws IOException {
    parse("#pre comment\n{//before a\n  'a' /* after a **/ #before separator\n  : /* after separator */ {/*before b*/'b'#after b\n://before c\n'c'/*after c*/}/*after close*/}#post comment no EOL", new Object[]{m,"a",m,"b","c",M,M,e});
  }

  // rfc7159 permits any JSON values to be top level values
  @Test
  public void testTopLevelValues() throws Exception {
    parse("\"\"", new Object[]{""});
    parse("\"hello\"", new Object[]{"hello"});
    parse("true", new Object[]{t});
    parse("false", new Object[]{f});
    parse("null", new Object[]{N});
    parse("42", new Object[]{42L});
    parse("1.414", new Object[]{1.414d});
    parse("/*comment*/1.414/*more comment*/", new Object[]{1.414d});
  }
}
