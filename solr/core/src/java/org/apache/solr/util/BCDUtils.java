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

package org.apache.solr.util;


/**
 *
 */
public class BCDUtils {
  // idiv is expensive...
  // use fixed point math to multiply by 1/10
  // http://www.cs.uiowa.edu/~jones/bcd/divide.html
  private static int div10(int a) { return (a * 0xcccd) >>> 19; }
  private static int mul10(int a) { return (a*10); }
  // private static int mul10(int a) { return ((a<<3)+(a<<1)); }
  // private static int mul10(int a) { return (a+(a<<2))<<1; } // attempt to use LEA instr
  // (imul32 on AMD64 only has a 3 cycle latency in any case)


  // something that won't clash with other base100int
// chars (something >= 100)
  private static final char NEG_CHAR=(char)126;
  // The zero exponent.
// NOTE: for smaller integer representations, this current implementation
// combines sign and exponent into the first char.  sign is negative if
// exponent is less than the zero point (no negative exponents themselves)
  private static final int ZERO_EXPONENT='a';  // 97

  // WARNING: assumption is that this is a legal int...
// no validation is done.  [+-]?digit*
//
// Normalization of zeros *is* done...
//  0004, 004, 04, 4 will all end up being equal
//  0,-0 are normalized to '' (zero length)
//
// The value is written to the output buffer
// from the end to the start.  The return value
// is the start of the Base100 int in the output buffer.
//
// As the output will be smaller than the input, arr and
// out may refer to the same array if desired.
//
  public static int base10toBase100(char[] arr, int start, int end,
                                    char[] out, int outend
                                    )
  {
    int wpos=outend;  // write position
    boolean neg=false;

    while (--end >= start) {
      int val = arr[end];
      if (val=='+') { break; }
      else if (val=='-') { neg=!neg; break; }
      else {
        val = val - '0';
        if (end > start) {
          int val2 = arr[end-1];
          if (val2=='+') { out[--wpos]=(char)val; break; }
          if (val2=='-') { out[--wpos]=(char)val; neg=!neg; break; }
          end--;
          val = val + (val2 - '0')*10;
        }
        out[--wpos] = (char)val;
      }
    }

    // remove leading base100 zeros
    while (wpos<outend && out[wpos]==0) wpos++;

    // check for a zero value
    if (wpos==outend) {
      // if zero, don't add negative sign
    } else if (neg) {
      out[--wpos]=NEG_CHAR;
    }

    return wpos;  // the start of the base100 int
  }

  // Converts a base100 number to base10 character form
// returns number of chars written.
// At least 1 char is always written.
  public static int base100toBase10(char[] arr, int start, int end,
                                    char[] out, int offset)
  {
    int wpos=offset;  // write position
    boolean firstDigit=true;
    for (int i=start; i<end; i++) {
      int val = arr[i];
      if (val== NEG_CHAR) { out[wpos++]='-'; continue; }
      char tens = (char)(val / 10 + '0');
      if (!firstDigit || tens!='0') {  // skip leading 0
        out[wpos++] = (char)(val / 10 + '0');    // tens position
      }
      out[wpos++] = (char)(val % 10 + '0');    // ones position
      firstDigit=false;
    }
    if (firstDigit) out[wpos++]='0';
    return wpos-offset;
  }

  public static String base10toBase100SortableInt(String val) {
    char[] arr = new char[val.length()+1];
    val.getChars(0,val.length(),arr,0);
    int len = base10toBase100SortableInt(arr,0,val.length(),arr,arr.length);
    return new String(arr,arr.length-len,len);
  }

  public static String base100SortableIntToBase10(String val) {
    int slen = val.length();
    char[] arr = new char[slen<<2];
    val.getChars(0,slen,arr,0);
    int len = base100SortableIntToBase10(arr,0,slen,arr,slen);
    return new String(arr,slen,len);
  }

  public static String base10toBase10kSortableInt(String val) {
    char[] arr = new char[val.length()+1];
    val.getChars(0,val.length(),arr,0);
    int len = base10toBase10kSortableInt(arr,0,val.length(),arr,arr.length);
    return new String(arr,arr.length-len,len);
  }

  public static String base10kSortableIntToBase10(String val) {
    int slen = val.length();
    char[] arr = new char[slen*5]; // +1 time for orig, +4 for new
    val.getChars(0,slen,arr,0);
    int len = base10kSortableIntToBase10(arr,0,slen,arr,slen);
    return new String(arr,slen,len);
  }

  /********* FUTURE
    // the zero exponent... exponents above this point are positive
    // and below are negative.
    // It is desirable to make ordinary numbers have a single byte
    // exponent when converted to UTF-8
    // For integers, the exponent will always be >=0, but this format
    // is meant to be valid for floating point numbers as well...
    private static final int ZERO_EXPONENT='a';  // 97

    // if exponent is larger than what can be represented
    // in a single byte (char), then this is the multibyte
    // escape char.
    // UCS-2 surrogates start at 0xD800
    private static final int POSITIVE_EXPONENT_ESCAPE=0x3fff;

    // if exponent is smaller than what can be represented in
    // a single byte, then this is the multibyte escape
    private static final int NEGATIVE_EXPONENT_ESCAPE=1;

    // if number is negative, it starts with this optional value
    // this should not overlap with any exponent values
    private static final int NEGATIVE_SIGN=0;
  **********/

    // WARNING: assumption is that this is a legal int...
    // no validation is done.  [+-]?digit*
    //
    // Normalization of zeros *is* done...
    //  0004, 004, 04, 4 will all end up being equal
    //  0,-0 are normalized to '' (zero length)
    //
    // The value is written to the output buffer
    // from the end to the start.  The return value
    // is the start of the Base100 int in the output buffer.
    //
    // As the output will be smaller than the input, arr and
    // out may refer to the same array if desired.
    //
    public static int base10toBase100SortableInt(char[] arr, int start, int end,
                                                 char[] out, int outend
                                      )
    {
      int wpos=outend;  // write position
      boolean neg=false;
      --end;  // position end pointer *on* the last char

      // read signs and leading zeros
      while (start <= end) {
        char val = arr[start];
        if (val=='-') neg=!neg;
        else if (val>='1' && val<='9') break;
        start++;
      }

      // eat whitespace on RHS?
      outer: while (start <= end) {
        switch(arr[end]) {
          case ' ':
          case '\t':
          case '\n':
          case '\r': end--; break;
          default: break outer;
        }
      }

      int hundreds=0;
      /******************************************************
       * remove RHS zero normalization since it only helps 1 in 100
       * numbers and complicates both encoding and decoding.

      // remove pairs of zeros on the RHS and keep track of
      // the count.
      while (start <= end) {
        char val = arr[end];

        if (val=='0' && start <= end) {
          val=arr[end-1];
          if (val=='0') {
            hundreds++;
            end-=2;
            continue;
          }
        }

        break;
      }
      *************************************************************/


      // now start at the end and work our way forward
      // encoding two base 10 digits into 1 base 100 digit
      while (start <= end) {
        int val = arr[end--];
        val = val - '0';
        if (start <= end) {
          int val2 = arr[end--];
          val = val + (val2 - '0')*10;
        }
        out[--wpos] = neg ? (char)(99-val) : (char)val;
      }

      /****** FUTURE: not needed for this implementation of exponent combined with sign
      // normalize all zeros to positive values
      if (wpos==outend) neg=false;
      ******/

      // adjust exponent by the number of base 100 chars written
      hundreds += outend - wpos;

      // write the exponent and sign combined
      out[--wpos] = neg ? (char)(ZERO_EXPONENT - hundreds) : (char)(ZERO_EXPONENT + hundreds);

      return outend-wpos;  // the length of the base100 int
    }

  // Converts a base100 sortable number to base10 character form
// returns number of chars written.
// At least 1 char is always written.
  public static int base100SortableIntToBase10(char[] arr, int start, int end,
                                               char[] out, int offset)
  {
    // Take care of "0" case first.  It's the only number that is represented
    // in one char.
    if (end-start == 1) {
      out[offset]='0';
      return 1;
    }

    int wpos = offset;  // write position
    boolean neg = false;
    int exp = arr[start++];
    if (exp < ZERO_EXPONENT) {
      neg=true;
      exp = ZERO_EXPONENT - exp;
      out[wpos++]='-';
    }

    boolean firstDigit=true;
    while (start < end) {
      int val = arr[start++];
      if (neg) val = 99 - val;
      // opt - if we ever want a faster version we can avoid one integer
      // divide by using fixed point math to multiply by 1/10
      // http://www.cs.uiowa.edu/~jones/bcd/divide.html
      // TIP: write a small function in gcc or cl and see what
      // the optimized assemply output looks like (and which is fastest).
      // In C you can specify "unsigned" which gives the compiler more
      // info than the Java compiler has.
      char tens = (char)(val / 10 + '0');
      if (!firstDigit || tens!='0') {  // skip leading 0
        out[wpos++] = tens;      // write tens position
      }
      out[wpos++] = (char)(val % 10 + '0');    // write ones position
      firstDigit=false;
    }

    // OPTIONAL: if trailing zeros were truncated, then this is where
    // we would restore them (compare number of chars read vs exponent)

    return wpos-offset;
  }

  public static int base10toBase10kSortableInt(char[] arr, int start, int end,
                                               char[] out, int outend
                                    )
  {
    int wpos=outend;  // write position
    boolean neg=false;
    --end;  // position end pointer *on* the last char

    // read signs and leading zeros
    while (start <= end) {
      char val = arr[start];
      if (val=='-') neg=!neg;
      else if (val>='1' && val<='9') break;
      start++;
    }

    // eat whitespace on RHS?
    outer: while (start <= end) {
      switch(arr[end]) {
        case ' ': // fallthrough
        case '\t': // fallthrough
        case '\n': // fallthrough
        case '\r': end--; break;
        default: break outer;
      }
    }

    int exp=0;

    /******************************************************
     * remove RHS zero normalization since it only helps 1 in 100
     * numbers and complicates both encoding and decoding.

    // remove pairs of zeros on the RHS and keep track of
    // the count.
    while (start <= end) {
      char val = arr[end];

      if (val=='0' && start <= end) {
        val=arr[end-1];
        if (val=='0') {
          hundreds++;
          end-=2;
          continue;
        }
      }

      break;
    }
    *************************************************************/


    // now start at the end and work our way forward
    // encoding two base 10 digits into 1 base 100 digit
    while (start <= end) {
      int val = arr[end--] - '0';          // ones
      if (start <= end) {
        val += (arr[end--] - '0')*10;      // tens
        if (start <= end) {
          val += (arr[end--] - '0')*100;    // hundreds
          if (start <= end) {
            val += (arr[end--] - '0')*1000;  // thousands
          }
        }
      }
      out[--wpos] = neg ? (char)(9999-val) : (char)val;
    }


    /****** FUTURE: not needed for this implementation of exponent combined with sign
    // normalize all zeros to positive values
    if (wpos==outend) neg=false;
    ******/

    // adjust exponent by the number of base 100 chars written
    exp += outend - wpos;

    // write the exponent and sign combined
    out[--wpos] = neg ? (char)(ZERO_EXPONENT - exp) : (char)(ZERO_EXPONENT + exp);

    return outend-wpos;  // the length of the base100 int
  }

  // Converts a base100 sortable number to base10 character form
// returns number of chars written.
// At least 1 char is always written.
  public static int base10kSortableIntToBase10(char[] arr, int start, int end,
                                               char[] out, int offset)
  {
    // Take care of "0" case first.  It's the only number that is represented
    // in one char since we don't chop trailing zeros.
    if (end-start == 1) {
      out[offset]='0';
      return 1;
    }

    int wpos = offset;  // write position
    boolean neg;
    int exp = arr[start++];
    if (exp < ZERO_EXPONENT) {
      neg=true;
      // We don't currently use exp on decoding...
      // exp = ZERO_EXPONENT - exp;
      out[wpos++]='-';
    } else {
      neg=false;
    }

    // since so many values will fall in one char, pull it
    // out of the loop (esp since the first value must
    // be special-cased to not print leading zeros.
    // integer division is still expensive, so it's best to check
    // if you actually need to do it.
    //
    // TIP: write a small function in gcc or cl and see what
    // the optimized assemply output looks like (and which is fastest).
    // In C you can specify "unsigned" which gives the compiler more
    // info than the Java compiler has.
    int val = arr[start++];
    if (neg) val = 9999 - val;

    /***
    if (val < 10) {
      out[wpos++] = (char)(val + '0');
    } else if (val < 100) {
      out[wpos++] = (char)(val/10 + '0');
      out[wpos++] = (char)(val%10 + '0');
    } else if (val < 1000) {
      out[wpos++] = (char)(val/100 + '0');
      out[wpos++] = (char)((val/10)%10 + '0');
      out[wpos++] = (char)(val%10 + '0');
    } else {
      out[wpos++] = (char)(val/1000 + '0');
      out[wpos++] = (char)((val/100)%10 + '0');
      out[wpos++] = (char)((val/10)%10 + '0');
      out[wpos++] = (char)(val % 10 + '0');
    }
    ***/

    if (val < 10) {
      out[wpos++] = (char)(val + '0');
    } else if (val < 100) {
      int div = div10(val);
      int ones = val - mul10(div); // mod 10
      out[wpos++] = (char)(div + '0');
      out[wpos++] = (char)(ones + '0');
    } else if (val < 1000) {
      int div = div10(val);
      int ones = val - mul10(div); // mod 10
      val=div;
      div = div10(val);
      int tens = val - mul10(div); // mod 10
      out[wpos++] = (char)(div + '0');
      out[wpos++] = (char)(tens + '0');
      out[wpos++] = (char)(ones + '0');
    } else {
      int div = div10(val);
      int ones = val - mul10(div); // mod 10
      val=div;
      div = div10(val);
      int tens = val - mul10(div); // mod 10
      val=div;
      div = div10(val);
      int hundreds = val - mul10(div); // mod 10

      out[wpos++] = (char)(div + '0');
      out[wpos++] = (char)(hundreds + '0');
      out[wpos++] = (char)(tens + '0');
      out[wpos++] = (char)(ones + '0');
    }


    while (start < end) {
      val = arr[start++];
      if (neg) val = 9999 - val;

      int div = div10(val);
      int ones = val - mul10(div); // mod 10
      val=div;
      div = div10(val);
      int tens = val - mul10(div); // mod 10
      val=div;
      div = div10(val);
      int hundreds = val - mul10(div); // mod 10

      /***
      int ones = val % 10;
      val /= 10;
      int tens = val!=0 ? val % 10 : 0;
      val /= 10;
      int hundreds = val!=0 ? val % 10 : 0;
      val /= 10;
      int thousands = val!=0 ? val % 10 : 0;
      ***/

      /***
      int thousands = val>=1000 ? val/1000 : 0;
      int hundreds  = val>=100 ? (val/100)%10 : 0;
      int tens      = val>=10 ? (val/10)%10 : 0;
      int ones      = val % 10;
      ***/

      /***
      int thousands =  val/1000;
      int hundreds  = (val/100)%10;
      int tens      = (val/10)%10;
      int ones      = val % 10;
      ***/

      out[wpos++] = (char)(div + '0');
      out[wpos++] = (char)(hundreds + '0');
      out[wpos++] = (char)(tens + '0');
      out[wpos++] = (char)(ones + '0');
    }

    // OPTIONAL: if trailing zeros were truncated, then this is where
    // we would restore them (compare number of chars read vs exponent)

    return wpos-offset;
  }



}
