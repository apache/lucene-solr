package org.apache.solr.common.util;

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

/**
 * <p>Fast, well distributed, cross-platform hash functions.
 * </p>
 *
 * <p>Development background: I was surprised to discovered that there isn't a good cross-platform hash function defined for strings. MD5, SHA, FVN, etc, all define hash functions over bytes, meaning that it's under-specified for strings.
 * </p>
 *
 * <p>So I set out to create a standard 32 bit string hash that would be well defined for implementation in all languages, have very high performance, and have very good hash properties such as distribution. After evaluating all the options, I settled on using Bob Jenkins' lookup3 as a base. It's a well studied and very fast hash function, and the hashword variant can work with 32 bits at a time (perfect for hashing unicode code points). It's also even faster on the latest JVMs which can translate pairs of shifts into native rotate instructions.
 * </p>
 * <p>The only problem with using lookup3 hashword is that it includes a length in the initial value. This would suck some performance out since directly hashing a UTF8 or UTF16 string (Java) would require a pre-scan to get the actual number of unicode code points. The solution was to simply remove the length factor, which is equivalent to biasing initVal by -(numCodePoints*4). This slightly modified lookup3 I define as lookup3ycs.
 * </p>
 * <p>So the definition of the cross-platform string hash lookup3ycs is as follows:
 * </p>
 * <p>The hash value of a character sequence (a string) is defined to be the hash of its unicode code points, according to lookup3 hashword, with the initval biased by -(length*4).
 * </p>
 *<p>So by definition
 *</p>
 * <pre>
 * lookup3ycs(k,offset,length,initval) == lookup3(k,offset,length,initval-(length*4))
 *
 * AND
 *
 * lookup3ycs(k,offset,length,initval+(length*4)) == lookup3(k,offset,length,initval)
 * </pre>
 * <p>An obvious advantage of this relationship is that you can use lookup3 if you don't have an implementation of lookup3ycs.
 * </p>
 *
 *
 * @author yonik
 */
public class Hash {
  /**
   * A Java implementation of hashword from lookup3.c by Bob Jenkins
   * (<a href="http://burtleburtle.net/bob/c/lookup3.c">original source</a>).
   *
   * @param k   the key to hash
   * @param offset   offset of the start of the key
   * @param length   length of the key
   * @param initval  initial value to fold into the hash
   * @return  the 32 bit hash code
   */
  @SuppressWarnings("fallthrough")
  public static int lookup3(int[] k, int offset, int length, int initval) {
    int a,b,c;
    a = b = c = 0xdeadbeef + (length<<2) + initval;

    int i=offset;
    while (length > 3)
    {
      a += k[i];
      b += k[i+1];
      c += k[i+2];

      // mix(a,b,c)... Java needs "out" parameters!!!
      // Note: recent JVMs (Sun JDK6) turn pairs of shifts (needed to do a rotate)
      // into real x86 rotate instructions.
      {
        a -= c;  a ^= (c<<4)|(c>>>-4);   c += b;
        b -= a;  b ^= (a<<6)|(a>>>-6);   a += c;
        c -= b;  c ^= (b<<8)|(b>>>-8);   b += a;
        a -= c;  a ^= (c<<16)|(c>>>-16); c += b;
        b -= a;  b ^= (a<<19)|(a>>>-19); a += c;
        c -= b;  c ^= (b<<4)|(b>>>-4);   b += a;
      }

      length -= 3;
      i += 3;
    }

    switch(length) {
      case 3 : c+=k[i+2];  // fall through
      case 2 : b+=k[i+1];  // fall through
      case 1 : a+=k[i+0];  // fall through
        // final(a,b,c);
      {
        c ^= b; c -= (b<<14)|(b>>>-14);
        a ^= c; a -= (c<<11)|(c>>>-11);
        b ^= a; b -= (a<<25)|(a>>>-25);
        c ^= b; c -= (b<<16)|(b>>>-16);
        a ^= c; a -= (c<<4)|(c>>>-4);
        b ^= a; b -= (a<<14)|(a>>>-14);
        c ^= b; c -= (b<<24)|(b>>>-24);
      }
      case 0:
        break;
    }
    return c;
  }


  /**
   * Identical to lookup3, except initval is biased by -(length&lt;&lt;2).
   * This is equivalent to leaving out the length factor in the initial state.
   * {@code lookup3ycs(k,offset,length,initval) == lookup3(k,offset,length,initval-(length<<2))}
   * and
   * {@code lookup3ycs(k,offset,length,initval+(length<<2)) == lookup3(k,offset,length,initval)}
   */
  public static int lookup3ycs(int[] k, int offset, int length, int initval) {
    return lookup3(k, offset, length, initval-(length<<2));
  }


  /**
   * <p>The hash value of a character sequence is defined to be the hash of
   * it's unicode code points, according to {@link #lookup3ycs(int[] k, int offset, int length, int initval)}
   * </p>
   * <p>If you know the number of code points in the {@code CharSequence}, you can
   * generate the same hash as the original lookup3
   * via {@code lookup3ycs(s, start, end, initval+(numCodePoints<<2))}
   */
  public static int lookup3ycs(CharSequence s, int start, int end, int initval) {
    int a,b,c;
    a = b = c = 0xdeadbeef + initval;
    // only difference from lookup3 is that "+ (length<<2)" is missing
    // since we don't know the number of code points to start with,
    // and don't want to have to pre-scan the string to find out.

    int i=start;
    boolean mixed=true;  // have the 3 state variables been adequately mixed?
    for(;;) {
      if (i>= end) break;
      mixed=false;
      char ch;
      ch = s.charAt(i++);
      a += Character.isHighSurrogate(ch) && i< end ? Character.toCodePoint(ch, s.charAt(i++)) : ch;
      if (i>= end) break;
      ch = s.charAt(i++);
      b += Character.isHighSurrogate(ch) && i< end ? Character.toCodePoint(ch, s.charAt(i++)) : ch;
      if (i>= end) break;
      ch = s.charAt(i++);
      c += Character.isHighSurrogate(ch) && i< end ? Character.toCodePoint(ch, s.charAt(i++)) : ch;
      if (i>= end) break;

      // mix(a,b,c)... Java needs "out" parameters!!!
      // Note: recent JVMs (Sun JDK6) turn pairs of shifts (needed to do a rotate)
      // into real x86 rotate instructions.
      {
        a -= c;  a ^= (c<<4)|(c>>>-4);   c += b;
        b -= a;  b ^= (a<<6)|(a>>>-6);   a += c;
        c -= b;  c ^= (b<<8)|(b>>>-8);   b += a;
        a -= c;  a ^= (c<<16)|(c>>>-16); c += b;
        b -= a;  b ^= (a<<19)|(a>>>-19); a += c;
        c -= b;  c ^= (b<<4)|(b>>>-4);   b += a;
      }
      mixed=true;
    }


    if (!mixed) {
      // final(a,b,c)
        c ^= b; c -= (b<<14)|(b>>>-14);
        a ^= c; a -= (c<<11)|(c>>>-11);
        b ^= a; b -= (a<<25)|(a>>>-25);
        c ^= b; c -= (b<<16)|(b>>>-16);
        a ^= c; a -= (c<<4)|(c>>>-4);
        b ^= a; b -= (a<<14)|(a>>>-14);
        c ^= b; c -= (b<<24)|(b>>>-24);
    }

    return c;
  }


  /**<p>This is the 64 bit version of lookup3ycs, corresponding to Bob Jenkin's
   * lookup3 hashlittle2 with initval biased by -(numCodePoints<<2).  It is equivalent
   * to lookup3ycs in that if the high bits of initval==0, then the low bits of the
   * result will be the same as lookup3ycs.
   * </p>
   */
  public static long lookup3ycs64(CharSequence s, int start, int end, long initval) {
    int a,b,c;
    a = b = c = 0xdeadbeef + (int)initval;
    c += (int)(initval>>>32);
    // only difference from lookup3 is that "+ (length<<2)" is missing
    // since we don't know the number of code points to start with,
    // and don't want to have to pre-scan the string to find out.

    int i=start;
    boolean mixed=true;  // have the 3 state variables been adequately mixed?
    for(;;) {
      if (i>= end) break;
      mixed=false;
      char ch;
      ch = s.charAt(i++);
      a += Character.isHighSurrogate(ch) && i< end ? Character.toCodePoint(ch, s.charAt(i++)) : ch;
      if (i>= end) break;
      ch = s.charAt(i++);
      b += Character.isHighSurrogate(ch) && i< end ? Character.toCodePoint(ch, s.charAt(i++)) : ch;
      if (i>= end) break;
      ch = s.charAt(i++);
      c += Character.isHighSurrogate(ch) && i< end ? Character.toCodePoint(ch, s.charAt(i++)) : ch;
      if (i>= end) break;

      // mix(a,b,c)... Java needs "out" parameters!!!
      // Note: recent JVMs (Sun JDK6) turn pairs of shifts (needed to do a rotate)
      // into real x86 rotate instructions.
      {
        a -= c;  a ^= (c<<4)|(c>>>-4);   c += b;
        b -= a;  b ^= (a<<6)|(a>>>-6);   a += c;
        c -= b;  c ^= (b<<8)|(b>>>-8);   b += a;
        a -= c;  a ^= (c<<16)|(c>>>-16); c += b;
        b -= a;  b ^= (a<<19)|(a>>>-19); a += c;
        c -= b;  c ^= (b<<4)|(b>>>-4);   b += a;
      }
      mixed=true;
    }


    if (!mixed) {
      // final(a,b,c)
        c ^= b; c -= (b<<14)|(b>>>-14);
        a ^= c; a -= (c<<11)|(c>>>-11);
        b ^= a; b -= (a<<25)|(a>>>-25);
        c ^= b; c -= (b<<16)|(b>>>-16);
        a ^= c; a -= (c<<4)|(c>>>-4);
        b ^= a; b -= (a<<14)|(a>>>-14);
        c ^= b; c -= (b<<24)|(b>>>-24);
    }

    return c + (((long)b) << 32);
  }

}
