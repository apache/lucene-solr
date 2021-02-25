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
   * its unicode code points, according to {@link #lookup3ycs(int[] k, int offset, int length, int initval)}
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
   * lookup3 hashlittle2 with initval biased by -(numCodePoints&lt;&lt;2).  It is equivalent
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


  /** Returns the MurmurHash3_x86_32 hash.
   * Original source/tests at https://github.com/yonik/java_util/
   */
  @SuppressWarnings({"fallthrough"})
  public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

    for (int i=offset; i<roundedEnd; i+=4) {
      // little endian load order
      int k1 = (data[i] & 0xff) | ((data[i+1] & 0xff) << 8) | ((data[i+2] & 0xff) << 16) | (data[i+3] << 24);
      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
      k1 *= c2;

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
      h1 = h1*5+0xe6546b64;
    }

    // tail
    int k1 = 0;

    switch(len & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
        // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        // fallthrough
      case 1:
        k1 |= (data[roundedEnd] & 0xff);
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
        k1 *= c2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }



  /** Returns the MurmurHash3_x86_32 hash of the UTF-8 bytes of the String without actually encoding
   * the string to a temporary buffer.  This is more than 2x faster than hashing the result
   * of String.getBytes().
   */
  public static int murmurhash3_x86_32(CharSequence data, int offset, int len, int seed) {

    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;

    int pos = offset;
    int end = offset + len;
    int k1 = 0;
    int k2 = 0;
    int shift = 0;
    int bits = 0;
    int nBytes = 0;   // length in UTF8 bytes


    while (pos < end) {
      int code = data.charAt(pos++);
      if (code < 0x80) {
        k2 = code;
        bits = 8;

        /***
         // optimized ascii implementation (currently slower!!! code size?)
         if (shift == 24) {
         k1 = k1 | (code << 24);

         k1 *= c1;
         k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
         k1 *= c2;

         h1 ^= k1;
         h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
         h1 = h1*5+0xe6546b64;

         shift = 0;
         nBytes += 4;
         k1 = 0;
         } else {
         k1 |= code << shift;
         shift += 8;
         }
         continue;
         ***/

      }
      else if (code < 0x800) {
        k2 = (0xC0 | (code >> 6))
            | ((0x80 | (code & 0x3F)) << 8);
        bits = 16;
      }
      else if (code < 0xD800 || code > 0xDFFF || pos>=end) {
        // we check for pos>=end to encode an unpaired surrogate as 3 bytes.
        k2 = (0xE0 | (code >> 12))
            | ((0x80 | ((code >> 6) & 0x3F)) << 8)
            | ((0x80 | (code & 0x3F)) << 16);
        bits = 24;
      } else {
        // surrogate pair
        // int utf32 = pos < end ? (int) data.charAt(pos++) : 0;
        int utf32 = (int) data.charAt(pos++);
        utf32 = ((code - 0xD7C0) << 10) + (utf32 & 0x3FF);
        k2 = (0xff & (0xF0 | (utf32 >> 18)))
            | ((0x80 | ((utf32 >> 12) & 0x3F))) << 8
            | ((0x80 | ((utf32 >> 6) & 0x3F))) << 16
            |  (0x80 | (utf32 & 0x3F)) << 24;
        bits = 32;
      }


      k1 |= k2 << shift;

      // int used_bits = 32 - shift;  // how many bits of k2 were used in k1.
      // int unused_bits = bits - used_bits; //  (bits-(32-shift)) == bits+shift-32  == bits-newshift

      shift += bits;
      if (shift >= 32) {
        // mix after we have a complete word

        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
        h1 = h1*5+0xe6546b64;

        shift -= 32;
        // unfortunately, java won't let you shift 32 bits off, so we need to check for 0
        if (shift != 0) {
          k1 = k2 >>> (bits-shift);   // bits used == bits - newshift
        } else {
          k1 = 0;
        }
        nBytes += 4;
      }

    } // inner

    // handle tail
    if (shift > 0) {
      nBytes += shift >> 3;
      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
      k1 *= c2;
      h1 ^= k1;
    }

    // finalization
    h1 ^= nBytes;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }


  /** 128 bits of state */
  public static final class LongPair {
    public long val1;
    public long val2;
  }

  public static final int fmix32(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  public static final long fmix64(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }

  /** Gets a long from a byte buffer in little endian byte order. */
  public static final long getLongLittleEndian(byte[] buf, int offset) {
    return     ((long)buf[offset+7]    << 56)   // no mask needed
        | ((buf[offset+6] & 0xffL) << 48)
        | ((buf[offset+5] & 0xffL) << 40)
        | ((buf[offset+4] & 0xffL) << 32)
        | ((buf[offset+3] & 0xffL) << 24)
        | ((buf[offset+2] & 0xffL) << 16)
        | ((buf[offset+1] & 0xffL) << 8)
        | ((buf[offset  ] & 0xffL));        // no shift needed
  }


  /** Returns the MurmurHash3_x64_128 hash, placing the result in "out". */
  @SuppressWarnings({"fallthrough"})
  public static void murmurhash3_x64_128(byte[] key, int offset, int len, int seed, LongPair out) {
    // The original algorithm does have a 32 bit unsigned seed.
    // We have to mask to match the behavior of the unsigned types and prevent sign extension.
    long h1 = seed & 0x00000000FFFFFFFFL;
    long h2 = seed & 0x00000000FFFFFFFFL;

    final long c1 = 0x87c37b91114253d5L;
    final long c2 = 0x4cf5ad432745937fL;

    int roundedEnd = offset + (len & 0xFFFFFFF0);  // round down to 16 byte block
    for (int i=offset; i<roundedEnd; i+=16) {
      long k1 = getLongLittleEndian(key, i);
      long k2 = getLongLittleEndian(key, i+8);
      k1 *= c1; k1  = Long.rotateLeft(k1,31); k1 *= c2; h1 ^= k1;
      h1 = Long.rotateLeft(h1,27); h1 += h2; h1 = h1*5+0x52dce729;
      k2 *= c2; k2  = Long.rotateLeft(k2,33); k2 *= c1; h2 ^= k2;
      h2 = Long.rotateLeft(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
    }

    long k1 = 0;
    long k2 = 0;

    switch (len & 15) {
      case 15: k2  = (key[roundedEnd+14] & 0xffL) << 48;
      case 14: k2 |= (key[roundedEnd+13] & 0xffL) << 40;
      case 13: k2 |= (key[roundedEnd+12] & 0xffL) << 32;
      case 12: k2 |= (key[roundedEnd+11] & 0xffL) << 24;
      case 11: k2 |= (key[roundedEnd+10] & 0xffL) << 16;
      case 10: k2 |= (key[roundedEnd+ 9] & 0xffL) << 8;
      case  9: k2 |= (key[roundedEnd+ 8] & 0xffL);
        k2 *= c2; k2  = Long.rotateLeft(k2, 33); k2 *= c1; h2 ^= k2;
      case  8: k1  = ((long)key[roundedEnd+7]) << 56;
      case  7: k1 |= (key[roundedEnd+6] & 0xffL) << 48;
      case  6: k1 |= (key[roundedEnd+5] & 0xffL) << 40;
      case  5: k1 |= (key[roundedEnd+4] & 0xffL) << 32;
      case  4: k1 |= (key[roundedEnd+3] & 0xffL) << 24;
      case  3: k1 |= (key[roundedEnd+2] & 0xffL) << 16;
      case  2: k1 |= (key[roundedEnd+1] & 0xffL) << 8;
      case  1: k1 |= (key[roundedEnd  ] & 0xffL);
        k1 *= c1; k1  = Long.rotateLeft(k1,31); k1 *= c2; h1 ^= k1;
    }

    //----------
    // finalization

    h1 ^= len; h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    out.val1 = h1;
    out.val2 = h2;
  }

}
