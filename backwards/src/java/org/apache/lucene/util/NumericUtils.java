package org.apache.lucene.util;

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

import org.apache.lucene.analysis.NumericTokenStream; // for javadocs
import org.apache.lucene.document.NumericField; // for javadocs
import org.apache.lucene.search.NumericRangeQuery; // for javadocs
import org.apache.lucene.search.NumericRangeFilter; // for javadocs

/**
 * This is a helper class to generate prefix-encoded representations for numerical values
 * and supplies converters to represent float/double values as sortable integers/longs.
 *
 * <p>To quickly execute range queries in Apache Lucene, a range is divided recursively
 * into multiple intervals for searching: The center of the range is searched only with
 * the lowest possible precision in the trie, while the boundaries are matched
 * more exactly. This reduces the number of terms dramatically.
 *
 * <p>This class generates terms to achieve this: First the numerical integer values need to
 * be converted to strings. For that integer values (32 bit or 64 bit) are made unsigned
 * and the bits are converted to ASCII chars with each 7 bit. The resulting string is
 * sortable like the original integer value. Each value is also prefixed
 * (in the first char) by the <code>shift</code> value (number of bits removed) used
 * during encoding.
 *
 * <p>To also index floating point numbers, this class supplies two methods to convert them
 * to integer values by changing their bit layout: {@link #doubleToSortableLong},
 * {@link #floatToSortableInt}. You will have no precision loss by
 * converting floating point numbers to integers and back (only that the integer form
 * is not usable). Other data types like dates can easily converted to longs or ints (e.g.
 * date to long: {@link java.util.Date#getTime}).
 *
 * <p>For easy usage, the trie algorithm is implemented for indexing inside
 * {@link NumericTokenStream} that can index <code>int</code>, <code>long</code>,
 * <code>float</code>, and <code>double</code>. For querying,
 * {@link NumericRangeQuery} and {@link NumericRangeFilter} implement the query part
 * for the same data types.
 *
 * <p>This class can also be used, to generate lexicographically sortable (according
 * {@link String#compareTo(String)}) representations of numeric data types for other
 * usages (e.g. sorting).
 *
 * <p><font color="red"><b>NOTE:</b> This API is experimental and
 * might change in incompatible ways in the next release.</font>
 *
 * @since 2.9
 */
public final class NumericUtils {

  private NumericUtils() {} // no instance!
  
  /**
   * The default precision step used by {@link NumericField}, {@link NumericTokenStream},
   * {@link NumericRangeQuery}, and {@link NumericRangeFilter} as default
   */
  public static final int PRECISION_STEP_DEFAULT = 4;
  
  /**
   * Expert: Longs are stored at lower precision by shifting off lower bits. The shift count is
   * stored as <code>SHIFT_START_LONG+shift</code> in the first character
   */
  public static final char SHIFT_START_LONG = (char)0x20;

  /**
   * Expert: The maximum term length (used for <code>char[]</code> buffer size)
   * for encoding <code>long</code> values.
   * @see #longToPrefixCoded(long,int,char[])
   */
  public static final int BUF_SIZE_LONG = 63/7 + 2;

  /**
   * Expert: Integers are stored at lower precision by shifting off lower bits. The shift count is
   * stored as <code>SHIFT_START_INT+shift</code> in the first character
   */
  public static final char SHIFT_START_INT  = (char)0x60;

  /**
   * Expert: The maximum term length (used for <code>char[]</code> buffer size)
   * for encoding <code>int</code> values.
   * @see #intToPrefixCoded(int,int,char[])
   */
  public static final int BUF_SIZE_INT = 31/7 + 2;

  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link NumericTokenStream}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   * @param buffer that will contain the encoded chars, must be at least of {@link #BUF_SIZE_LONG}
   * length
   * @return number of chars written to buffer
   */
  public static int longToPrefixCoded(final long val, final int shift, final char[] buffer) {
    if (shift>63 || shift<0)
      throw new IllegalArgumentException("Illegal shift value, must be 0..63");
    int nChars = (63-shift)/7 + 1, len = nChars+1;
    buffer[0] = (char)(SHIFT_START_LONG + shift);
    long sortableBits = val ^ 0x8000000000000000L;
    sortableBits >>>= shift;
    while (nChars>=1) {
      // Store 7 bits per character for good efficiency when UTF-8 encoding.
      // The whole number is right-justified so that lucene can prefix-encode
      // the terms more efficiently.
      buffer[nChars--] = (char)(sortableBits & 0x7f);
      sortableBits >>>= 7;
    }
    return len;
  }

  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link LongRangeBuilder}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   */
  public static String longToPrefixCoded(final long val, final int shift) {
    final char[] buffer = new char[BUF_SIZE_LONG];
    final int len = longToPrefixCoded(val, shift, buffer);
    return new String(buffer, 0, len);
  }

  /**
   * This is a convenience method, that returns prefix coded bits of a long without
   * reducing the precision. It can be used to store the full precision value as a
   * stored field in index.
   * <p>To decode, use {@link #prefixCodedToLong}.
   */
  public static String longToPrefixCoded(final long val) {
    return longToPrefixCoded(val, 0);
  }
  
  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link NumericTokenStream}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   * @param buffer that will contain the encoded chars, must be at least of {@link #BUF_SIZE_INT}
   * length
   * @return number of chars written to buffer
   */
  public static int intToPrefixCoded(final int val, final int shift, final char[] buffer) {
    if (shift>31 || shift<0)
      throw new IllegalArgumentException("Illegal shift value, must be 0..31");
    int nChars = (31-shift)/7 + 1, len = nChars+1;
    buffer[0] = (char)(SHIFT_START_INT + shift);
    int sortableBits = val ^ 0x80000000;
    sortableBits >>>= shift;
    while (nChars>=1) {
      // Store 7 bits per character for good efficiency when UTF-8 encoding.
      // The whole number is right-justified so that lucene can prefix-encode
      // the terms more efficiently.
      buffer[nChars--] = (char)(sortableBits & 0x7f);
      sortableBits >>>= 7;
    }
    return len;
  }

  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link IntRangeBuilder}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   */
  public static String intToPrefixCoded(final int val, final int shift) {
    final char[] buffer = new char[BUF_SIZE_INT];
    final int len = intToPrefixCoded(val, shift, buffer);
    return new String(buffer, 0, len);
  }

  /**
   * This is a convenience method, that returns prefix coded bits of an int without
   * reducing the precision. It can be used to store the full precision value as a
   * stored field in index.
   * <p>To decode, use {@link #prefixCodedToInt}.
   */
  public static String intToPrefixCoded(final int val) {
    return intToPrefixCoded(val, 0);
  }

  /**
   * Returns a long from prefixCoded characters.
   * Rightmost bits will be zero for lower precision codes.
   * This method can be used to decode e.g. a stored field.
   * @throws NumberFormatException if the supplied string is
   * not correctly prefix encoded.
   * @see #longToPrefixCoded(long)
   */
  public static long prefixCodedToLong(final String prefixCoded) {
    final int shift = prefixCoded.charAt(0)-SHIFT_START_LONG;
    if (shift>63 || shift<0)
      throw new NumberFormatException("Invalid shift value in prefixCoded string (is encoded value really a LONG?)");
    long sortableBits = 0L;
    for (int i=1, len=prefixCoded.length(); i<len; i++) {
      sortableBits <<= 7;
      final char ch = prefixCoded.charAt(i);
      if (ch>0x7f) {
        throw new NumberFormatException(
          "Invalid prefixCoded numerical value representation (char "+
          Integer.toHexString((int)ch)+" at position "+i+" is invalid)"
        );
      }
      sortableBits |= (long)ch;
    }
    return (sortableBits << shift) ^ 0x8000000000000000L;
  }

  /**
   * Returns an int from prefixCoded characters.
   * Rightmost bits will be zero for lower precision codes.
   * This method can be used to decode e.g. a stored field.
   * @throws NumberFormatException if the supplied string is
   * not correctly prefix encoded.
   * @see #intToPrefixCoded(int)
   */
  public static int prefixCodedToInt(final String prefixCoded) {
    final int shift = prefixCoded.charAt(0)-SHIFT_START_INT;
    if (shift>31 || shift<0)
      throw new NumberFormatException("Invalid shift value in prefixCoded string (is encoded value really an INT?)");
    int sortableBits = 0;
    for (int i=1, len=prefixCoded.length(); i<len; i++) {
      sortableBits <<= 7;
      final char ch = prefixCoded.charAt(i);
      if (ch>0x7f) {
        throw new NumberFormatException(
          "Invalid prefixCoded numerical value representation (char "+
          Integer.toHexString((int)ch)+" at position "+i+" is invalid)"
        );
      }
      sortableBits |= (int)ch;
    }
    return (sortableBits << shift) ^ 0x80000000;
  }

  /**
   * Converts a <code>double</code> value to a sortable signed <code>long</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;double format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as long.
   * By this the precision is not reduced, but the value can easily used as a long.
   * @see #sortableLongToDouble
   */
  public static long doubleToSortableLong(double val) {
    long f = Double.doubleToRawLongBits(val);
    if (f<0) f ^= 0x7fffffffffffffffL;
    return f;
  }

  /**
   * Convenience method: this just returns:
   *   longToPrefixCoded(doubleToSortableLong(val))
   */
  public static String doubleToPrefixCoded(double val) {
    return longToPrefixCoded(doubleToSortableLong(val));
  }

  /**
   * Converts a sortable <code>long</code> back to a <code>double</code>.
   * @see #doubleToSortableLong
   */
  public static double sortableLongToDouble(long val) {
    if (val<0) val ^= 0x7fffffffffffffffL;
    return Double.longBitsToDouble(val);
  }

  /**
   * Convenience method: this just returns:
   *    sortableLongToDouble(prefixCodedToLong(val))
   */
  public static double prefixCodedToDouble(String val) {
    return sortableLongToDouble(prefixCodedToLong(val));
  }

  /**
   * Converts a <code>float</code> value to a sortable signed <code>int</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;float format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as int.
   * By this the precision is not reduced, but the value can easily used as an int.
   * @see #sortableIntToFloat
   */
  public static int floatToSortableInt(float val) {
    int f = Float.floatToRawIntBits(val);
    if (f<0) f ^= 0x7fffffff;
    return f;
  }

  /**
   * Convenience method: this just returns:
   *   intToPrefixCoded(floatToSortableInt(val))
   */
  public static String floatToPrefixCoded(float val) {
    return intToPrefixCoded(floatToSortableInt(val));
  }

  /**
   * Converts a sortable <code>int</code> back to a <code>float</code>.
   * @see #floatToSortableInt
   */
  public static float sortableIntToFloat(int val) {
    if (val<0) val ^= 0x7fffffff;
    return Float.intBitsToFloat(val);
  }

  /**
   * Convenience method: this just returns:
   *    sortableIntToFloat(prefixCodedToInt(val))
   */
  public static float prefixCodedToFloat(String val) {
    return sortableIntToFloat(prefixCodedToInt(val));
  }

  /**
   * Expert: Splits a long range recursively.
   * You may implement a builder that adds clauses to a
   * {@link org.apache.lucene.search.BooleanQuery} for each call to its
   * {@link LongRangeBuilder#addRange(String,String)}
   * method.
   * <p>This method is used by {@link NumericRangeQuery}.
   */
  public static void splitLongRange(final LongRangeBuilder builder,
    final int precisionStep,  final long minBound, final long maxBound
  ) {
    splitRange(builder, 64, precisionStep, minBound, maxBound);
  }
  
  /**
   * Expert: Splits an int range recursively.
   * You may implement a builder that adds clauses to a
   * {@link org.apache.lucene.search.BooleanQuery} for each call to its
   * {@link IntRangeBuilder#addRange(String,String)}
   * method.
   * <p>This method is used by {@link NumericRangeQuery}.
   */
  public static void splitIntRange(final IntRangeBuilder builder,
    final int precisionStep,  final int minBound, final int maxBound
  ) {
    splitRange(builder, 32, precisionStep, (long)minBound, (long)maxBound);
  }
  
  /** This helper does the splitting for both 32 and 64 bit. */
  private static void splitRange(
    final Object builder, final int valSize,
    final int precisionStep, long minBound, long maxBound
  ) {
    if (precisionStep < 1)
      throw new IllegalArgumentException("precisionStep must be >=1");
    if (minBound > maxBound) return;
    for (int shift=0; ; shift += precisionStep) {
      // calculate new bounds for inner precision
      final long diff = 1L << (shift+precisionStep),
        mask = ((1L<<precisionStep) - 1L) << shift;
      final boolean
        hasLower = (minBound & mask) != 0L,
        hasUpper = (maxBound & mask) != mask;
      final long
        nextMinBound = (hasLower ? (minBound + diff) : minBound) & ~mask,
        nextMaxBound = (hasUpper ? (maxBound - diff) : maxBound) & ~mask;

      if (shift+precisionStep>=valSize || nextMinBound>nextMaxBound) {
        // We are in the lowest precision or the next precision is not available.
        addRange(builder, valSize, minBound, maxBound, shift);
        // exit the split recursion loop
        break;
      }
      
      if (hasLower)
        addRange(builder, valSize, minBound, minBound | mask, shift);
      if (hasUpper)
        addRange(builder, valSize, maxBound & ~mask, maxBound, shift);
      
      // recurse to next precision
      minBound = nextMinBound;
      maxBound = nextMaxBound;
    }
  }
  
  /** Helper that delegates to correct range builder */
  private static void addRange(
    final Object builder, final int valSize,
    long minBound, long maxBound,
    final int shift
  ) {
    // for the max bound set all lower bits (that were shifted away):
    // this is important for testing or other usages of the splitted range
    // (e.g. to reconstruct the full range). The prefixEncoding will remove
    // the bits anyway, so they do not hurt!
    maxBound |= (1L << shift) - 1L;
    // delegate to correct range builder
    switch(valSize) {
      case 64:
        ((LongRangeBuilder)builder).addRange(minBound, maxBound, shift);
        break;
      case 32:
        ((IntRangeBuilder)builder).addRange((int)minBound, (int)maxBound, shift);
        break;
      default:
        // Should not happen!
        throw new IllegalArgumentException("valSize must be 32 or 64.");
    }
  }

  /**
   * Expert: Callback for {@link #splitLongRange}.
   * You need to overwrite only one of the methods.
   * <p><font color="red"><b>NOTE:</b> This is a very low-level interface,
   * the method signatures may change in later versions.</font>
   */
  public static abstract class LongRangeBuilder {
    
    /**
     * Overwrite this method, if you like to receive the already prefix encoded range bounds.
     * You can directly build classical (inclusive) range queries from them.
     */
    public void addRange(String minPrefixCoded, String maxPrefixCoded) {
      throw new UnsupportedOperationException();
    }
    
    /**
     * Overwrite this method, if you like to receive the raw long range bounds.
     * You can use this for e.g. debugging purposes (print out range bounds).
     */
    public void addRange(final long min, final long max, final int shift) {
      addRange(longToPrefixCoded(min, shift), longToPrefixCoded(max, shift));
    }
  
  }
  
  /**
   * Expert: Callback for {@link #splitIntRange}.
   * You need to overwrite only one of the methods.
   * <p><font color="red"><b>NOTE:</b> This is a very low-level interface,
   * the method signatures may change in later versions.</font>
   */
  public static abstract class IntRangeBuilder {
    
    /**
     * Overwrite this method, if you like to receive the already prefix encoded range bounds.
     * You can directly build classical range (inclusive) queries from them.
     */
    public void addRange(String minPrefixCoded, String maxPrefixCoded) {
      throw new UnsupportedOperationException();
    }
    
    /**
     * Overwrite this method, if you like to receive the raw int range bounds.
     * You can use this for e.g. debugging purposes (print out range bounds).
     */
    public void addRange(final int min, final int max, final int shift) {
      addRange(intToPrefixCoded(min, shift), intToPrefixCoded(max, shift));
    }
  
  }
  
}
