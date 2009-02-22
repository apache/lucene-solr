package org.apache.lucene.search.trie;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.ExtendedFieldCache;

/**
 * This is a helper class to construct the trie-based index entries for numerical values.
 * For more information on how the algorithm works, see the
 * {@linkplain org.apache.lucene.search.trie package description}.
 * <h3>The trie format using prefix encoded numerical values</h3>
 * <p>To quickly execute range queries in Apache Lucene, a range is divided recursively
 * into multiple intervals for searching: The center of the range is searched only with
 * the lowest possible precision in the trie, while the boundaries are matched
 * more exactly. This reduces the number of terms dramatically.
 * <p>This class generates terms to achive this: First the numerical integer values need to
 * be converted to strings. For that integer values (32 bit or 64 bit) are made unsigned
 * and the bits are converted to ASCII chars with each 7 bit. The resulting string is
 * sortable like the original integer value.
 * <p>To also index floating point numbers, this class supplies two methods to convert them
 * to integer values by changing their bit layout: {@link #doubleToSortableLong},
 * {@link #floatToSortableInt}. You will have no precision loss by
 * converting floating point numbers to integers and back (only that the integer form does
 * is not usable). Other data types like dates can easily converted to longs or ints (e.g.
 * date to long: {@link java.util.Date#getTime}).
 * <p>To index the different precisions of the long values each encoded value is also reduced
 * by zeroing bits from the right. Each value is also prefixed (in the first char) by the
 * <code>shift</code> value (number of bits removed) used during encoding. This series of
 * different precision values can be indexed into a Lucene {@link Document} using
 * {@link #addIndexedFields(Document,String,String[])}. The default is to index the original
 * precision in the supplied field name and the lower precisions in an additional helper field.
 * Because of this, the full-precision field can also be sorted (using {@link #getLongSortField}
 * or {@link #getIntSortField}).
 * <p>The number of bits removed from the right for each trie entry is called
 * <code>precisionStep</code> in this API. For comparing the different step values, see the
 * {@linkplain org.apache.lucene.search.trie package description}.
 */
public final class TrieUtils {

  private TrieUtils() {} // no instance!

  /**
   * The default &quot;helper&quot; field containing the lower precision terms is the original
   * fieldname with this appended. This suffix is used in
   * {@link #addIndexedFields(Document,String,String[])} and the corresponding c'tor
   * of <code>(Long|Int)TrieRangeFilter</code>.
   */
  public static final String LOWER_PRECISION_FIELD_NAME_SUFFIX="#trie";

  /**
   * Longs are stored at lower precision by shifting off lower bits. The shift count is
   * stored as <code>SHIFT_START_LONG+shift</code> in the first character
   */
  public static final char SHIFT_START_LONG = (char)0x20;

  /**
   * Integers are stored at lower precision by shifting off lower bits. The shift count is
   * stored as <code>SHIFT_START_INT+shift</code> in the first character
   */
  public static final char SHIFT_START_INT  = (char)0x60;

  /**
   * A parser instance for filling a {@link ExtendedFieldCache}, that parses prefix encoded fields as longs.
   */
  public static final ExtendedFieldCache.LongParser FIELD_CACHE_LONG_PARSER=new ExtendedFieldCache.LongParser(){
    public final long parseLong(final String val) {
      return prefixCodedToLong(val);
    }
  };
  
  /**
   * A parser instance for filling a {@link FieldCache}, that parses prefix encoded fields as ints.
   */
  public static final FieldCache.IntParser FIELD_CACHE_INT_PARSER=new FieldCache.IntParser(){
    public final int parseInt(final String val) {
      return prefixCodedToInt(val);
    }
  };

  /**
   * A parser instance for filling a {@link ExtendedFieldCache}, that parses prefix encoded fields as doubles.
   * This uses {@link #sortableLongToDouble} to convert the encoded long to a double.
   */
  public static final ExtendedFieldCache.DoubleParser FIELD_CACHE_DOUBLE_PARSER=new ExtendedFieldCache.DoubleParser(){
    public final double parseDouble(final String val) {
      return sortableLongToDouble(prefixCodedToLong(val));
    }
  };
  
  /**
   * A parser instance for filling a {@link FieldCache}, that parses prefix encoded fields as floats.
   * This uses {@link #sortableIntToFloat} to convert the encoded int to a float.
   */
  public static final FieldCache.FloatParser FIELD_CACHE_FLOAT_PARSER=new FieldCache.FloatParser(){
    public final float parseFloat(final String val) {
      return sortableIntToFloat(prefixCodedToInt(val));
    }
  };

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
   * This is method is used by {@link #trieCodeLong}.
   */
  public static String longToPrefixCoded(final long val, final int shift) {
    if (shift>63 || shift<0)
      throw new IllegalArgumentException("Illegal shift value, must be 0..63");
    int nChars = (63-shift)/7 + 1;
    final char[] arr = new char[nChars+1];
    arr[0] = (char)(SHIFT_START_LONG + shift);
    long sortableBits = val ^ 0x8000000000000000L;
    sortableBits >>>= shift;
    while (nChars>=1) {
      // Store 7 bits per character for good efficiency when UTF-8 encoding.
      // The whole number is right-justified so that lucene can prefix-encode
      // the terms more efficiently.
      arr[nChars--] = (char)(sortableBits & 0x7f);
      sortableBits >>>= 7;
    }
    return new String(arr);
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
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link #trieCodeInt}.
   */
  public static String intToPrefixCoded(final int val, final int shift) {
    if (shift>31 || shift<0)
      throw new IllegalArgumentException("Illegal shift value, must be 0..31");
    int nChars = (31-shift)/7 + 1;
    final char[] arr = new char[nChars+1];
    arr[0] = (char)(SHIFT_START_INT + shift);
    int sortableBits = val ^ 0x80000000;
    sortableBits >>>= shift;
    while (nChars>=1) {
      // Store 7 bits per character for good efficiency when UTF-8 encoding.
      // The whole number is right-justified so that lucene can prefix-encode
      // the terms more efficiently.
      arr[nChars--] = (char)(sortableBits & 0x7f);
      sortableBits >>>= 7;
    }
    return new String(arr);
  }

  /**
   * Returns a long from prefixCoded characters.
   * Rightmost bits will be zero for lower precision codes.
   * This method can be used to decode e.g. a stored field.
   * @see #longToPrefixCoded(long)
   */
  public static long prefixCodedToLong(final String prefixCoded) {
    final int len = prefixCoded.length();
    final int shift = prefixCoded.charAt(0)-SHIFT_START_LONG;
    if (shift>63 || shift<0)
      throw new NumberFormatException("Invalid shift value in prefixCoded string (is encoded value really a LONG?)");
    long sortableBits = 0L;
    for (int i=1; i<len; i++) {
      sortableBits <<= 7;
      final char ch = prefixCoded.charAt(i);
      if (ch>0x7f) {
        throw new NumberFormatException(
          "Invalid prefixCoded numerical value representation (char "+
          Integer.toHexString((int)ch)+" at position "+i+" is invalid)"
        );
      }
      sortableBits |= (long)(ch & 0x7f);
    }
    return (sortableBits << shift) ^ 0x8000000000000000L;
  }

  /**
   * Returns an int from prefixCoded characters.
   * Rightmost bits will be zero for lower precision codes.
   * This method can be used to decode e.g. a stored field.
   * @see #intToPrefixCoded(int)
   */
  public static int prefixCodedToInt(final String prefixCoded) {
    final int len = prefixCoded.length();
    final int shift = prefixCoded.charAt(0)-SHIFT_START_INT;
    if (shift>31 || shift<0)
      throw new NumberFormatException("Invalid shift value in prefixCoded string (is encoded value really an INT?)");
    int sortableBits = 0;
    for (int i=1; i<len; i++) {
      sortableBits <<= 7;
      final char ch = prefixCoded.charAt(i);
      if (ch>0x7f) {
        throw new NumberFormatException(
          "Invalid prefixCoded numerical value representation (char "+
          Integer.toHexString((int)ch)+" at position "+i+" is invalid)"
        );
      }
      sortableBits |= (int)(ch & 0x7f);
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
    long f = Double.doubleToLongBits(val);
    if (f<0) f ^= 0x7fffffffffffffffL;
    return f;
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
   * Converts a <code>float</code> value to a sortable signed <code>int</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;float format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as int.
   * By this the precision is not reduced, but the value can easily used as an int.
   * @see #sortableIntToFloat
   */
  public static int floatToSortableInt(float val) {
    int f = Float.floatToIntBits(val);
    if (f<0) f ^= 0x7fffffff;
    return f;
  }

  /**
   * Converts a sortable <code>int</code> back to a <code>float</code>.
   * @see #floatToSortableInt
   */
  public static float sortableIntToFloat(int val) {
    if (val<0) val ^= 0x7fffffff;
    return Float.intBitsToFloat(val);
  }

  /** A factory method, that generates a {@link SortField} instance for sorting prefix encoded long values. */
  public static SortField getLongSortField(final String field, boolean reverse) {
    return new SortField(field, FIELD_CACHE_LONG_PARSER, reverse);
  }
  
  /** A factory method, that generates a {@link SortField} instance for sorting prefix encoded int values. */
  public static SortField getIntSortField(final String field, boolean reverse) {
    return new SortField(field, FIELD_CACHE_INT_PARSER, reverse);
  }

  /**
   * Returns a sequence of trie coded numbers suitable for {@link LongTrieRangeFilter}.
   * Each successive string in the list has had it's precision reduced by <code>precisionStep</code>.
   * For sorting, index the first full-precision value into a separate field and the
   * remaining values into another field.
   * <p>To achieve this, use {@link #addIndexedFields(Document,String,String[])}.
   */
  public static String[] trieCodeLong(long val, int precisionStep) {
    if (precisionStep<1 || precisionStep>64)
      throw new IllegalArgumentException("precisionStep may only be 1..64");
    String[] arr = new String[63/precisionStep+1];
    int idx = 0;
    for (int shift=0; shift<64; shift+=precisionStep) {
      arr[idx++] = longToPrefixCoded(val, shift);
    }
    return arr;
  }

  /**
   * Returns a sequence of trie coded numbers suitable for {@link IntTrieRangeFilter}.
   * Each successive string in the list has had it's precision reduced by <code>precisionStep</code>.
   * For sorting, index the first full-precision value into a separate field and the
   * remaining values into another field.
   * <p>To achieve this, use {@link #addIndexedFields(Document,String,String[])}.
   */
  public static String[] trieCodeInt(int val, int precisionStep) {
    if (precisionStep<1 || precisionStep>32)
      throw new IllegalArgumentException("precisionStep may only be 1..32");
    String[] arr = new String[31/precisionStep+1];
    int idx = 0;
    for (int shift=0; shift<32; shift+=precisionStep) {
      arr[idx++] = intToPrefixCoded(val, shift);
    }
    return arr;
  }

  /**
   * Indexes the full precision value only in the main field (for sorting), and indexes all other
   * lower precision values in <code>field+LOWER_PRECISION_FIELD_NAME_SUFFIX</code>.
   * <p><b>This is the recommended variant to add trie fields to the index.</b>
   * By this it is possible to sort the field using a <code>SortField</code> instance
   * returned by {@link #getLongSortField} or {@link #getIntSortField}.
   * <p>This method does not store the fields and saves no term frequency or norms
   * (which are normally not needed for trie fields). If you want to additionally store
   * the value, you can use the normal methods of {@link Document} to achive this, just specify
   * <code>Field.Store.YES</code>, <code>Field.Index.NO</code> and the same field name.
   * <p>Examples:
   * <pre>
   *  addIndexedFields(doc, "mydouble", trieCodeLong(doubleToSortableLong(1.414d), 4));
   *  addIndexedFields(doc, "mylong", trieCodeLong(123456L, 4));
   * </pre>
   **/
  public static void addIndexedFields(Document doc, String field, String[] trieCoded) {
    addIndexedFields(doc, new String[]{field, field+LOWER_PRECISION_FIELD_NAME_SUFFIX}, trieCoded);
  }

  /**
   * Expert: Indexes the full precision value only in the main field (for sorting), and indexes all other
   * lower precision values in the <code>lowerPrecision</code> field.
   * If you do not specify the same field name for the main and lower precision one,
   * it is possible to sort the field using a <code>SortField</code> instance
   * returned by {@link #getLongSortField} or {@link #getIntSortField}.
   * <p>This method does not store the fields and saves no term frequency or norms
   * (which are normally not needed for trie fields). If you want to additionally store
   * the value, you can use the normal methods of {@link Document} to achive this, just specify
   * <code>Field.Store.YES</code>, <code>Field.Index.NO</code> and the same main field name.
   * <p>Examples:
   * <pre>
   *  addIndexedFields(doc, "mydouble", "mydoubletrie", trieCodeLong(doubleToSortableLong(1.414d), 4));
   *  addIndexedFields(doc, "mylong", "mylongtrie", trieCodeLong(123456L, 4));
   * </pre>
   * @see #addIndexedFields(Document,String,String[])
   **/
  public static void addIndexedFields(Document doc, String field, String lowerPrecisionField, String[] trieCoded) {
    addIndexedFields(doc, new String[]{field, lowerPrecisionField}, trieCoded);
  }

  /**
   * Expert: Indexes a series of trie coded values into a lucene {@link Document}
   * using the given field names.
   * If the array of field names is shorter than the trie coded one, all trie coded
   * values with higher index get the last field name.
   * <p>This method does not store the fields and saves no term frequency or norms
   * (which are normally not needed for trie fields). If you want to additionally store
   * the value, you can use the normal methods of {@link Document} to achive this, just specify
   * <code>Field.Store.YES</code>, <code>Field.Index.NO</code> and the same main field name.
   **/
  public static void addIndexedFields(Document doc, String[] fields, String[] trieCoded) {
    for (int i=0; i<trieCoded.length; i++) {
      final int fnum = Math.min(fields.length-1, i);
      final Field f = new Field(fields[fnum], trieCoded[i], Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
      f.setOmitTf(true);
      doc.add(f);
    }
  }

  /**
   * Expert: Splits a long range recursively.
   * You may implement a builder that adds clauses to a
   * {@link org.apache.lucene.search.BooleanQuery} for each call to its
   * {@link IntRangeBuilder#addRange(String,String,int)}
   * method.
   * <p>This method is used by {@link LongTrieRangeFilter}.
   */
  public static void splitLongRange(final LongRangeBuilder builder,
    final int precisionStep,  final long minBound, final long maxBound
  ) {
    if (precisionStep<1 || precisionStep>64)
      throw new IllegalArgumentException("precisionStep may only be 1..64");
    splitRange(builder, 64, precisionStep, minBound, maxBound);
  }
  
  /**
   * Expert: Splits an int range recursively.
   * You may implement a builder that adds clauses to a
   * {@link org.apache.lucene.search.BooleanQuery} for each call to its
   * {@link IntRangeBuilder#addRange(String,String,int)}
   * method.
   * <p>This method is used by {@link IntTrieRangeFilter}.
   */
  public static void splitIntRange(final IntRangeBuilder builder,
    final int precisionStep,  final int minBound, final int maxBound
  ) {
    if (precisionStep<1 || precisionStep>32)
      throw new IllegalArgumentException("precisionStep may only be 1..32");
    splitRange(builder, 32, precisionStep, (long)minBound, (long)maxBound);
  }
  
  /** This helper does the splitting for both 32 and 64 bit. */
  private static void splitRange(
    final Object builder, final int valSize,
    final int precisionStep, long minBound, long maxBound
  ) {
    for (int level=0,shift=0;; level++) {
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
        addRange(builder, valSize, minBound, maxBound, shift, level);
        // exit the split recursion loop
        break;
      }
      
      if (hasLower)
        addRange(builder, valSize, minBound, minBound | mask, shift, level);
      if (hasUpper)
        addRange(builder, valSize, maxBound & ~mask, maxBound, shift, level);
      
      // recurse to next precision
      minBound = nextMinBound;
      maxBound = nextMaxBound;
      shift += precisionStep;
    }
  }
  
  /** Helper that delegates to correct range builder */
  private static void addRange(
    final Object builder, final int valSize,
    long minBound, long maxBound,
    final int shift, final int level
  ) {
    // for the max bound set all lower bits (that were shifted away):
    // this is important for testing or other usages of the splitted range
    // (e.g. to reconstruct the full range). The prefixEncoding will remove
    // the bits anyway, so they do not hurt!
    maxBound |= (1L << shift) - 1L;
    // delegate to correct range builder
    switch(valSize) {
      case 64:
        ((LongRangeBuilder)builder).addRange(minBound, maxBound, shift, level);
        break;
      case 32:
        ((IntRangeBuilder)builder).addRange((int)minBound, (int)maxBound, shift, level);
        break;
      default:
        // Should not happen!
        throw new IllegalArgumentException("valSize must be 32 or 64.");
    }
  }

  /**
   * Expert: Callback for {@link #splitLongRange}.
   * You need to overwrite only one of the methods.
   * <p><font color="red">WARNING: This is a very low-level interface,
   * the method signatures may change in later versions.</font>
   */
  public static abstract class LongRangeBuilder {
    
    /**
     * Overwrite this method, if you like to receive the already prefix encoded range bounds.
     * You can directly build classical range queries from them.
     * The level gives the precision level (0 = highest precision) of the encoded values.
     * This parameter could be used as an index to an array of fieldnames like the
     * parameters to {@link #addIndexedFields(Document,String[],String[])} for specifying
     * the field names for each precision:
     * <pre>
     *  String field = fields[Math.min(fields.length-1, level)];
     * </pre>
     */
    public void addRange(String minPrefixCoded, String maxPrefixCoded, int level) {
      throw new UnsupportedOperationException();
    }
    
    /**
     * Overwrite this method, if you like to receive the raw long range bounds.
     * You can use this for e.g. debugging purposes (print out range bounds).
     */
    public void addRange(final long min, final long max, final int shift, final int level) {
      /*System.out.println(Long.toHexString((min^0x8000000000000000L) >>> shift)+".."+
        Long.toHexString((max^0x8000000000000000L) >>> shift));*/
      addRange(longToPrefixCoded(min, shift), longToPrefixCoded(max, shift), level);
    }
  
  }
  
  /**
   * Expert: Callback for {@link #splitIntRange}.
   * You need to overwrite only one of the methods.
   * <p><font color="red">WARNING: This is a very low-level interface,
   * the method signatures may change in later versions.</font>
   */
  public static abstract class IntRangeBuilder {
    
    /**
     * Overwrite this method, if you like to receive the already prefix encoded range bounds.
     * You can directly build classical range queries from them.
     * The level gives the precision level (0 = highest precision) of the encoded values.
     * This parameter could be used as an index to an array of fieldnames like the
     * parameters to {@link #addIndexedFields(Document,String[],String[])} for specifying
     * the field names for each precision:
     * <pre>
     *  String field = fields[Math.min(fields.length-1, level)];
     * </pre>
     */
    public void addRange(String minPrefixCoded, String maxPrefixCoded, int level) {
      throw new UnsupportedOperationException();
    }
    
    /**
     * Overwrite this method, if you like to receive the raw int range bounds.
     * You can use this for e.g. debugging purposes (print out range bounds).
     */
    public void addRange(final int min, final int max, final int shift, final int level) {
      /*System.out.println(Integer.toHexString((min^0x80000000) >>> shift)+".."+
        Integer.toHexString((max^0x80000000) >>> shift));*/
      addRange(intToPrefixCoded(min, shift), intToPrefixCoded(max, shift), level);
    }
  
  }
  
}
