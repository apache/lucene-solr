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

import java.util.Date;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.ExtendedFieldCache;

/**
 * This is a helper class to construct the trie-based index entries for numerical values.
 * <p>For more information on how the algorithm works, see the package description {@link org.apache.lucene.search.trie}.
 * The format of how the numerical values are stored in index is documented here:
 * <p>All numerical values are first converted to special <code>unsigned long</code>s by applying some bit-wise transformations. This means:<ul>
 * <li>{@link Date}s are casted to UNIX timestamps (milliseconds since 1970-01-01, this is how Java represents date/time
 * internally): {@link Date#getTime()}. The resulting <code>signed long</code> is transformed to the unsigned form like so:</li>
 * <li><code>signed long</code>s are shifted, so that {@link Long#MIN_VALUE} is mapped to <code>0x0000000000000000</code>,
 * {@link Long#MAX_VALUE} is mapped to <code>0xffffffffffffffff</code>.</li>
 * <li><code>double</code>s are converted by getting their IEEE 754 floating-point "double format" bit layout and then some bits
 * are swapped, to be able to compare the result as <code>unsigned long</code>s.</li>
 * </ul>
 * <p>For each variant (you can choose between {@link #VARIANT_8BIT}, {@link #VARIANT_4BIT}, and {@link #VARIANT_2BIT}),
 * the bitmap of this <code>unsigned long</code> is divided into parts of a number of bits (starting with the most-significant bits)
 * and each part converted to characters between {@link #TRIE_CODED_SYMBOL_MIN} and {@link #TRIE_CODED_SYMBOL_MAX}.
 * The resulting {@link String} is comparable like the corresponding <code>unsigned long</code>.
 * <p>To store the different precisions of the long values (from one character [only the most significant one] to the full encoded length),
 * each lower precision is prefixed by the length ({@link #TRIE_CODED_PADDING_START}<code>+precision == 0x20+precision</code>),
 * in an extra "helper" field with a suffixed field name (i.e. fieldname "numeric" =&gt; lower precision's name "numeric#trie").
 * The full long is not prefixed at all and indexed and stored according to the given flags in the original field name.
 * By this it is possible to get the correct enumeration of terms in correct precision
 * of the term list by just jumping to the correct fieldname and/or prefix. The full precision value may also be
 * stored in the document. Having the full precision value as term in a separate field with the original name,
 * sorting of query results against such fields is possible using the original field name.
 */
public final class TrieUtils {

  /** Instance of TrieUtils using a trie factor of 8 bit.
   * This is the <b>recommended<b> one (rather fast and storage optimized) */
  public static final TrieUtils VARIANT_8BIT=new TrieUtils(8);

  /** Instance of TrieUtils using a trie factor of 4 bit. */
  public static final TrieUtils VARIANT_4BIT=new TrieUtils(4);

  /** Instance of TrieUtils using a trie factor of 2 bit.
   * This may be good for some indexes, but it needs much storage space
   * and is not much faster than 8 bit in most cases. */
  public static final TrieUtils VARIANT_2BIT=new TrieUtils(2);

  /** Marker (PADDING)  before lower-precision trie entries to signal the precision value. See class description! */
  public static final char TRIE_CODED_PADDING_START=(char)0x20;
  
  /** The "helper" field containing the lower precision terms is the original fieldname with this appended. */
  public static final String LOWER_PRECISION_FIELD_NAME_SUFFIX="#trie";

  /** Character used as lower end */
  public static final char TRIE_CODED_SYMBOL_MIN=(char)0x100;

  /**
   * A parser instance for filling a {@link ExtendedFieldCache}, that parses trie encoded fields as longs,
   * auto detecting the trie encoding variant using the String length.
   */
  public static final ExtendedFieldCache.LongParser FIELD_CACHE_LONG_PARSER_AUTO=new ExtendedFieldCache.LongParser(){
    public final long parseLong(String val) {
      return trieCodedToLongAuto(val);
    }
  };
  
  /**
   * A parser instance for filling a {@link ExtendedFieldCache}, that parses trie encoded fields as doubles,
   * auto detecting the trie encoding variant using the String length.
   */
  public static final ExtendedFieldCache.DoubleParser FIELD_CACHE_DOUBLE_PARSER_AUTO=new ExtendedFieldCache.DoubleParser(){
    public final double parseDouble(String val) {
      return trieCodedToDoubleAuto(val);
    }
  };
  
  /**
   * Detects and returns the variant of a trie encoded string using the length.
   * @throws NumberFormatException if the length is not 8, 16, or 32 chars.
   */
  public static final TrieUtils autoDetectVariant(final String s) {
    final int l=s.length();
    if (l==VARIANT_8BIT.TRIE_CODED_LENGTH) {
      return VARIANT_8BIT;
    } else if (l==VARIANT_4BIT.TRIE_CODED_LENGTH) {
      return VARIANT_4BIT;
    } else if (l==VARIANT_2BIT.TRIE_CODED_LENGTH) {
      return VARIANT_2BIT;
    } else {
      throw new NumberFormatException("Invalid trie encoded numerical value representation (incompatible length).");
    }
  }

  /**
   * Converts a encoded <code>String</code> value back to a <code>long</code>,
   * auto detecting the trie encoding variant using the String length.
   */
  public static final long trieCodedToLongAuto(final String s) {
    return autoDetectVariant(s).trieCodedToLong(s);
  }

  /**
   * Converts a encoded <code>String</code> value back to a <code>double</code>,
   * auto detecting the trie encoding variant using the String length.
   */
  public static final double trieCodedToDoubleAuto(final String s) {
    return autoDetectVariant(s).trieCodedToDouble(s);
  }

  /**
   * Converts a encoded <code>String</code> value back to a <code>Date</code>,
   * auto detecting the trie encoding variant using the String length.
   */
  public static final Date trieCodedToDateAuto(final String s) {
    return autoDetectVariant(s).trieCodedToDate(s);
  }

  /**
   * A factory method, that generates a {@link SortField} instance for sorting trie encoded values,
   * automatically detecting the trie encoding variant using the String length.
   */
  public static final SortField getSortFieldAuto(final String field) {
    return new SortField(field, FIELD_CACHE_LONG_PARSER_AUTO);
  }
  
  /**
   * A factory method, that generates a {@link SortField} instance for sorting trie encoded values,
   * automatically detecting the trie encoding variant using the String length.
   */
  public static final SortField getSortFieldAuto(final String field, boolean reverse) {
    return new SortField(field, FIELD_CACHE_LONG_PARSER_AUTO, reverse);
  }
  
  // TrieUtils instance's part
  
  private TrieUtils(int bits) {
    assert 64%bits == 0;
    
    // helper variable for conversion
    mask = (1L << bits) - 1L;		

    // init global "constants"
    TRIE_BITS=bits;
    TRIE_CODED_LENGTH=64/TRIE_BITS;
    TRIE_CODED_SYMBOL_MAX=(char)(TRIE_CODED_SYMBOL_MIN+mask);
    TRIE_CODED_NUMERIC_MIN=longToTrieCoded(Long.MIN_VALUE);
    TRIE_CODED_NUMERIC_MAX=longToTrieCoded(Long.MAX_VALUE);
  }

  // internal conversion to/from strings

  private final String internalLongToTrieCoded(long l) {
    final char[] buf=new char[TRIE_CODED_LENGTH];
    for (int i=TRIE_CODED_LENGTH-1; i>=0; i--) {
      buf[i] = (char)( TRIE_CODED_SYMBOL_MIN + (l & mask) );
      l = l >>> TRIE_BITS;
    }
    return new String(buf);
  }

  private final long internalTrieCodedToLong(final String s) {
    if (s==null) throw new NullPointerException("Trie encoded string may not be NULL");
    final int len=s.length();
    if (len!=TRIE_CODED_LENGTH) throw new NumberFormatException(
      "Invalid trie encoded numerical value representation (incompatible length, must be "+TRIE_CODED_LENGTH+")"
    );
    long l=0L;
    for (int i=0; i<len; i++) {
      char ch=s.charAt(i);
      if (ch>=TRIE_CODED_SYMBOL_MIN && ch<=TRIE_CODED_SYMBOL_MAX) {
        l = (l << TRIE_BITS) | (long)(ch-TRIE_CODED_SYMBOL_MIN);
      } else {
        throw new NumberFormatException(
          "Invalid trie encoded numerical value representation (char "+
          Integer.toHexString((int)ch)+" at position "+i+" is invalid)"
        );
      }
    }
    return l;
  }

  // Long's

  /** Converts a <code>long</code> value encoded to a <code>String</code>. */
  public String longToTrieCoded(final long l) {
    return internalLongToTrieCoded(l ^ 0x8000000000000000L);
  }

  /** Converts a encoded <code>String</code> value back to a <code>long</code>. */
  public long trieCodedToLong(final String s) {
    return internalTrieCodedToLong(s) ^ 0x8000000000000000L;
  }

  // Double's

  /** Converts a <code>double</code> value encoded to a <code>String</code>. */
  public String doubleToTrieCoded(final double d) {
    long l=Double.doubleToLongBits(d);
    if ((l & 0x8000000000000000L) == 0L) {
      // >0
      l |= 0x8000000000000000L;
    } else {
      // <0
      l = ~l;
    }
    return internalLongToTrieCoded(l);
  }

  /** Converts a encoded <code>String</code> value back to a <code>double</code>. */
  public double trieCodedToDouble(final String s) {
    long l=internalTrieCodedToLong(s);
    if ((l & 0x8000000000000000L) != 0L) {
      // >0
      l &= 0x7fffffffffffffffL;
    } else {
      // <0
      l = ~l;
    }
    return Double.longBitsToDouble(l);
  }

  // Date's

  /** Converts a <code>Date</code> value encoded to a <code>String</code>. */
  public String dateToTrieCoded(final Date d) {
    return longToTrieCoded(d.getTime());
  }

  /** Converts a encoded <code>String</code> value back to a <code>Date</code>. */
  public Date trieCodedToDate(final String s) {
    return new Date(trieCodedToLong(s));
  }

  // increment / decrement

  /** Increments an encoded String value by 1. Needed by {@link TrieRangeFilter}. */
  public String incrementTrieCoded(final String v) {
    final int l=v.length();
    final char[] buf=new char[l];
    boolean inc=true;
    for (int i=l-1; i>=0; i--) {
      int b=v.charAt(i)-TRIE_CODED_SYMBOL_MIN;
      if (inc) b++;
      if (inc=(b>(int)mask)) b=0;
      buf[i]=(char)(TRIE_CODED_SYMBOL_MIN+b);
    }
    return new String(buf);
  }

  /** Decrements an encoded String value by 1. Needed by {@link TrieRangeFilter}. */
  public String decrementTrieCoded(final String v) {
    final int l=v.length();
    final char[] buf=new char[l];
    boolean dec=true;
    for (int i=l-1; i>=0; i--) {
      int b=v.charAt(i)-TRIE_CODED_SYMBOL_MIN;
      if (dec) b--;
      if (dec=(b<0)) b=(int)mask;
      buf[i]=(char)(TRIE_CODED_SYMBOL_MIN+b);
    }
    return new String(buf);
  }

  private void addConvertedTrieCodedDocumentField(
    final Document ldoc, final String fieldname, final String val,
    final boolean index, final Field.Store store
  ) {
    Field f=new Field(fieldname, val, store, index?Field.Index.NOT_ANALYZED_NO_NORMS:Field.Index.NO);
    if (index) {
      f.setOmitTf(true);
      ldoc.add(f);
      // add the lower precision values in the helper field with prefix
      final StringBuffer sb=new StringBuffer(TRIE_CODED_LENGTH);
      synchronized(sb) {
        for (int i=TRIE_CODED_LENGTH-1; i>0; i--) {
          sb.setLength(0);
          f=new Field(
            fieldname + LOWER_PRECISION_FIELD_NAME_SUFFIX,
            sb.append( (char)(TRIE_CODED_PADDING_START+i) ).append( val.substring(0,i) ).toString(),
            Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS
          );
          f.setOmitTf(true);
          ldoc.add(f);
        }
      }
    } else {
      ldoc.add(f);
    }
  }

  /**
   * Stores a double value in trie-form in document for indexing.
   * <p>To store the different precisions of the long values (from one byte [only the most significant one] to the full eight bytes),
   * each lower precision is prefixed by the length ({@link #TRIE_CODED_PADDING_START}<code>+precision</code>),
   * in an extra "helper" field with a name of <code>fieldname+{@link #LOWER_PRECISION_FIELD_NAME_SUFFIX}</code>
   * (i.e. fieldname "numeric" => lower precision's name "numeric#trie").
   * The full long is not prefixed at all and indexed and stored according to the given flags in the original field name.
   * If the field should not be searchable, set <code>index</code> to <code>false</code>. It is then only stored (for convenience).
   * Fields added to a document using this method can be queried by {@link TrieRangeQuery}. 
   */
  public void addDoubleTrieCodedDocumentField(
    final Document ldoc, final String fieldname, final double val,
    final boolean index, final Field.Store store
  ) {
    addConvertedTrieCodedDocumentField(ldoc, fieldname, doubleToTrieCoded(val), index, store);
  }

  /**
   * Stores a Date value in trie-form in document for indexing.
   * <p>To store the different precisions of the long values (from one byte [only the most significant one] to the full eight bytes),
   * each lower precision is prefixed by the length ({@link #TRIE_CODED_PADDING_START}<code>+precision</code>),
   * in an extra "helper" field with a name of <code>fieldname+{@link #LOWER_PRECISION_FIELD_NAME_SUFFIX}</code>
   * (i.e. fieldname "numeric" => lower precision's name "numeric#trie").
   * The full long is not prefixed at all and indexed and stored according to the given flags in the original field name.
   * If the field should not be searchable, set <code>index</code> to <code>false</code>. It is then only stored (for convenience).
   * Fields added to a document using this method can be queried by {@link TrieRangeQuery}. 
   */
  public void addDateTrieCodedDocumentField(
    final Document ldoc, final String fieldname,
    final Date val, final boolean index, final Field.Store store
  ) {
    addConvertedTrieCodedDocumentField(ldoc, fieldname, dateToTrieCoded(val), index, store);
  }

  /**
   * Stores a long value in trie-form in document for indexing.
   * <p>To store the different precisions of the long values (from one byte [only the most significant one] to the full eight bytes),
   * each lower precision is prefixed by the length ({@link #TRIE_CODED_PADDING_START}<code>+precision</code>),
   * in an extra "helper" field with a name of <code>fieldname+{@link #LOWER_PRECISION_FIELD_NAME_SUFFIX}</code>
   * (i.e. fieldname "numeric" => lower precision's name "numeric#trie").
   * The full long is not prefixed at all and indexed and stored according to the given flags in the original field name.
   * If the field should not be searchable, set <code>index</code> to <code>false</code>. It is then only stored (for convenience).
   * Fields added to a document using this method can be queried by {@link TrieRangeQuery}. 
   */
  public void addLongTrieCodedDocumentField(
    final Document ldoc, final String fieldname,
    final long val, final boolean index, final Field.Store store
  ) {
    addConvertedTrieCodedDocumentField(ldoc, fieldname, longToTrieCoded(val), index, store);
  }
  
  /** A factory method, that generates a {@link SortField} instance for sorting trie encoded values. */
  public SortField getSortField(final String field) {
    return new SortField(field, FIELD_CACHE_LONG_PARSER);
  }
  
  /** A factory method, that generates a {@link SortField} instance for sorting trie encoded values. */
  public SortField getSortField(final String field, boolean reverse) {
    return new SortField(field, FIELD_CACHE_LONG_PARSER, reverse);
  }
  
  /** A parser instance for filling a {@link ExtendedFieldCache}, that parses trie encoded fields as longs. */
  public final ExtendedFieldCache.LongParser FIELD_CACHE_LONG_PARSER=new ExtendedFieldCache.LongParser(){
    public final long parseLong(String val) {
      return trieCodedToLong(val);
    }
  };
  
  /** A parser instance for filling a {@link ExtendedFieldCache}, that parses trie encoded fields as doubles. */
  public final ExtendedFieldCache.DoubleParser FIELD_CACHE_DOUBLE_PARSER=new ExtendedFieldCache.DoubleParser(){
    public final double parseDouble(String val) {
      return trieCodedToDouble(val);
    }
  };
  
  private final long mask;
  
  /** Number of bits used in this trie variant (2, 4, or 8) */
  public final int TRIE_BITS;

  /** Length (in chars) of an encoded value (8, 16, or 32 chars) */
  public final int TRIE_CODED_LENGTH;

  /** Character used as upper end (depends on trie bits, its <code>{@link #TRIE_CODED_SYMBOL_MIN}+2^{@link #TRIE_BITS}-1</code>) */
  public final char TRIE_CODED_SYMBOL_MAX;

  /** minimum encoded value of a numerical index entry: {@link Long#MIN_VALUE} */
  public final String TRIE_CODED_NUMERIC_MIN;

  /** maximum encoded value of a numerical index entry: {@link Long#MAX_VALUE} */
  public final String TRIE_CODED_NUMERIC_MAX;

}
