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
package org.apache.lucene.uninverting;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexReader; // javadocs
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LegacyNumericUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Expert: Maintains caches of term values.
 *
 * <p>Created: May 19, 2004 11:13:14 AM
 *
 * @since   lucene 1.4
 * @see FieldCacheSanityChecker
 *
 * @lucene.internal
 */
interface FieldCache {

  /**
   * Placeholder indicating creation of this cache is currently in-progress.
   */
  public static final class CreationPlaceholder implements Accountable {
    Accountable value;

    @Override
    public long ramBytesUsed() {
      // don't call on the in-progress value, might make things angry.
      return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }
  }

  /**
   * interface to all parsers. It is used to parse different numeric types.
   */
  public interface Parser {
    
    /**
     * Pulls a {@link TermsEnum} from the given {@link Terms}. This method allows certain parsers
     * to filter the actual TermsEnum before the field cache is filled.
     * 
     * @param terms the {@link Terms} instance to create the {@link TermsEnum} from.
     * @return a possibly filtered {@link TermsEnum} instance, this method must not return <code>null</code>.
     * @throws IOException if an {@link IOException} occurs
     * @deprecated index with Points instead
     */
    @Deprecated
    public TermsEnum termsEnum(Terms terms) throws IOException;
    
    /** Parse's this field's value */
    public long parseValue(BytesRef term);
  }
  
  /**
   * Base class for points parsers. These parsers do not use the inverted index, but instead
   * uninvert point data.
   * 
   * This abstraction can be cleaned up when Parser.termsEnum is removed.
   */
  public abstract class PointParser implements Parser {
    public final TermsEnum termsEnum(Terms terms) throws IOException {
      throw new UnsupportedOperationException("makes no sense for parsing points");
    }
  }

  /** Expert: The cache used internally by sorting and range query classes. */
  public static FieldCache DEFAULT = new FieldCacheImpl();

  /**
   * A parser instance for int values encoded by {@link org.apache.lucene.util.NumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.IntPoint}.
   */
  public static final Parser INT_POINT_PARSER = new PointParser() {
    @Override
    public long parseValue(BytesRef point) {
      return NumericUtils.sortableBytesToInt(point.bytes, point.offset);
    }
    
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".INT_POINT_PARSER"; 
    }
  };
  
  /**
   * A parser instance for long values encoded by {@link org.apache.lucene.util.NumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.LongPoint}.
   */
  public static final Parser LONG_POINT_PARSER = new PointParser() {
    @Override
    public long parseValue(BytesRef point) {
      return NumericUtils.sortableBytesToLong(point.bytes, point.offset);
    }
    
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".LONG_POINT_PARSER"; 
    }
  };
  
  /**
   * A parser instance for float values encoded by {@link org.apache.lucene.util.NumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.FloatPoint}.
   */
  public static final Parser FLOAT_POINT_PARSER = new PointParser() {
    @Override
    public long parseValue(BytesRef point) {
      return NumericUtils.sortableFloatBits(NumericUtils.sortableBytesToInt(point.bytes, point.offset));
    }
    
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".FLOAT_POINT_PARSER"; 
    }
  };
  
  /**
   * A parser instance for double values encoded by {@link org.apache.lucene.util.NumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.DoublePoint}.
   */
  public static final Parser DOUBLE_POINT_PARSER = new PointParser() {
    @Override
    public long parseValue(BytesRef point) {
      return NumericUtils.sortableDoubleBits(NumericUtils.sortableBytesToLong(point.bytes, point.offset));
    }
    
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".DOUBLE_POINT_PARSER"; 
    }
  };
  
  /**
   * A parser instance for int values encoded by {@link org.apache.lucene.util.LegacyNumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.LegacyIntField}/{@link org.apache.lucene.analysis.LegacyNumericTokenStream}.
   * @deprecated Index with points and use {@link #INT_POINT_PARSER} instead.
   */
  @Deprecated
  public static final Parser LEGACY_INT_PARSER = new Parser() {
    @Override
    public long parseValue(BytesRef term) {
      return LegacyNumericUtils.prefixCodedToInt(term);
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return LegacyNumericUtils.filterPrefixCodedInts(terms.iterator());
    }
    
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".LEGACY_INT_PARSER"; 
    }
  };

  /**
   * A parser instance for float values encoded with {@link org.apache.lucene.util.LegacyNumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.LegacyFloatField}/{@link org.apache.lucene.analysis.LegacyNumericTokenStream}.
   * @deprecated Index with points and use {@link #FLOAT_POINT_PARSER} instead.
   */
  @Deprecated
  public static final Parser LEGACY_FLOAT_PARSER = new Parser() {
    @Override
    public long parseValue(BytesRef term) {
      int val = LegacyNumericUtils.prefixCodedToInt(term);
      if (val<0) val ^= 0x7fffffff;
      return val;
    }
    
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".LEGACY_FLOAT_PARSER"; 
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return LegacyNumericUtils.filterPrefixCodedInts(terms.iterator());
    }
  };

  /**
   * A parser instance for long values encoded by {@link org.apache.lucene.util.LegacyNumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.LegacyLongField}/{@link org.apache.lucene.analysis.LegacyNumericTokenStream}.
   * @deprecated Index with points and use {@link #LONG_POINT_PARSER} instead.
   */
  @Deprecated
  public static final Parser LEGACY_LONG_PARSER = new Parser() {
    @Override
    public long parseValue(BytesRef term) {
      return LegacyNumericUtils.prefixCodedToLong(term);
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".LEGACY_LONG_PARSER"; 
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return LegacyNumericUtils.filterPrefixCodedLongs(terms.iterator());
    }
  };

  /**
   * A parser instance for double values encoded with {@link org.apache.lucene.util.LegacyNumericUtils}, e.g. when indexed
   * via {@link org.apache.lucene.document.LegacyDoubleField}/{@link org.apache.lucene.analysis.LegacyNumericTokenStream}.
   * @deprecated Index with points and use {@link #DOUBLE_POINT_PARSER} instead.
   */
  @Deprecated
  public static final Parser LEGACY_DOUBLE_PARSER = new Parser() {
    @Override
    public long parseValue(BytesRef term) {
      long val = LegacyNumericUtils.prefixCodedToLong(term);
      if (val<0) val ^= 0x7fffffffffffffffL;
      return val;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".LEGACY_DOUBLE_PARSER"; 
    }
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return LegacyNumericUtils.filterPrefixCodedLongs(terms.iterator());
    }
  };
  
  /** Checks the internal cache for an appropriate entry, and if none is found,
   *  reads the terms/points in <code>field</code> and returns a bit set at the size of
   *  <code>reader.maxDoc()</code>, with turned on bits for each docid that 
   *  does have a value for this field.
   *  @param parser May be {@code null} if coming from the inverted index, otherwise
   *                can be a {@link PointParser} to compute from point values.
   */
  public Bits getDocsWithField(LeafReader reader, String field, Parser parser) throws IOException;

  /**
   * Returns a {@link NumericDocValues} over the values found in documents in the given
   * field. If the field was indexed as {@link NumericDocValuesField}, it simply
   * uses {@link org.apache.lucene.index.LeafReader#getNumericDocValues(String)} to read the values.
   * Otherwise, it checks the internal cache for an appropriate entry, and if
   * none is found, reads the terms/points in <code>field</code> as longs and returns
   * an array of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * 
   * @param reader
   *          Used to get field values.
   * @param field
   *          Which field contains the longs.
   * @param parser
   *          Computes long for string values. May be {@code null} if the
   *          requested field was indexed as {@link NumericDocValuesField} or
   *          {@link org.apache.lucene.document.LegacyLongField}.
   * @param setDocsWithField
   *          If true then {@link #getDocsWithField} will also be computed and
   *          stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException
   *           If any error occurs.
   */
  public NumericDocValues getNumerics(LeafReader reader, String field, Parser parser, boolean setDocsWithField) throws IOException;
  
  /** Checks the internal cache for an appropriate entry, and if none
   * is found, reads the term values in <code>field</code>
   * and returns a {@link BinaryDocValues} instance, providing a
   * method to retrieve the term (as a BytesRef) per document.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public BinaryDocValues getTerms(LeafReader reader, String field, boolean setDocsWithField) throws IOException;

  /** Expert: just like {@link #getTerms(org.apache.lucene.index.LeafReader,String,boolean)},
   *  but you can specify whether more RAM should be consumed in exchange for
   *  faster lookups (default is "true").  Note that the
   *  first call for a given reader and field "wins",
   *  subsequent calls will share the same cache entry. */
  public BinaryDocValues getTerms(LeafReader reader, String field, boolean setDocsWithField, float acceptableOverheadRatio) throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none
   * is found, reads the term values in <code>field</code>
   * and returns a {@link SortedDocValues} instance,
   * providing methods to retrieve sort ordinals and terms
   * (as a ByteRef) per document.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public SortedDocValues getTermsIndex(LeafReader reader, String field) throws IOException;

  /** Expert: just like {@link
   *  #getTermsIndex(org.apache.lucene.index.LeafReader,String)}, but you can specify
   *  whether more RAM should be consumed in exchange for
   *  faster lookups (default is "true").  Note that the
   *  first call for a given reader and field "wins",
   *  subsequent calls will share the same cache entry. */
  public SortedDocValues getTermsIndex(LeafReader reader, String field, float acceptableOverheadRatio) throws IOException;

  /** Can be passed to {@link #getDocTermOrds} to filter for 32-bit numeric terms */
  public static final BytesRef INT32_TERM_PREFIX = new BytesRef(new byte[] { LegacyNumericUtils.SHIFT_START_INT });
  /** Can be passed to {@link #getDocTermOrds} to filter for 64-bit numeric terms */
  public static final BytesRef INT64_TERM_PREFIX = new BytesRef(new byte[] { LegacyNumericUtils.SHIFT_START_LONG });
  
  /**
   * Checks the internal cache for an appropriate entry, and if none is found, reads the term values
   * in <code>field</code> and returns a {@link DocTermOrds} instance, providing a method to retrieve
   * the terms (as ords) per document.
   *
   * @param reader  Used to build a {@link DocTermOrds} instance
   * @param field   Which field contains the strings.
   * @param prefix  prefix for a subset of the terms which should be uninverted. Can be null or
   *                {@link #INT32_TERM_PREFIX} or {@link #INT64_TERM_PREFIX}
   *                
   * @return a {@link DocTermOrds} instance
   * @throws IOException  If any error occurs.
   */
  public SortedSetDocValues getDocTermOrds(LeafReader reader, String field, BytesRef prefix) throws IOException;

  /**
   * EXPERT: A unique Identifier/Description for each item in the FieldCache. 
   * Can be useful for logging/debugging.
   * @lucene.experimental
   */
  public final class CacheEntry {

    private final Object readerKey;
    private final String fieldName;
    private final Class<?> cacheType;
    private final Object custom;
    private final Accountable value;

    public CacheEntry(Object readerKey, String fieldName,
                      Class<?> cacheType,
                      Object custom,
                      Accountable value) {
      this.readerKey = readerKey;
      this.fieldName = fieldName;
      this.cacheType = cacheType;
      this.custom = custom;
      this.value = value;
    }

    public Object getReaderKey() {
      return readerKey;
    }

    public String getFieldName() {
      return fieldName;
    }

    public Class<?> getCacheType() {
      return cacheType;
    }

    public Object getCustom() {
      return custom;
    }

    public Object getValue() {
      return value;
    }

    /**
     * The most recently estimated size of the value, null unless 
     * estimateSize has been called.
     */
    public String getEstimatedSize() {
      long bytesUsed = value == null ? 0L : value.ramBytesUsed();
      return RamUsageEstimator.humanReadableUnits(bytesUsed);
    }
    
    @Override
    public String toString() {
      StringBuilder b = new StringBuilder(250);
      b.append("'").append(getReaderKey()).append("'=>");
      b.append("'").append(getFieldName()).append("',");
      b.append(getCacheType()).append(",").append(getCustom());
      b.append("=>").append(getValue().getClass().getName()).append("#");
      b.append(System.identityHashCode(getValue()));
      
      String s = getEstimatedSize();
      b.append(" (size =~ ").append(s).append(')');

      return b.toString();
    }
  }
  
  /**
   * EXPERT: Generates an array of CacheEntry objects representing all items 
   * currently in the FieldCache.
   * <p>
   * NOTE: These CacheEntry objects maintain a strong reference to the 
   * Cached Values.  Maintaining references to a CacheEntry the AtomicIndexReader 
   * associated with it has garbage collected will prevent the Value itself
   * from being garbage collected when the Cache drops the WeakReference.
   * </p>
   * @lucene.experimental
   */
  public CacheEntry[] getCacheEntries();

  /**
   * <p>
   * EXPERT: Instructs the FieldCache to forcibly expunge all entries 
   * from the underlying caches.  This is intended only to be used for 
   * test methods as a way to ensure a known base state of the Cache 
   * (with out needing to rely on GC to free WeakReferences).  
   * It should not be relied on for "Cache maintenance" in general 
   * application code.
   * </p>
   * @lucene.experimental
   */
  public void purgeAllCaches();

  /**
   * Expert: drops all cache entries associated with this
   * reader {@link IndexReader#getCoreCacheKey}.  NOTE: this cache key must
   * precisely match the reader that the cache entry is
   * keyed on. If you pass a top-level reader, it usually
   * will have no effect as Lucene now caches at the segment
   * reader level.
   */
  public void purgeByCacheKey(Object coreCacheKey);

  /**
   * If non-null, FieldCacheImpl will warn whenever
   * entries are created that are not sane according to
   * {@link FieldCacheSanityChecker}.
   */
  public void setInfoStream(PrintStream stream);

  /** counterpart of {@link #setInfoStream(PrintStream)} */
  public PrintStream getInfoStream();
}
