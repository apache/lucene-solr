package org.apache.lucene.search;

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

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;

import org.apache.lucene.analysis.NumericTokenStream; // for javadocs
import org.apache.lucene.document.IntField; // for javadocs
import org.apache.lucene.document.FloatField; // for javadocs
import org.apache.lucene.document.LongField; // for javadocs
import org.apache.lucene.document.DoubleField; // for javadocs
import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Expert: Maintains caches of term values.
 *
 * <p>Created: May 19, 2004 11:13:14 AM
 *
 * @since   lucene 1.4
 * @see org.apache.lucene.util.FieldCacheSanityChecker
 */
public interface FieldCache {

  public static final class CreationPlaceholder {
    Object value;
  }

  /**
   * Hack: When thrown from a Parser (NUMERIC_UTILS_* ones), this stops
   * processing terms and returns the current FieldCache
   * array.
   */
  public static final class StopFillCacheException extends RuntimeException {
  }
  
  /**
   * Marker interface as super-interface to all parsers. It
   * is used to specify a custom parser to {@link
   * SortField#SortField(String, FieldCache.Parser)}.
   */
  public interface Parser {
  }

  /** Interface to parse bytes from document fields.
   * @see FieldCache#getBytes(AtomicReader, String, FieldCache.ByteParser, boolean)
   */
  public interface ByteParser extends Parser {
    /** Return a single Byte representation of this field's value. */
    public byte parseByte(BytesRef term);
  }

  /** Interface to parse shorts from document fields.
   * @see FieldCache#getShorts(AtomicReader, String, FieldCache.ShortParser, boolean)
   */
  public interface ShortParser extends Parser {
    /** Return a short representation of this field's value. */
    public short parseShort(BytesRef term);
  }

  /** Interface to parse ints from document fields.
   * @see FieldCache#getInts(AtomicReader, String, FieldCache.IntParser, boolean)
   */
  public interface IntParser extends Parser {
    /** Return an integer representation of this field's value. */
    public int parseInt(BytesRef term);
  }

  /** Interface to parse floats from document fields.
   * @see FieldCache#getFloats(AtomicReader, String, FieldCache.FloatParser, boolean)
   */
  public interface FloatParser extends Parser {
    /** Return an float representation of this field's value. */
    public float parseFloat(BytesRef term);
  }

  /** Interface to parse long from document fields.
   * @see FieldCache#getLongs(AtomicReader, String, FieldCache.LongParser, boolean)
   */
  public interface LongParser extends Parser {
    /** Return an long representation of this field's value. */
    public long parseLong(BytesRef term);
  }

  /** Interface to parse doubles from document fields.
   * @see FieldCache#getDoubles(AtomicReader, String, FieldCache.DoubleParser, boolean)
   */
  public interface DoubleParser extends Parser {
    /** Return an long representation of this field's value. */
    public double parseDouble(BytesRef term);
  }

  /** Expert: The cache used internally by sorting and range query classes. */
  public static FieldCache DEFAULT = new FieldCacheImpl();

  /** The default parser for byte values, which are encoded by {@link Byte#toString(byte)} */
  public static final ByteParser DEFAULT_BYTE_PARSER = new ByteParser() {
    public byte parseByte(BytesRef term) {
      // TODO: would be far better to directly parse from
      // UTF8 bytes... but really users should use
      // IntField, instead, which already decodes
      // directly from byte[]
      return Byte.parseByte(term.utf8ToString());
    }
    protected Object readResolve() {
      return DEFAULT_BYTE_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".DEFAULT_BYTE_PARSER"; 
    }
  };

  /** The default parser for short values, which are encoded by {@link Short#toString(short)} */
  public static final ShortParser DEFAULT_SHORT_PARSER = new ShortParser() {
    public short parseShort(BytesRef term) {
      // TODO: would be far better to directly parse from
      // UTF8 bytes... but really users should use
      // IntField, instead, which already decodes
      // directly from byte[]
      return Short.parseShort(term.utf8ToString());
    }
    protected Object readResolve() {
      return DEFAULT_SHORT_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".DEFAULT_SHORT_PARSER"; 
    }
  };

  /** The default parser for int values, which are encoded by {@link Integer#toString(int)} */
  public static final IntParser DEFAULT_INT_PARSER = new IntParser() {
    public int parseInt(BytesRef term) {
      // TODO: would be far better to directly parse from
      // UTF8 bytes... but really users should use
      // IntField, instead, which already decodes
      // directly from byte[]
      return Integer.parseInt(term.utf8ToString());
    }
    protected Object readResolve() {
      return DEFAULT_INT_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".DEFAULT_INT_PARSER"; 
    }
  };

  /** The default parser for float values, which are encoded by {@link Float#toString(float)} */
  public static final FloatParser DEFAULT_FLOAT_PARSER = new FloatParser() {
    public float parseFloat(BytesRef term) {
      // TODO: would be far better to directly parse from
      // UTF8 bytes... but really users should use
      // FloatField, instead, which already decodes
      // directly from byte[]
      return Float.parseFloat(term.utf8ToString());
    }
    protected Object readResolve() {
      return DEFAULT_FLOAT_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".DEFAULT_FLOAT_PARSER"; 
    }
  };

  /** The default parser for long values, which are encoded by {@link Long#toString(long)} */
  public static final LongParser DEFAULT_LONG_PARSER = new LongParser() {
    public long parseLong(BytesRef term) {
      // TODO: would be far better to directly parse from
      // UTF8 bytes... but really users should use
      // LongField, instead, which already decodes
      // directly from byte[]
      return Long.parseLong(term.utf8ToString());
    }
    protected Object readResolve() {
      return DEFAULT_LONG_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".DEFAULT_LONG_PARSER"; 
    }
  };

  /** The default parser for double values, which are encoded by {@link Double#toString(double)} */
  public static final DoubleParser DEFAULT_DOUBLE_PARSER = new DoubleParser() {
    public double parseDouble(BytesRef term) {
      // TODO: would be far better to directly parse from
      // UTF8 bytes... but really users should use
      // DoubleField, instead, which already decodes
      // directly from byte[]
      return Double.parseDouble(term.utf8ToString());
    }
    protected Object readResolve() {
      return DEFAULT_DOUBLE_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".DEFAULT_DOUBLE_PARSER"; 
    }
  };

  /**
   * A parser instance for int values encoded by {@link NumericUtils}, e.g. when indexed
   * via {@link IntField}/{@link NumericTokenStream}.
   */
  public static final IntParser NUMERIC_UTILS_INT_PARSER=new IntParser(){
    public int parseInt(BytesRef term) {
      if (NumericUtils.getPrefixCodedIntShift(term) > 0)
        throw new FieldCacheImpl.StopFillCacheException();
      return NumericUtils.prefixCodedToInt(term);
    }
    protected Object readResolve() {
      return NUMERIC_UTILS_INT_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_INT_PARSER"; 
    }
  };

  /**
   * A parser instance for float values encoded with {@link NumericUtils}, e.g. when indexed
   * via {@link FloatField}/{@link NumericTokenStream}.
   */
  public static final FloatParser NUMERIC_UTILS_FLOAT_PARSER=new FloatParser(){
    public float parseFloat(BytesRef term) {
      if (NumericUtils.getPrefixCodedIntShift(term) > 0)
        throw new FieldCacheImpl.StopFillCacheException();
      return NumericUtils.sortableIntToFloat(NumericUtils.prefixCodedToInt(term));
    }
    protected Object readResolve() {
      return NUMERIC_UTILS_FLOAT_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_FLOAT_PARSER"; 
    }
  };

  /**
   * A parser instance for long values encoded by {@link NumericUtils}, e.g. when indexed
   * via {@link LongField}/{@link NumericTokenStream}.
   */
  public static final LongParser NUMERIC_UTILS_LONG_PARSER = new LongParser(){
    public long parseLong(BytesRef term) {
      if (NumericUtils.getPrefixCodedLongShift(term) > 0)
        throw new FieldCacheImpl.StopFillCacheException();
      return NumericUtils.prefixCodedToLong(term);
    }
    protected Object readResolve() {
      return NUMERIC_UTILS_LONG_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_LONG_PARSER"; 
    }
  };

  /**
   * A parser instance for double values encoded with {@link NumericUtils}, e.g. when indexed
   * via {@link DoubleField}/{@link NumericTokenStream}.
   */
  public static final DoubleParser NUMERIC_UTILS_DOUBLE_PARSER = new DoubleParser(){
    public double parseDouble(BytesRef term) {
      if (NumericUtils.getPrefixCodedLongShift(term) > 0)
        throw new FieldCacheImpl.StopFillCacheException();
      return NumericUtils.sortableLongToDouble(NumericUtils.prefixCodedToLong(term));
    }
    protected Object readResolve() {
      return NUMERIC_UTILS_DOUBLE_PARSER;
    }
    @Override
    public String toString() { 
      return FieldCache.class.getName()+".NUMERIC_UTILS_DOUBLE_PARSER"; 
    }
  };
  
 
  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> and returns a bit set at the size of
   * <code>reader.maxDoc()</code>, with turned on bits for each docid that 
   * does have a value for this field.
   */
  public Bits getDocsWithField(AtomicReader reader, String field) 
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as a single byte and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the single byte values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public byte[] getBytes (AtomicReader reader, String field, boolean setDocsWithField)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as bytes and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the bytes.
   * @param parser  Computes byte for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public byte[] getBytes (AtomicReader reader, String field, ByteParser parser, boolean setDocsWithField)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as shorts and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the shorts.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public short[] getShorts (AtomicReader reader, String field, boolean setDocsWithField)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as shorts and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the shorts.
   * @param parser  Computes short for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public short[] getShorts (AtomicReader reader, String field, ShortParser parser, boolean setDocsWithField)
  throws IOException;
  
  /** Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as integers and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the integers.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public int[] getInts (AtomicReader reader, String field, boolean setDocsWithField)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as integers and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the integers.
   * @param parser  Computes integer for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public int[] getInts (AtomicReader reader, String field, IntParser parser, boolean setDocsWithField)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if
   * none is found, reads the terms in <code>field</code> as floats and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the floats.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public float[] getFloats (AtomicReader reader, String field, boolean setDocsWithField)
  throws IOException;

  /** Checks the internal cache for an appropriate entry, and if
   * none is found, reads the terms in <code>field</code> as floats and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   * @param reader  Used to get field values.
   * @param field   Which field contains the floats.
   * @param parser  Computes float for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public float[] getFloats (AtomicReader reader, String field,
                            FloatParser parser, boolean setDocsWithField) throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as longs and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the longs.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws java.io.IOException If any error occurs.
   */
  public long[] getLongs(AtomicReader reader, String field, boolean setDocsWithField)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as longs and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the longs.
   * @param parser Computes integer for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public long[] getLongs(AtomicReader reader, String field, LongParser parser, boolean setDocsWithField)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is
   * found, reads the terms in <code>field</code> as integers and returns an array
   * of size <code>reader.maxDoc()</code> of the value each document
   * has in the given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the doubles.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public double[] getDoubles(AtomicReader reader, String field, boolean setDocsWithField)
          throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found,
   * reads the terms in <code>field</code> as doubles and returns an array of
   * size <code>reader.maxDoc()</code> of the value each document has in the
   * given field.
   *
   * @param reader Used to get field values.
   * @param field  Which field contains the doubles.
   * @param parser Computes integer for string values.
   * @param setDocsWithField  If true then {@link #getDocsWithField} will
   *        also be computed and stored in the FieldCache.
   * @return The values in the given field for each document.
   * @throws IOException If any error occurs.
   */
  public double[] getDoubles(AtomicReader reader, String field, DoubleParser parser, boolean setDocsWithField)
          throws IOException;

  /** Returned by {@link #getTerms} */
  public abstract static class DocTerms {
    /** The BytesRef argument must not be null; the method
     *  returns the same BytesRef, or an empty (length=0)
     *  BytesRef if the doc did not have this field or was
     *  deleted. */
    public abstract BytesRef getTerm(int docID, BytesRef ret);

    /** Returns true if this doc has this field and is not
     *  deleted. */
    public abstract boolean exists(int docID);

    /** Number of documents */
    public abstract int size();
  }

  /** Checks the internal cache for an appropriate entry, and if none
   * is found, reads the term values in <code>field</code>
   * and returns a {@link DocTerms} instance, providing a
   * method to retrieve the term (as a BytesRef) per document.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public DocTerms getTerms (AtomicReader reader, String field)
  throws IOException;

  /** Expert: just like {@link #getTerms(AtomicReader,String)},
   *  but you can specify whether more RAM should be consumed in exchange for
   *  faster lookups (default is "true").  Note that the
   *  first call for a given reader and field "wins",
   *  subsequent calls will share the same cache entry. */
  public DocTerms getTerms (AtomicReader reader, String field, boolean fasterButMoreRAM)
  throws IOException;

  /** Returned by {@link #getTermsIndex} */
  public abstract static class DocTermsIndex {

    public int binarySearchLookup(BytesRef key, BytesRef spare) {
      // this special case is the reason that Arrays.binarySearch() isn't useful.
      if (key == null)
        return 0;
	  
      int low = 1;
      int high = numOrd()-1;

      while (low <= high) {
        int mid = (low + high) >>> 1;
        int cmp = lookup(mid, spare).compareTo(key);

        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return mid; // key found
      }
      return -(low + 1);  // key not found.
    }

    /** The BytesRef argument must not be null; the method
     *  returns the same BytesRef, or an empty (length=0)
     *  BytesRef if this ord is the null ord (0). */
    public abstract BytesRef lookup(int ord, BytesRef reuse);

    /** Convenience method, to lookup the Term for a doc.
     *  If this doc is deleted or did not have this field,
     *  this will return an empty (length=0) BytesRef. */
    public BytesRef getTerm(int docID, BytesRef reuse) {
      return lookup(getOrd(docID), reuse);
    }

    /** Returns sort ord for this document.  Ord 0 is
     *  reserved for docs that are deleted or did not have
     *  this field.  */
    public abstract int getOrd(int docID);

    /** Returns total unique ord count; this includes +1 for
     *  the null ord (always 0). */
    public abstract int numOrd();

    /** Number of documents */
    public abstract int size();

    /** Returns a TermsEnum that can iterate over the values in this index entry */
    public abstract TermsEnum getTermsEnum();

    /** @lucene.internal */
    public abstract PackedInts.Reader getDocToOrd();
  }

  /** Checks the internal cache for an appropriate entry, and if none
   * is found, reads the term values in <code>field</code>
   * and returns a {@link DocTerms} instance, providing a
   * method to retrieve the term (as a BytesRef) per document.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return The values in the given field for each document.
   * @throws IOException  If any error occurs.
   */
  public DocTermsIndex getTermsIndex (AtomicReader reader, String field)
  throws IOException;

  /** Expert: just like {@link
   *  #getTermsIndex(AtomicReader,String)}, but you can specify
   *  whether more RAM should be consumed in exchange for
   *  faster lookups (default is "true").  Note that the
   *  first call for a given reader and field "wins",
   *  subsequent calls will share the same cache entry. */
  public DocTermsIndex getTermsIndex (AtomicReader reader, String field, boolean fasterButMoreRAM)
  throws IOException;

  /**
   * Checks the internal cache for an appropriate entry, and if none is found, reads the term values
   * in <code>field</code> and returns a {@link DocTermOrds} instance, providing a method to retrieve
   * the terms (as ords) per document.
   *
   * @param reader  Used to build a {@link DocTermOrds} instance
   * @param field   Which field contains the strings.
   * @return a {@link DocTermOrds} instance
   * @throws IOException  If any error occurs.
   */
  public DocTermOrds getDocTermOrds(AtomicReader reader, String field) throws IOException;

  /**
   * EXPERT: A unique Identifier/Description for each item in the FieldCache. 
   * Can be useful for logging/debugging.
   * @lucene.experimental
   */
  public static abstract class CacheEntry {
    public abstract Object getReaderKey();
    public abstract String getFieldName();
    public abstract Class<?> getCacheType();
    public abstract Object getCustom();
    public abstract Object getValue();
    private String size = null;
    protected final void setEstimatedSize(String size) {
      this.size = size;
    }

    /** 
     * Computes (and stores) the estimated size of the cache Value 
     * @see #getEstimatedSize
     */
    public void estimateSize() {
      long size = RamUsageEstimator.sizeOf(getValue());
      setEstimatedSize(RamUsageEstimator.humanReadableUnits(size));
    }

    /**
     * The most recently estimated size of the value, null unless 
     * estimateSize has been called.
     */
    public final String getEstimatedSize() {
      return size;
    }
    
    
    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("'").append(getReaderKey()).append("'=>");
      b.append("'").append(getFieldName()).append("',");
      b.append(getCacheType()).append(",").append(getCustom());
      b.append("=>").append(getValue().getClass().getName()).append("#");
      b.append(System.identityHashCode(getValue()));
      
      String s = getEstimatedSize();
      if(null != s) {
        b.append(" (size =~ ").append(s).append(')');
      }

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
  public abstract CacheEntry[] getCacheEntries();

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
  public abstract void purgeAllCaches();

  /**
   * Expert: drops all cache entries associated with this
   * reader.  NOTE: this reader must precisely match the
   * reader that the cache entry is keyed on. If you pass a
   * top-level reader, it usually will have no effect as
   * Lucene now caches at the segment reader level.
   */
  public abstract void purge(AtomicReader r);

  /**
   * If non-null, FieldCacheImpl will warn whenever
   * entries are created that are not sane according to
   * {@link org.apache.lucene.util.FieldCacheSanityChecker}.
   */
  public void setInfoStream(PrintStream stream);

  /** counterpart of {@link #setInfoStream(PrintStream)} */
  public PrintStream getInfoStream();
}
