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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.StringHelper;

/**
 * Expert: The default cache implementation, storing all values in memory.
 * A WeakHashMap is used for storage.
 *
 * <p>Created: May 19, 2004 4:40:36 PM
 *
 * @since   lucene 1.4
 * @version $Id$
 */
 // TODO: change interface to FieldCache in 3.0 when removed
class FieldCacheImpl implements ExtendedFieldCache {
	
  private Map caches;
  FieldCacheImpl() {
    init();
  }
  private synchronized void init() {
    caches = new HashMap(7);
    caches.put(Byte.TYPE, new ByteCache(this));
    caches.put(Short.TYPE, new ShortCache(this));
    caches.put(Integer.TYPE, new IntCache(this));
    caches.put(Float.TYPE, new FloatCache(this));
    caches.put(Long.TYPE, new LongCache(this));
    caches.put(Double.TYPE, new DoubleCache(this));
    caches.put(String.class, new StringCache(this));
    caches.put(StringIndex.class, new StringIndexCache(this));
    caches.put(Comparable.class, new CustomCache(this));
    caches.put(Object.class, new AutoCache(this));
  }

  public void purgeAllCaches() {
    init();
  }
  
  public CacheEntry[] getCacheEntries() {
    List result = new ArrayList(17);
    Iterator outerKeys = caches.keySet().iterator();
    while (outerKeys.hasNext()) {
      Class cacheType = (Class)outerKeys.next();
      Cache cache = (Cache)caches.get(cacheType);
      Iterator innerKeys = cache.readerCache.keySet().iterator();
      while (innerKeys.hasNext()) {
        // we've now materialized a hard ref
        Object readerKey = innerKeys.next();
        // innerKeys was backed by WeakHashMap, sanity check
        // that it wasn't GCed before we made hard ref
        if (null != readerKey && cache.readerCache.containsKey(readerKey)) {
          Map innerCache = ((Map)cache.readerCache.get(readerKey));
          Iterator keys = innerCache.keySet().iterator();
          while (keys.hasNext()) {
            Entry entry = (Entry) keys.next();
            result.add(new CacheEntryImpl(readerKey, entry.field,
                                          cacheType, entry.type,
                                          entry.custom, entry.locale,
                                          innerCache.get(entry)));
          }
        }
      }
    }
    return (CacheEntry[]) result.toArray(new CacheEntry[result.size()]);
  }
  
  private static final class CacheEntryImpl extends CacheEntry {
    /** 
     * @deprecated Only needed because of Entry (ab)use by 
     *             FieldSortedHitQueue, remove when FieldSortedHitQueue 
     *             is removed
     */
    private final int sortFieldType;
    /** 
     * @deprecated Only needed because of Entry (ab)use by 
     *             FieldSortedHitQueue, remove when FieldSortedHitQueue 
     *             is removed
     */
    private final Locale locale;

    private final Object readerKey;
    private final String fieldName;
    private final Class cacheType;
    private final Object custom;
    private final Object value;
    CacheEntryImpl(Object readerKey, String fieldName,
                   Class cacheType, int sortFieldType,
                   Object custom, Locale locale, 
                   Object value) {
        this.readerKey = readerKey;
        this.fieldName = fieldName;
        this.cacheType = cacheType;
        this.sortFieldType = sortFieldType;
        this.custom = custom;
        this.locale = locale;
        this.value = value;

        // :HACK: for testing.
//         if (null != locale || SortField.CUSTOM != sortFieldType) {
//           throw new RuntimeException("Locale/sortFieldType: " + this);
//         }

    }
    public Object getReaderKey() { return readerKey; }
    public String getFieldName() { return fieldName; }
    public Class getCacheType() { return cacheType; }
    public Object getCustom() { return custom; }
    public Object getValue() { return value; }
    /** 
     * Adds warning to super.toString if Local or sortFieldType were specified
     * @deprecated Only needed because of Entry (ab)use by 
     *             FieldSortedHitQueue, remove when FieldSortedHitQueue 
     *             is removed
     */
    public String toString() {
      String r = super.toString();
      if (null != locale) {
        r = r + "...!!!Locale:" + locale + "???";
      }
      if (SortField.CUSTOM != sortFieldType) {
        r = r + "...!!!SortType:" + sortFieldType + "???";
      }
      return r;
    }
  }

  /**
   * Hack: When thrown from a Parser (NUMERIC_UTILS_* ones), this stops
   * processing terms and returns the current FieldCache
   * array.
   */
  static final class StopFillCacheException extends RuntimeException {
  }

  /** Expert: Internal cache. */
  abstract static class Cache {
    Cache() {
      this.wrapper = null;
    }

    Cache(FieldCache wrapper) {
      this.wrapper = wrapper;
    }

    final FieldCache wrapper;

    final Map readerCache = new WeakHashMap();
    
    protected abstract Object createValue(IndexReader reader, Entry key)
        throws IOException;

    public Object get(IndexReader reader, Entry key) throws IOException {
      Map innerCache;
      Object value;
      final Object readerKey = reader.getFieldCacheKey();
      synchronized (readerCache) {
        innerCache = (Map) readerCache.get(readerKey);
        if (innerCache == null) {
          innerCache = new HashMap();
          readerCache.put(readerKey, innerCache);
          value = null;
        } else {
          value = innerCache.get(key);
        }
        if (value == null) {
          value = new CreationPlaceholder();
          innerCache.put(key, value);
        }
      }
      if (value instanceof CreationPlaceholder) {
        synchronized (value) {
          CreationPlaceholder progress = (CreationPlaceholder) value;
          if (progress.value == null) {
            progress.value = createValue(reader, key);
            synchronized (readerCache) {
              innerCache.put(key, progress.value);
            }
          }
          return progress.value;
        }
      }
      return value;
    }
  }

  /** Expert: Every composite-key in the internal cache is of this type. */
  static class Entry {
    final String field;        // which Fieldable
    /** 
     * @deprecated Only (ab)used by FieldSortedHitQueue, 
     *             remove when FieldSortedHitQueue is removed
     */
    final int type;            // which SortField type
    final Object custom;       // which custom comparator or parser
    /** 
     * @deprecated Only (ab)used by FieldSortedHitQueue, 
     *             remove when FieldSortedHitQueue is removed
     */
    final Locale locale;       // the locale we're sorting (if string)

    /** 
     * @deprecated Only (ab)used by FieldSortedHitQueue, 
     *             remove when FieldSortedHitQueue is removed
     */
    Entry (String field, int type, Locale locale) {
      this.field = StringHelper.intern(field);
      this.type = type;
      this.custom = null;
      this.locale = locale;
    }

    /** Creates one of these objects for a custom comparator/parser. */
    Entry (String field, Object custom) {
      this.field = StringHelper.intern(field);
      this.type = SortField.CUSTOM;
      this.custom = custom;
      this.locale = null;
    }

    /** 
     * @deprecated Only (ab)used by FieldSortedHitQueue, 
     *             remove when FieldSortedHitQueue is removed
     */
    Entry (String field, int type, Parser parser) {
      this.field = StringHelper.intern(field);
      this.type = type;
      this.custom = parser;
      this.locale = null;
    }

    /** Two of these are equal iff they reference the same field and type. */
    public boolean equals (Object o) {
      if (o instanceof Entry) {
        Entry other = (Entry) o;
        if (other.field == field && other.type == type) {
          if (other.locale == null ? locale == null : other.locale.equals(locale)) {
            if (other.custom == null) {
              if (custom == null) return true;
            } else if (other.custom.equals (custom)) {
              return true;
            }
          }
        }
      }
      return false;
    }

    /** Composes a hashcode based on the field and type. */
    public int hashCode() {
      return field.hashCode() ^ type ^ (custom==null ? 0 : custom.hashCode()) ^ (locale==null ? 0 : locale.hashCode());
    }
  }

  // inherit javadocs
  public byte[] getBytes (IndexReader reader, String field) throws IOException {
    return getBytes(reader, field, null);
  }

  // inherit javadocs
  public byte[] getBytes(IndexReader reader, String field, ByteParser parser)
      throws IOException {
    return (byte[]) ((Cache)caches.get(Byte.TYPE)).get(reader, new Entry(field, parser));
  }

  static final class ByteCache extends Cache {
    ByteCache(FieldCache wrapper) {
      super(wrapper);
    }
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      ByteParser parser = (ByteParser) entry.custom;
      if (parser == null) {
        return wrapper.getBytes(reader, field, FieldCache.DEFAULT_BYTE_PARSER);
      }
      final byte[] retArray = new byte[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          byte termval = parser.parseByte(term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } catch (StopFillCacheException stop) {
      } finally {
        termDocs.close();
        termEnum.close();
      }
      return retArray;
    }
  };
  
  // inherit javadocs
  public short[] getShorts (IndexReader reader, String field) throws IOException {
    return getShorts(reader, field, null);
  }

  // inherit javadocs
  public short[] getShorts(IndexReader reader, String field, ShortParser parser)
      throws IOException {
    return (short[]) ((Cache)caches.get(Short.TYPE)).get(reader, new Entry(field, parser));
  }

  static final class ShortCache extends Cache {
    ShortCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      ShortParser parser = (ShortParser) entry.custom;
      if (parser == null) {
        return wrapper.getShorts(reader, field, FieldCache.DEFAULT_SHORT_PARSER);
      }
      final short[] retArray = new short[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          short termval = parser.parseShort(term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } catch (StopFillCacheException stop) {
      } finally {
        termDocs.close();
        termEnum.close();
      }
      return retArray;
    }
  };
  
  // inherit javadocs
  public int[] getInts (IndexReader reader, String field) throws IOException {
    return getInts(reader, field, null);
  }

  // inherit javadocs
  public int[] getInts(IndexReader reader, String field, IntParser parser)
      throws IOException {
    return (int[]) ((Cache)caches.get(Integer.TYPE)).get(reader, new Entry(field, parser));
  }

  static final class IntCache extends Cache {
    IntCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      IntParser parser = (IntParser) entry.custom;
      if (parser == null) {
        try {
          return wrapper.getInts(reader, field, DEFAULT_INT_PARSER);
        } catch (NumberFormatException ne) {
          return wrapper.getInts(reader, field, NUMERIC_UTILS_INT_PARSER);      
        }
      }
      int[] retArray = null;
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          int termval = parser.parseInt(term.text());
          if (retArray == null) // late init
            retArray = new int[reader.maxDoc()];
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } catch (StopFillCacheException stop) {
      } finally {
        termDocs.close();
        termEnum.close();
      }
      if (retArray == null) // no values
        retArray = new int[reader.maxDoc()];
      return retArray;
    }
  };


  // inherit javadocs
  public float[] getFloats (IndexReader reader, String field)
    throws IOException {
    return getFloats(reader, field, null);
  }

  // inherit javadocs
  public float[] getFloats(IndexReader reader, String field, FloatParser parser)
    throws IOException {

    return (float[]) ((Cache)caches.get(Float.TYPE)).get(reader, new Entry(field, parser));
  }

  static final class FloatCache extends Cache {
    FloatCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      FloatParser parser = (FloatParser) entry.custom;
      if (parser == null) {
        try {
          return wrapper.getFloats(reader, field, DEFAULT_FLOAT_PARSER);
        } catch (NumberFormatException ne) {
          return wrapper.getFloats(reader, field, NUMERIC_UTILS_FLOAT_PARSER);      
        }
    }
      float[] retArray = null;
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          float termval = parser.parseFloat(term.text());
          if (retArray == null) // late init
            retArray = new float[reader.maxDoc()];
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } catch (StopFillCacheException stop) {
      } finally {
        termDocs.close();
        termEnum.close();
      }
      if (retArray == null) // no values
        retArray = new float[reader.maxDoc()];
      return retArray;
    }
  };


  public long[] getLongs(IndexReader reader, String field) throws IOException {
    return getLongs(reader, field, null);
  }

  // inherit javadocs
  public long[] getLongs(IndexReader reader, String field, FieldCache.LongParser parser)
      throws IOException {
    return (long[]) ((Cache)caches.get(Long.TYPE)).get(reader, new Entry(field, parser));
  }

  /** @deprecated Will be removed in 3.0, this is for binary compatibility only */
  public long[] getLongs(IndexReader reader, String field, ExtendedFieldCache.LongParser parser)
      throws IOException {
    return (long[]) ((Cache)caches.get(Long.TYPE)).get(reader, new Entry(field, parser));
  }

  static final class LongCache extends Cache {
    LongCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      FieldCache.LongParser parser = (FieldCache.LongParser) entry.custom;
      if (parser == null) {
        try {
          return wrapper.getLongs(reader, field, DEFAULT_LONG_PARSER);
        } catch (NumberFormatException ne) {
          return wrapper.getLongs(reader, field, NUMERIC_UTILS_LONG_PARSER);      
        }
      }
      long[] retArray = null;
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term(field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          long termval = parser.parseLong(term.text());
          if (retArray == null) // late init
            retArray = new long[reader.maxDoc()];
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } catch (StopFillCacheException stop) {
      } finally {
        termDocs.close();
        termEnum.close();
      }
      if (retArray == null) // no values
        retArray = new long[reader.maxDoc()];
      return retArray;
    }
  };

  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field)
    throws IOException {
    return getDoubles(reader, field, null);
  }

  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field, FieldCache.DoubleParser parser)
      throws IOException {
    return (double[]) ((Cache)caches.get(Double.TYPE)).get(reader, new Entry(field, parser));
  }

  /** @deprecated Will be removed in 3.0, this is for binary compatibility only */
  public double[] getDoubles(IndexReader reader, String field, ExtendedFieldCache.DoubleParser parser)
      throws IOException {
    return (double[]) ((Cache)caches.get(Double.TYPE)).get(reader, new Entry(field, parser));
  }

  static final class DoubleCache extends Cache {
    DoubleCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      FieldCache.DoubleParser parser = (FieldCache.DoubleParser) entry.custom;
      if (parser == null) {
        try {
          return wrapper.getDoubles(reader, field, DEFAULT_DOUBLE_PARSER);
        } catch (NumberFormatException ne) {
          return wrapper.getDoubles(reader, field, NUMERIC_UTILS_DOUBLE_PARSER);      
        }
      }
      double[] retArray = null;
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          double termval = parser.parseDouble(term.text());
          if (retArray == null) // late init
            retArray = new double[reader.maxDoc()];
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } catch (StopFillCacheException stop) {
      } finally {
        termDocs.close();
        termEnum.close();
      }
      if (retArray == null) // no values
        retArray = new double[reader.maxDoc()];
      return retArray;
    }
  };

  // inherit javadocs
  public String[] getStrings(IndexReader reader, String field)
      throws IOException {
    return (String[]) ((Cache)caches.get(String.class)).get(reader, new Entry(field, (Parser)null));
  }

  static final class StringCache extends Cache {
    StringCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      String field = StringHelper.intern((String) entryKey.field);
      final String[] retArray = new String[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          String termval = term.text();
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      return retArray;
    }
  };

  // inherit javadocs
  public StringIndex getStringIndex(IndexReader reader, String field)
      throws IOException {
    return (StringIndex) ((Cache)caches.get(StringIndex.class)).get(reader, new Entry(field, (Parser)null));
  }

  static final class StringIndexCache extends Cache {
    StringIndexCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      String field = StringHelper.intern((String) entryKey.field);
      final int[] retArray = new int[reader.maxDoc()];
      String[] mterms = new String[reader.maxDoc()+1];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      int t = 0;  // current term number

      // an entry for documents that have no terms in this field
      // should a document with no terms be at top or bottom?
      // this puts them at the top - if it is changed, FieldDocSortedHitQueue
      // needs to change as well.
      mterms[t++] = null;

      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;

          // store term text
          // we expect that there is at most one term per document
          if (t >= mterms.length) throw new RuntimeException ("there are more terms than " +
                  "documents in field \"" + field + "\", but it's impossible to sort on " +
                  "tokenized fields");
          mterms[t] = term.text();

          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = t;
          }

          t++;
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }

      if (t == 0) {
        // if there are no terms, make the term array
        // have a single null entry
        mterms = new String[1];
      } else if (t < mterms.length) {
        // if there are less terms than documents,
        // trim off the dead array space
        String[] terms = new String[t];
        System.arraycopy (mterms, 0, terms, 0, t);
        mterms = terms;
      }

      StringIndex value = new StringIndex (retArray, mterms);
      return value;
    }
  };

  /** The pattern used to detect integer values in a field */
  /** removed for java 1.3 compatibility
   protected static final Pattern pIntegers = Pattern.compile ("[0-9\\-]+");
   **/

  /** The pattern used to detect float values in a field */
  /**
   * removed for java 1.3 compatibility
   * protected static final Object pFloats = Pattern.compile ("[0-9+\\-\\.eEfFdD]+");
   */

	// inherit javadocs
  public Object getAuto(IndexReader reader, String field) throws IOException {
    return ((Cache)caches.get(Object.class)).get(reader, new Entry(field, (Parser)null));
  }

  /**
   * @deprecated Please specify the exact type, instead.
   *  Especially, guessing does <b>not</b> work with the new
   *  {@link NumericField} type.
   */
  static final class AutoCache extends Cache {
    AutoCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      String field = StringHelper.intern((String) entryKey.field);
      TermEnum enumerator = reader.terms (new Term (field));
      try {
        Term term = enumerator.term();
        if (term == null) {
          throw new RuntimeException ("no terms in field " + field + " - cannot determine type");
        }
        Object ret = null;
        if (term.field() == field) {
          String termtext = term.text().trim();

          try {
            Integer.parseInt (termtext);
            ret = wrapper.getInts (reader, field);
          } catch (NumberFormatException nfe1) {
            try {
              Long.parseLong(termtext);
              ret = wrapper.getLongs (reader, field);
            } catch (NumberFormatException nfe2) {
              try {
                Float.parseFloat (termtext);
                ret = wrapper.getFloats (reader, field);
              } catch (NumberFormatException nfe3) {
                ret = wrapper.getStringIndex (reader, field);
              }
            }
          }          
        } else {
          throw new RuntimeException ("field \"" + field + "\" does not appear to be indexed");
        }
        return ret;
      } finally {
        enumerator.close();
      }
    }
  };

  /** @deprecated */
  public Comparable[] getCustom(IndexReader reader, String field,
      SortComparator comparator) throws IOException {
    return (Comparable[]) ((Cache)caches.get(Comparable.class)).get(reader, new Entry(field, comparator));
  }

  /** @deprecated */
  static final class CustomCache extends Cache {
    CustomCache(FieldCache wrapper) {
      super(wrapper);
    }

    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = (Entry) entryKey;
      String field = entry.field;
      SortComparator comparator = (SortComparator) entry.custom;
      final Comparable[] retArray = new Comparable[reader.maxDoc()];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field));
      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;
          Comparable termval = comparator.getComparable (term.text());
          termDocs.seek (termEnum);
          while (termDocs.next()) {
            retArray[termDocs.doc()] = termval;
          }
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }
      return retArray;
    }
  };
  
}

