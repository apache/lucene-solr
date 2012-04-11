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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FieldCacheSanityChecker;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Expert: The default cache implementation, storing all values in memory.
 * A WeakHashMap is used for storage.
 *
 * @since   lucene 1.4
 */
class FieldCacheImpl implements FieldCache {
	
  private Map<Class<?>,Cache> caches;
  FieldCacheImpl() {
    init();
  }
  private synchronized void init() {
    caches = new HashMap<Class<?>,Cache>(9);
    caches.put(Byte.TYPE, new ByteCache(this));
    caches.put(Short.TYPE, new ShortCache(this));
    caches.put(Integer.TYPE, new IntCache(this));
    caches.put(Float.TYPE, new FloatCache(this));
    caches.put(Long.TYPE, new LongCache(this));
    caches.put(Double.TYPE, new DoubleCache(this));
    caches.put(DocTerms.class, new DocTermsCache(this));
    caches.put(DocTermsIndex.class, new DocTermsIndexCache(this));
    caches.put(DocTermOrds.class, new DocTermOrdsCache(this));
    caches.put(DocsWithFieldCache.class, new DocsWithFieldCache(this));
  }

  public synchronized void purgeAllCaches() {
    init();
  }

  public synchronized void purge(AtomicReader r) {
    for(Cache c : caches.values()) {
      c.purge(r);
    }
  }
  
  public synchronized CacheEntry[] getCacheEntries() {
    List<CacheEntry> result = new ArrayList<CacheEntry>(17);
    for(final Map.Entry<Class<?>,Cache> cacheEntry: caches.entrySet()) {
      final Cache cache = cacheEntry.getValue();
      final Class<?> cacheType = cacheEntry.getKey();
      synchronized(cache.readerCache) {
        for (final Map.Entry<Object,Map<Entry, Object>> readerCacheEntry : cache.readerCache.entrySet()) {
          final Object readerKey = readerCacheEntry.getKey();
          if (readerKey == null) continue;
          final Map<Entry, Object> innerCache = readerCacheEntry.getValue();
          for (final Map.Entry<Entry, Object> mapEntry : innerCache.entrySet()) {
            Entry entry = mapEntry.getKey();
            result.add(new CacheEntryImpl(readerKey, entry.field,
                                          cacheType, entry.custom,
                                          mapEntry.getValue()));
          }
        }
      }
    }
    return result.toArray(new CacheEntry[result.size()]);
  }
  
  private static final class CacheEntryImpl extends CacheEntry {
    private final Object readerKey;
    private final String fieldName;
    private final Class<?> cacheType;
    private final Object custom;
    private final Object value;
    CacheEntryImpl(Object readerKey, String fieldName,
                   Class<?> cacheType,
                   Object custom,
                   Object value) {
        this.readerKey = readerKey;
        this.fieldName = fieldName;
        this.cacheType = cacheType;
        this.custom = custom;
        this.value = value;

        // :HACK: for testing.
//         if (null != locale || SortField.CUSTOM != sortFieldType) {
//           throw new RuntimeException("Locale/sortFieldType: " + this);
//         }

    }
    @Override
    public Object getReaderKey() { return readerKey; }
    @Override
    public String getFieldName() { return fieldName; }
    @Override
    public Class<?> getCacheType() { return cacheType; }
    @Override
    public Object getCustom() { return custom; }
    @Override
    public Object getValue() { return value; }
  }

  /**
   * Hack: When thrown from a Parser (NUMERIC_UTILS_* ones), this stops
   * processing terms and returns the current FieldCache
   * array.
   */
  static final class StopFillCacheException extends RuntimeException {
  }
  
  // per-segment fieldcaches don't purge until the shared core closes.
  final SegmentReader.CoreClosedListener purgeCore = new SegmentReader.CoreClosedListener() {
    @Override
    public void onClose(SegmentReader owner) {
      FieldCacheImpl.this.purge(owner);
    }
  };

  // composite/SlowMultiReaderWrapper fieldcaches don't purge until composite reader is closed.
  final IndexReader.ReaderClosedListener purgeReader = new IndexReader.ReaderClosedListener() {
    @Override
    public void onClose(IndexReader owner) {
      assert owner instanceof AtomicReader;
      FieldCacheImpl.this.purge((AtomicReader) owner);
    }
  };
  
  private void initReader(AtomicReader reader) {
    if (reader instanceof SegmentReader) {
      ((SegmentReader) reader).addCoreClosedListener(purgeCore);
    } else {
      // we have a slow reader of some sort, try to register a purge event
      // rather than relying on gc:
      Object key = reader.getCoreCacheKey();
      if (key instanceof AtomicReader) {
        ((AtomicReader)key).addReaderClosedListener(purgeReader); 
      } else {
        // last chance
        reader.addReaderClosedListener(purgeReader); 				
      }
    }
  }

  /** Expert: Internal cache. */
  abstract static class Cache {

    Cache(FieldCacheImpl wrapper) {
      this.wrapper = wrapper;
    }

    final FieldCacheImpl wrapper;

    final Map<Object,Map<Entry,Object>> readerCache = new WeakHashMap<Object,Map<Entry,Object>>();
    
    protected abstract Object createValue(AtomicReader reader, Entry key, boolean setDocsWithField)
        throws IOException;

    /** Remove this reader from the cache, if present. */
    public void purge(AtomicReader r) {
      Object readerKey = r.getCoreCacheKey();
      synchronized(readerCache) {
        readerCache.remove(readerKey);
      }
    }

    /** Sets the key to the value for the provided reader;
     *  if the key is already set then this doesn't change it. */
    public void put(AtomicReader reader, Entry key, Object value) {
      final Object readerKey = reader.getCoreCacheKey();
      synchronized (readerCache) {
        Map<Entry,Object> innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          // First time this reader is using FieldCache
          innerCache = new HashMap<Entry,Object>();
          readerCache.put(readerKey, innerCache);
          wrapper.initReader(reader);
        }
        if (innerCache.get(key) == null) {
          innerCache.put(key, value);
        } else {
          // Another thread beat us to it; leave the current
          // value
        }
      }
    }

    public Object get(AtomicReader reader, Entry key, boolean setDocsWithField) throws IOException {
      Map<Entry,Object> innerCache;
      Object value;
      final Object readerKey = reader.getCoreCacheKey();
      synchronized (readerCache) {
        innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          // First time this reader is using FieldCache
          innerCache = new HashMap<Entry,Object>();
          readerCache.put(readerKey, innerCache);
          wrapper.initReader(reader);
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
            progress.value = createValue(reader, key, setDocsWithField);
            synchronized (readerCache) {
              innerCache.put(key, progress.value);
            }

            // Only check if key.custom (the parser) is
            // non-null; else, we check twice for a single
            // call to FieldCache.getXXX
            if (key.custom != null && wrapper != null) {
              final PrintStream infoStream = wrapper.getInfoStream();
              if (infoStream != null) {
                printNewInsanity(infoStream, progress.value);
              }
            }
          }
          return progress.value;
        }
      }
      return value;
    }

    private void printNewInsanity(PrintStream infoStream, Object value) {
      final FieldCacheSanityChecker.Insanity[] insanities = FieldCacheSanityChecker.checkSanity(wrapper);
      for(int i=0;i<insanities.length;i++) {
        final FieldCacheSanityChecker.Insanity insanity = insanities[i];
        final CacheEntry[] entries = insanity.getCacheEntries();
        for(int j=0;j<entries.length;j++) {
          if (entries[j].getValue() == value) {
            // OK this insanity involves our entry
            infoStream.println("WARNING: new FieldCache insanity created\nDetails: " + insanity.toString());
            infoStream.println("\nStack:\n");
            new Throwable().printStackTrace(infoStream);
            break;
          }
        }
      }
    }
  }

  /** Expert: Every composite-key in the internal cache is of this type. */
  static class Entry {
    final String field;        // which Fieldable
    final Object custom;       // which custom comparator or parser

    /** Creates one of these objects for a custom comparator/parser. */
    Entry (String field, Object custom) {
      this.field = field;
      this.custom = custom;
    }

    /** Two of these are equal iff they reference the same field and type. */
    @Override
    public boolean equals (Object o) {
      if (o instanceof Entry) {
        Entry other = (Entry) o;
        if (other.field.equals(field)) {
          if (other.custom == null) {
            if (custom == null) return true;
          } else if (other.custom.equals (custom)) {
            return true;
          }
        }
      }
      return false;
    }

    /** Composes a hashcode based on the field and type. */
    @Override
    public int hashCode() {
      return field.hashCode() ^ (custom==null ? 0 : custom.hashCode());
    }
  }

  // inherit javadocs
  public byte[] getBytes (AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return getBytes(reader, field, null, setDocsWithField);
  }

  // inherit javadocs
  public byte[] getBytes(AtomicReader reader, String field, ByteParser parser, boolean setDocsWithField)
      throws IOException {
    return (byte[]) caches.get(Byte.TYPE).get(reader, new Entry(field, parser), setDocsWithField);
  }

  static final class ByteCache extends Cache {
    ByteCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }
    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField)
        throws IOException {
      String field = entryKey.field;
      ByteParser parser = (ByteParser) entryKey.custom;
      if (parser == null) {
        return wrapper.getBytes(reader, field, FieldCache.DEFAULT_BYTE_PARSER, setDocsWithField);
      }
      final int maxDoc = reader.maxDoc();
      final byte[] retArray = new byte[maxDoc];
      Terms terms = reader.terms(field);
      FixedBitSet docsWithField = null;
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            wrapper.setDocsWithField(reader, field, new Bits.MatchAllBits(maxDoc));
            setDocsWithField = false;
          }
        }
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final byte termval = parser.parseByte(term);
            docs = termsEnum.docs(null, docs, false);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
              if (setDocsWithField) {
                if (docsWithField == null) {
                  // Lazy init
                  docsWithField = new FixedBitSet(maxDoc);
                }
                docsWithField.set(docID);
              }
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }
      if (setDocsWithField) {
        wrapper.setDocsWithField(reader, field, docsWithField);
      }
      return retArray;
    }
  }
  
  // inherit javadocs
  public short[] getShorts (AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return getShorts(reader, field, null, setDocsWithField);
  }

  // inherit javadocs
  public short[] getShorts(AtomicReader reader, String field, ShortParser parser, boolean setDocsWithField)
      throws IOException {
    return (short[]) caches.get(Short.TYPE).get(reader, new Entry(field, parser), setDocsWithField);
  }

  static final class ShortCache extends Cache {
    ShortCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField)
        throws IOException {
      String field = entryKey.field;
      ShortParser parser = (ShortParser) entryKey.custom;
      if (parser == null) {
        return wrapper.getShorts(reader, field, FieldCache.DEFAULT_SHORT_PARSER, setDocsWithField);
      }
      final int maxDoc = reader.maxDoc();
      final short[] retArray = new short[maxDoc];
      Terms terms = reader.terms(field);
      FixedBitSet docsWithField = null;
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            wrapper.setDocsWithField(reader, field, new Bits.MatchAllBits(maxDoc));
            setDocsWithField = false;
          }
        }
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final short termval = parser.parseShort(term);
            docs = termsEnum.docs(null, docs, false);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
              if (setDocsWithField) {
                if (docsWithField == null) {
                  // Lazy init
                  docsWithField = new FixedBitSet(maxDoc);
                }
                docsWithField.set(docID);
              }
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }
      if (setDocsWithField) {
        wrapper.setDocsWithField(reader, field, docsWithField);
      }
      return retArray;
    }
  }

  // null Bits means no docs matched
  void setDocsWithField(AtomicReader reader, String field, Bits docsWithField) {
    final int maxDoc = reader.maxDoc();
    final Bits bits;
    if (docsWithField == null) {
      bits = new Bits.MatchNoBits(maxDoc);
    } else if (docsWithField instanceof FixedBitSet) {
      final int numSet = ((FixedBitSet) docsWithField).cardinality();
      if (numSet >= maxDoc) {
        // The cardinality of the BitSet is maxDoc if all documents have a value.
        assert numSet == maxDoc;
        bits = new Bits.MatchAllBits(maxDoc);
      } else {
        bits = docsWithField;
      }
    } else {
      bits = docsWithField;
    }
    caches.get(DocsWithFieldCache.class).put(reader, new Entry(field, null), bits);
  }
  
  // inherit javadocs
  public int[] getInts (AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return getInts(reader, field, null, setDocsWithField);
  }

  // inherit javadocs
  public int[] getInts(AtomicReader reader, String field, IntParser parser, boolean setDocsWithField)
      throws IOException {
    return (int[]) caches.get(Integer.TYPE).get(reader, new Entry(field, parser), setDocsWithField);
  }

  static final class IntCache extends Cache {
    IntCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField)
        throws IOException {
      String field = entryKey.field;
      IntParser parser = (IntParser) entryKey.custom;
      if (parser == null) {
        try {
          return wrapper.getInts(reader, field, DEFAULT_INT_PARSER, setDocsWithField);
        } catch (NumberFormatException ne) {
          return wrapper.getInts(reader, field, NUMERIC_UTILS_INT_PARSER, setDocsWithField);
        }
      }
      final int maxDoc = reader.maxDoc();
      int[] retArray = null;

      Terms terms = reader.terms(field);
      FixedBitSet docsWithField = null;
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            wrapper.setDocsWithField(reader, field, new Bits.MatchAllBits(maxDoc));
            setDocsWithField = false;
          }
        }
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final int termval = parser.parseInt(term);
            if (retArray == null) {
              // late init so numeric fields don't double allocate
              retArray = new int[maxDoc];
            }

            docs = termsEnum.docs(null, docs, false);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
              if (setDocsWithField) {
                if (docsWithField == null) {
                  // Lazy init
                  docsWithField = new FixedBitSet(maxDoc);
                }
                docsWithField.set(docID);
              }
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }

      if (retArray == null) {
        // no values
        retArray = new int[maxDoc];
      }
      if (setDocsWithField) {
        wrapper.setDocsWithField(reader, field, docsWithField);
      }
      return retArray;
    }
  }
  
  public Bits getDocsWithField(AtomicReader reader, String field)
      throws IOException {
    return (Bits) caches.get(DocsWithFieldCache.class).get(reader, new Entry(field, null), false);
  }

  static final class DocsWithFieldCache extends Cache {
    DocsWithFieldCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }
    
    @Override
      protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField /* ignored */)
    throws IOException {
      final String field = entryKey.field;      
      FixedBitSet res = null;
      Terms terms = reader.terms(field);
      final int maxDoc = reader.maxDoc();
      if (terms != null) {
        final int termsDocCount = terms.getDocCount();
        assert termsDocCount <= maxDoc;
        if (termsDocCount == maxDoc) {
          // Fast case: all docs have this field:
          return new Bits.MatchAllBits(maxDoc);
        }
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (res == null) {
            // lazy init
            res = new FixedBitSet(maxDoc);
          }

          docs = termsEnum.docs(null, docs, false);
          // TODO: use bulk API
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            res.set(docID);
          }
        }
      }
      if (res == null) {
        return new Bits.MatchNoBits(maxDoc);
      }
      final int numSet = res.cardinality();
      if (numSet >= maxDoc) {
        // The cardinality of the BitSet is maxDoc if all documents have a value.
        assert numSet == maxDoc;
        return new Bits.MatchAllBits(maxDoc);
      }
      return res;
    }
  }

  // inherit javadocs
  public float[] getFloats (AtomicReader reader, String field, boolean setDocsWithField)
    throws IOException {
    return getFloats(reader, field, null, setDocsWithField);
  }

  // inherit javadocs
  public float[] getFloats(AtomicReader reader, String field, FloatParser parser, boolean setDocsWithField)
    throws IOException {

    return (float[]) caches.get(Float.TYPE).get(reader, new Entry(field, parser), setDocsWithField);
  }

  static final class FloatCache extends Cache {
    FloatCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField)
        throws IOException {
      String field = entryKey.field;
      FloatParser parser = (FloatParser) entryKey.custom;
      if (parser == null) {
        try {
          return wrapper.getFloats(reader, field, DEFAULT_FLOAT_PARSER, setDocsWithField);
        } catch (NumberFormatException ne) {
          return wrapper.getFloats(reader, field, NUMERIC_UTILS_FLOAT_PARSER, setDocsWithField);
        }
      }
      final int maxDoc = reader.maxDoc();
      float[] retArray = null;

      Terms terms = reader.terms(field);
      FixedBitSet docsWithField = null;
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            wrapper.setDocsWithField(reader, field, new Bits.MatchAllBits(maxDoc));
            setDocsWithField = false;
          }
        }
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final float termval = parser.parseFloat(term);
            if (retArray == null) {
              // late init so numeric fields don't double allocate
              retArray = new float[maxDoc];
            }
            
            docs = termsEnum.docs(null, docs, false);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
              if (setDocsWithField) {
                if (docsWithField == null) {
                  // Lazy init
                  docsWithField = new FixedBitSet(maxDoc);
                }
                docsWithField.set(docID);
              }
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }

      if (retArray == null) {
        // no values
        retArray = new float[maxDoc];
      }
      if (setDocsWithField) {
        wrapper.setDocsWithField(reader, field, docsWithField);
      }
      return retArray;
    }
  }


  public long[] getLongs(AtomicReader reader, String field, boolean setDocsWithField) throws IOException {
    return getLongs(reader, field, null, setDocsWithField);
  }
  
  // inherit javadocs
  public long[] getLongs(AtomicReader reader, String field, FieldCache.LongParser parser, boolean setDocsWithField)
      throws IOException {
    return (long[]) caches.get(Long.TYPE).get(reader, new Entry(field, parser), setDocsWithField);
  }

  static final class LongCache extends Cache {
    LongCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField)
        throws IOException {
      String field = entryKey.field;
      FieldCache.LongParser parser = (FieldCache.LongParser) entryKey.custom;
      if (parser == null) {
        try {
          return wrapper.getLongs(reader, field, DEFAULT_LONG_PARSER, setDocsWithField);
        } catch (NumberFormatException ne) {
          return wrapper.getLongs(reader, field, NUMERIC_UTILS_LONG_PARSER, setDocsWithField);
        }
      }
      final int maxDoc = reader.maxDoc();
      long[] retArray = null;

      Terms terms = reader.terms(field);
      FixedBitSet docsWithField = null;
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            wrapper.setDocsWithField(reader, field, new Bits.MatchAllBits(maxDoc));
            setDocsWithField = false;
          }
        }
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final long termval = parser.parseLong(term);
            if (retArray == null) {
              // late init so numeric fields don't double allocate
              retArray = new long[maxDoc];
            }

            docs = termsEnum.docs(null, docs, false);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
              if (setDocsWithField) {
                if (docsWithField == null) {
                  // Lazy init
                  docsWithField = new FixedBitSet(maxDoc);
                }
                docsWithField.set(docID);
              }
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }

      if (retArray == null) {
        // no values
        retArray = new long[maxDoc];
      }
      if (setDocsWithField) {
        wrapper.setDocsWithField(reader, field, docsWithField);
      }
      return retArray;
    }
  }

  // inherit javadocs
  public double[] getDoubles(AtomicReader reader, String field, boolean setDocsWithField)
    throws IOException {
    return getDoubles(reader, field, null, setDocsWithField);
  }

  // inherit javadocs
  public double[] getDoubles(AtomicReader reader, String field, FieldCache.DoubleParser parser, boolean setDocsWithField)
      throws IOException {
    return (double[]) caches.get(Double.TYPE).get(reader, new Entry(field, parser), setDocsWithField);
  }

  static final class DoubleCache extends Cache {
    DoubleCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField)
        throws IOException {
      String field = entryKey.field;
      FieldCache.DoubleParser parser = (FieldCache.DoubleParser) entryKey.custom;
      if (parser == null) {
        try {
          return wrapper.getDoubles(reader, field, DEFAULT_DOUBLE_PARSER, setDocsWithField);
        } catch (NumberFormatException ne) {
          return wrapper.getDoubles(reader, field, NUMERIC_UTILS_DOUBLE_PARSER, setDocsWithField);
        }
      }
      final int maxDoc = reader.maxDoc();
      double[] retArray = null;

      Terms terms = reader.terms(field);
      FixedBitSet docsWithField = null;
      if (terms != null) {
        if (setDocsWithField) {
          final int termsDocCount = terms.getDocCount();
          assert termsDocCount <= maxDoc;
          if (termsDocCount == maxDoc) {
            // Fast case: all docs have this field:
            wrapper.setDocsWithField(reader, field, new Bits.MatchAllBits(maxDoc));
            setDocsWithField = false;
          }
        }
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final double termval = parser.parseDouble(term);
            if (retArray == null) {
              // late init so numeric fields don't double allocate
              retArray = new double[maxDoc];
            }

            docs = termsEnum.docs(null, docs, false);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
              if (setDocsWithField) {
                if (docsWithField == null) {
                  // Lazy init
                  docsWithField = new FixedBitSet(maxDoc);
                }
                docsWithField.set(docID);
              }
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }
      if (retArray == null) { // no values
        retArray = new double[maxDoc];
      }
      if (setDocsWithField) {
        wrapper.setDocsWithField(reader, field, docsWithField);
      }
      return retArray;
    }
  }

  public static class DocTermsIndexImpl extends DocTermsIndex {
    private final PagedBytes.Reader bytes;
    private final PackedInts.Reader termOrdToBytesOffset;
    private final PackedInts.Reader docToTermOrd;
    private final int numOrd;

    public DocTermsIndexImpl(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, PackedInts.Reader docToTermOrd, int numOrd) {
      this.bytes = bytes;
      this.docToTermOrd = docToTermOrd;
      this.termOrdToBytesOffset = termOrdToBytesOffset;
      this.numOrd = numOrd;
    }

    @Override
    public PackedInts.Reader getDocToOrd() {
      return docToTermOrd;
    }

    @Override
    public int numOrd() {
      return numOrd;
    }

    @Override
    public int getOrd(int docID) {
      return (int) docToTermOrd.get(docID);
    }

    @Override
    public int size() {
      return docToTermOrd.size();
    }

    @Override
    public BytesRef lookup(int ord, BytesRef ret) {
      return bytes.fill(ret, termOrdToBytesOffset.get(ord));
    }

    @Override
    public TermsEnum getTermsEnum() {
      return this.new DocTermsIndexEnum();
    }

    class DocTermsIndexEnum extends TermsEnum {
      int currentOrd;
      int currentBlockNumber;
      int end;  // end position in the current block
      final byte[][] blocks;
      final int[] blockEnds;

      final BytesRef term = new BytesRef();

      public DocTermsIndexEnum() {
        currentOrd = 0;
        currentBlockNumber = 0;
        blocks = bytes.getBlocks();
        blockEnds = bytes.getBlockEnds();
        currentBlockNumber = bytes.fillAndGetIndex(term, termOrdToBytesOffset.get(0));
        end = blockEnds[currentBlockNumber];
      }

      @Override
      public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */) throws IOException {
        int low = 1;
        int high = numOrd-1;
        
        while (low <= high) {
          int mid = (low + high) >>> 1;
          seekExact(mid);
          int cmp = term.compareTo(text);

          if (cmp < 0)
            low = mid + 1;
          else if (cmp > 0)
            high = mid - 1;
          else
            return SeekStatus.FOUND; // key found
        }
        
        if (low == numOrd) {
          return SeekStatus.END;
        } else {
          seekExact(low);
          return SeekStatus.NOT_FOUND;
        }
      }

      public void seekExact(long ord) throws IOException {
        assert(ord >= 0 && ord <= numOrd);
        // TODO: if gap is small, could iterate from current position?  Or let user decide that?
        currentBlockNumber = bytes.fillAndGetIndex(term, termOrdToBytesOffset.get((int)ord));
        end = blockEnds[currentBlockNumber];
        currentOrd = (int)ord;
      }

      @Override
      public BytesRef next() throws IOException {
        int start = term.offset + term.length;
        if (start >= end) {
          // switch byte blocks
          if (currentBlockNumber +1 >= blocks.length) {
            return null;
          }
          currentBlockNumber++;
          term.bytes = blocks[currentBlockNumber];
          end = blockEnds[currentBlockNumber];
          start = 0;
          if (end<=0) return null;  // special case of empty last array
        }

        currentOrd++;

        byte[] block = term.bytes;
        if ((block[start] & 128) == 0) {
          term.length = block[start];
          term.offset = start+1;
        } else {
          term.length = (((block[start] & 0x7f)) << 8) | (block[1+start] & 0xff);
          term.offset = start+2;
        }

        return term;
      }

      @Override
      public BytesRef term() throws IOException {
        return term;
      }

      @Override
      public long ord() throws IOException {
        return currentOrd;
      }

      @Override
      public int docFreq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long totalTermFreq() {
        return -1;
      }

      @Override
      public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public void seekExact(BytesRef term, TermState state) throws IOException {
        assert state != null && state instanceof OrdTermState;
        this.seekExact(((OrdTermState)state).ord);
      }

      @Override
      public TermState termState() throws IOException {
        OrdTermState state = new OrdTermState();
        state.ord = currentOrd;
        return state;
      }
    }
  }

  private static boolean DEFAULT_FASTER_BUT_MORE_RAM = true;

  public DocTermsIndex getTermsIndex(AtomicReader reader, String field) throws IOException {
    return getTermsIndex(reader, field, DEFAULT_FASTER_BUT_MORE_RAM);
  }

  public DocTermsIndex getTermsIndex(AtomicReader reader, String field, boolean fasterButMoreRAM) throws IOException {
    return (DocTermsIndex) caches.get(DocTermsIndex.class).get(reader, new Entry(field, Boolean.valueOf(fasterButMoreRAM)), false);
  }

  static class DocTermsIndexCache extends Cache {
    DocTermsIndexCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField /* ignored */)
        throws IOException {

      Terms terms = reader.terms(entryKey.field);

      final boolean fasterButMoreRAM = ((Boolean) entryKey.custom).booleanValue();

      final PagedBytes bytes = new PagedBytes(15);

      int startBytesBPV;
      int startTermsBPV;
      int startNumUniqueTerms;

      int maxDoc = reader.maxDoc();
      final int termCountHardLimit;
      if (maxDoc == Integer.MAX_VALUE) {
        termCountHardLimit = Integer.MAX_VALUE;
      } else {
        termCountHardLimit = maxDoc+1;
      }

      if (terms != null) {
        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = 0;
        try {
          numUniqueTerms = terms.size();
        } catch (UnsupportedOperationException uoe) {
          numUniqueTerms = -1;
        }
        if (numUniqueTerms != -1) {

          if (numUniqueTerms > termCountHardLimit) {
            // app is misusing the API (there is more than
            // one term per doc); in this case we make best
            // effort to load what we can (see LUCENE-2142)
            numUniqueTerms = termCountHardLimit;
          }

          startBytesBPV = PackedInts.bitsRequired(numUniqueTerms*4);
          startTermsBPV = PackedInts.bitsRequired(numUniqueTerms);

          startNumUniqueTerms = (int) numUniqueTerms;
        } else {
          startBytesBPV = 1;
          startTermsBPV = 1;
          startNumUniqueTerms = 1;
        }
      } else {
        startBytesBPV = 1;
        startTermsBPV = 1;
        startNumUniqueTerms = 1;
      }

      GrowableWriter termOrdToBytesOffset = new GrowableWriter(startBytesBPV, 1+startNumUniqueTerms, fasterButMoreRAM);
      final GrowableWriter docToTermOrd = new GrowableWriter(startTermsBPV, maxDoc, fasterButMoreRAM);

      // 0 is reserved for "unset"
      bytes.copyUsingLengthPrefix(new BytesRef());
      int termOrd = 1;

      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;

        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (termOrd >= termCountHardLimit) {
            break;
          }

          if (termOrd == termOrdToBytesOffset.size()) {
            // NOTE: this code only runs if the incoming
            // reader impl doesn't implement
            // size (which should be uncommon)
            termOrdToBytesOffset = termOrdToBytesOffset.resize(ArrayUtil.oversize(1+termOrd, 1));
          }
          termOrdToBytesOffset.set(termOrd, bytes.copyUsingLengthPrefix(term));
          docs = termsEnum.docs(null, docs, false);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            docToTermOrd.set(docID, termOrd);
          }
          termOrd++;
        }

        if (termOrdToBytesOffset.size() > termOrd) {
          termOrdToBytesOffset = termOrdToBytesOffset.resize(termOrd);
        }
      }

      // maybe an int-only impl?
      return new DocTermsIndexImpl(bytes.freeze(true), termOrdToBytesOffset.getMutable(), docToTermOrd.getMutable(), termOrd);
    }
  }

  private static class DocTermsImpl extends DocTerms {
    private final PagedBytes.Reader bytes;
    private final PackedInts.Reader docToOffset;

    public DocTermsImpl(PagedBytes.Reader bytes, PackedInts.Reader docToOffset) {
      this.bytes = bytes;
      this.docToOffset = docToOffset;
    }

    @Override
    public int size() {
      return docToOffset.size();
    }

    @Override
    public boolean exists(int docID) {
      return docToOffset.get(docID) == 0;
    }

    @Override
    public BytesRef getTerm(int docID, BytesRef ret) {
      final int pointer = (int) docToOffset.get(docID);
      return bytes.fill(ret, pointer);
    }      
  }

  // TODO: this if DocTermsIndex was already created, we
  // should share it...
  public DocTerms getTerms(AtomicReader reader, String field) throws IOException {
    return getTerms(reader, field, DEFAULT_FASTER_BUT_MORE_RAM);
  }

  public DocTerms getTerms(AtomicReader reader, String field, boolean fasterButMoreRAM) throws IOException {
    return (DocTerms) caches.get(DocTerms.class).get(reader, new Entry(field, Boolean.valueOf(fasterButMoreRAM)), false);
  }

  static final class DocTermsCache extends Cache {
    DocTermsCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField /* ignored */)
        throws IOException {

      Terms terms = reader.terms(entryKey.field);

      final boolean fasterButMoreRAM = ((Boolean) entryKey.custom).booleanValue();

      final int termCountHardLimit = reader.maxDoc();

      // Holds the actual term data, expanded.
      final PagedBytes bytes = new PagedBytes(15);

      int startBPV;

      if (terms != null) {
        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = 0;
        try {
          numUniqueTerms = terms.size();
        } catch (UnsupportedOperationException uoe) {
          numUniqueTerms = -1;
        }
        if (numUniqueTerms != -1) {
          if (numUniqueTerms > termCountHardLimit) {
            numUniqueTerms = termCountHardLimit;
          }
          startBPV = PackedInts.bitsRequired(numUniqueTerms*4);
        } else {
          startBPV = 1;
        }
      } else {
        startBPV = 1;
      }

      final GrowableWriter docToOffset = new GrowableWriter(startBPV, reader.maxDoc(), fasterButMoreRAM);
      
      // pointer==0 means not set
      bytes.copyUsingLengthPrefix(new BytesRef());

      if (terms != null) {
        int termCount = 0;
        final TermsEnum termsEnum = terms.iterator(null);
        DocsEnum docs = null;
        while(true) {
          if (termCount++ == termCountHardLimit) {
            // app is misusing the API (there is more than
            // one term per doc); in this case we make best
            // effort to load what we can (see LUCENE-2142)
            break;
          }

          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          final long pointer = bytes.copyUsingLengthPrefix(term);
          docs = termsEnum.docs(null, docs, false);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            docToOffset.set(docID, pointer);
          }
        }
      }

      // maybe an int-only impl?
      return new DocTermsImpl(bytes.freeze(true), docToOffset.getMutable());
    }
  }

  public DocTermOrds getDocTermOrds(AtomicReader reader, String field) throws IOException {
    return (DocTermOrds) caches.get(DocTermOrds.class).get(reader, new Entry(field, null), false);
  }

  static final class DocTermOrdsCache extends Cache {
    DocTermOrdsCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(AtomicReader reader, Entry entryKey, boolean setDocsWithField /* ignored */)
        throws IOException {
      return new DocTermOrds(reader, entryKey.field);
    }
  }

  private volatile PrintStream infoStream;

  public void setInfoStream(PrintStream stream) {
    infoStream = stream;
  }

  public PrintStream getInfoStream() {
    return infoStream;
  }
}

