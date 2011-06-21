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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.cache.ByteValuesCreator;
import org.apache.lucene.search.cache.DocTermsCreator;
import org.apache.lucene.search.cache.DocTermsIndexCreator;
import org.apache.lucene.search.cache.DoubleValuesCreator;
import org.apache.lucene.search.cache.EntryCreator;
import org.apache.lucene.search.cache.FloatValuesCreator;
import org.apache.lucene.search.cache.IntValuesCreator;
import org.apache.lucene.search.cache.LongValuesCreator;
import org.apache.lucene.search.cache.ShortValuesCreator;
import org.apache.lucene.search.cache.CachedArray.ByteValues;
import org.apache.lucene.search.cache.CachedArray.DoubleValues;
import org.apache.lucene.search.cache.CachedArray.FloatValues;
import org.apache.lucene.search.cache.CachedArray.IntValues;
import org.apache.lucene.search.cache.CachedArray.LongValues;
import org.apache.lucene.search.cache.CachedArray.ShortValues;
import org.apache.lucene.util.FieldCacheSanityChecker;

/**
 * Expert: The default cache implementation, storing all values in memory.
 * A WeakHashMap is used for storage.
 *
 * <p>Created: May 19, 2004 4:40:36 PM
 * 
 * @lucene.internal -- this is now public so that the tests can use reflection
 * to call methods.  It will likely be removed without (much) notice.
 * 
 * @since   lucene 1.4
 */
public class FieldCacheImpl implements FieldCache {  // Made Public so that 
	
  private Map<Class<?>,Cache> caches;
  FieldCacheImpl() {
    init();
  }
  private synchronized void init() {
    caches = new HashMap<Class<?>,Cache>(7);
    caches.put(Byte.TYPE, new Cache<ByteValues>(this));
    caches.put(Short.TYPE, new Cache<ShortValues>(this));
    caches.put(Integer.TYPE, new Cache<IntValues>(this));
    caches.put(Float.TYPE, new Cache<FloatValues>(this));
    caches.put(Long.TYPE, new Cache<LongValues>(this));
    caches.put(Double.TYPE, new Cache<DoubleValues>(this));
    caches.put(DocTermsIndex.class, new Cache<DocTermsIndex>(this));
    caches.put(DocTerms.class, new Cache<DocTerms>(this));
  }
  
  public synchronized void purgeAllCaches() {
    init();
  }

  public synchronized void purge(IndexReader r) {
    for(Cache c : caches.values()) {
      c.purge(r);
    }
  }
  
  public synchronized CacheEntry[] getCacheEntries() {
    List<CacheEntry> result = new ArrayList<CacheEntry>(17);
    for(final Map.Entry<Class<?>,Cache> cacheEntry: caches.entrySet()) {
      final Cache<?> cache = cacheEntry.getValue();
      final Class<?> cacheType = cacheEntry.getKey();
      synchronized(cache.readerCache) {
        for( Object readerKey : cache.readerCache.keySet() ) {
          Map<?, Object> innerCache = cache.readerCache.get(readerKey);
          for (final Map.Entry<?, Object> mapEntry : innerCache.entrySet()) {
            Entry entry = (Entry)mapEntry.getKey();
            result.add(new CacheEntryImpl(readerKey, entry.field,
                                          cacheType, entry.creator,
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
    private final EntryCreator custom;
    private final Object value;
    CacheEntryImpl(Object readerKey, String fieldName,
                   Class<?> cacheType,
                   EntryCreator custom,
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

  final static IndexReader.ReaderFinishedListener purgeReader = new IndexReader.ReaderFinishedListener() {
    // @Override -- not until Java 1.6
    public void finished(IndexReader reader) {
      FieldCache.DEFAULT.purge(reader);
    }
  };

  /** Expert: Internal cache. */
  final static class Cache<T> {
    Cache() {
      this.wrapper = null;
    }

    Cache(FieldCache wrapper) {
      this.wrapper = wrapper;
    }

    final FieldCache wrapper;

    final Map<Object,Map<Entry<T>,Object>> readerCache = new WeakHashMap<Object,Map<Entry<T>,Object>>();

    protected Object createValue(IndexReader reader, Entry entryKey) throws IOException {
      return entryKey.creator.create( reader );
    }

    /** Remove this reader from the cache, if present. */
    public void purge(IndexReader r) {
      Object readerKey = r.getCoreCacheKey();
      synchronized(readerCache) {
        readerCache.remove(readerKey);
      }
    }

    @SuppressWarnings("unchecked")
    public Object get(IndexReader reader, Entry<T> key) throws IOException {
      Map<Entry<T>,Object> innerCache;
      Object value;
      final Object readerKey = reader.getCoreCacheKey();
      synchronized (readerCache) {
        innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          // First time this reader is using FieldCache
          innerCache = new HashMap<Entry<T>,Object>();
          readerCache.put(readerKey, innerCache);
          reader.addReaderFinishedListener(purgeReader);
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

            // Only check if key.custom (the parser) is
            // non-null; else, we check twice for a single
            // call to FieldCache.getXXX
            if (key.creator != null && wrapper != null) {
              final PrintStream infoStream = wrapper.getInfoStream();
              if (infoStream != null) {
                printNewInsanity(infoStream, progress.value);
              }
            }
          }
          return progress.value;
        }
      }
      
      // Validate new entries
      if( key.creator.shouldValidate() ) {
        key.creator.validate( (T)value, reader);
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
  static class Entry<T> {
    final String field;        // which Fieldable
    final EntryCreator<T> creator;       // which custom comparator or parser

    /** Creates one of these objects for a custom comparator/parser. */
    Entry (String field, EntryCreator<T> custom) {
      this.field = field;
      this.creator = custom;
    }

    /** Two of these are equal iff they reference the same field and type. */
    @Override
    public boolean equals (Object o) {
      if (o instanceof Entry) {
        Entry other = (Entry) o;
        if (other.field.equals(field)) {
          if (other.creator == null) {
            if (creator == null) return true;
          } else if (other.creator.equals (creator)) {
            return true;
          }
        }
      }
      return false;
    }

    /** Composes a hashcode based on the field and type. */
    @Override
    public int hashCode() {
      return field.hashCode() ^ (creator==null ? 0 : creator.hashCode());
    }
  }

  // inherit javadocs
  public byte[] getBytes (IndexReader reader, String field) throws IOException {
    return getBytes(reader, field, new ByteValuesCreator(field, null)).values;
  }

  // inherit javadocs
  public byte[] getBytes(IndexReader reader, String field, ByteParser parser) throws IOException {
    return getBytes(reader, field, new ByteValuesCreator(field, parser)).values;
  }

  @SuppressWarnings("unchecked")
  public ByteValues getBytes(IndexReader reader, String field, EntryCreator<ByteValues> creator ) throws IOException 
  {
    return (ByteValues)caches.get(Byte.TYPE).get(reader, new Entry(field, creator));
  }
  
  // inherit javadocs
  public short[] getShorts (IndexReader reader, String field) throws IOException {
    return getShorts(reader, field, new ShortValuesCreator(field,null)).values;
  }

  // inherit javadocs
  public short[] getShorts(IndexReader reader, String field, ShortParser parser) throws IOException {
    return getShorts(reader, field, new ShortValuesCreator(field,parser)).values;
  }

  @SuppressWarnings("unchecked")
  public ShortValues getShorts(IndexReader reader, String field, EntryCreator<ShortValues> creator ) throws IOException 
  {
    return (ShortValues)caches.get(Short.TYPE).get(reader, new Entry(field, creator));
  }
  
  // inherit javadocs
  public int[] getInts (IndexReader reader, String field) throws IOException {
    return getInts(reader, field, new IntValuesCreator( field, null )).values;
  }

  // inherit javadocs
  public int[] getInts(IndexReader reader, String field, IntParser parser) throws IOException {
    return getInts(reader, field, new IntValuesCreator( field, parser )).values;
  }

  @SuppressWarnings("unchecked")
  public IntValues getInts(IndexReader reader, String field, EntryCreator<IntValues> creator ) throws IOException {
    return (IntValues)caches.get(Integer.TYPE).get(reader, new Entry(field, creator));
  }
  
  // inherit javadocs
  public float[] getFloats (IndexReader reader, String field) throws IOException {
    return getFloats(reader, field, new FloatValuesCreator( field, null ) ).values;
  }

  // inherit javadocs
  public float[] getFloats(IndexReader reader, String field, FloatParser parser) throws IOException {
    return getFloats(reader, field, new FloatValuesCreator( field, parser ) ).values;
  }

  @SuppressWarnings("unchecked")
  public FloatValues getFloats(IndexReader reader, String field, EntryCreator<FloatValues> creator ) throws IOException {
    return (FloatValues)caches.get(Float.TYPE).get(reader, new Entry(field, creator));
  }

  public long[] getLongs(IndexReader reader, String field) throws IOException {
    return getLongs(reader, field, new LongValuesCreator( field, null ) ).values;
  }

  // inherit javadocs
  public long[] getLongs(IndexReader reader, String field, FieldCache.LongParser parser) throws IOException {
    return getLongs(reader, field, new LongValuesCreator( field, parser ) ).values;
  }

  @SuppressWarnings("unchecked")
  public LongValues getLongs(IndexReader reader, String field, EntryCreator<LongValues> creator ) throws IOException {
    return (LongValues)caches.get(Long.TYPE).get(reader, new Entry(field, creator));
  }
  
  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field) throws IOException {
    return getDoubles(reader, field, new DoubleValuesCreator( field, null ) ).values;
  }

  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field, FieldCache.DoubleParser parser) throws IOException {
    return getDoubles(reader, field, new DoubleValuesCreator( field, parser ) ).values;
  }

  @SuppressWarnings("unchecked")
  public DoubleValues getDoubles(IndexReader reader, String field, EntryCreator<DoubleValues> creator ) throws IOException {
    return (DoubleValues)caches.get(Double.TYPE).get(reader, new Entry(field, creator));
  }

  public DocTermsIndex getTermsIndex(IndexReader reader, String field) throws IOException {    
    return getTermsIndex(reader, field, new DocTermsIndexCreator(field));
  }

  public DocTermsIndex getTermsIndex(IndexReader reader, String field, boolean fasterButMoreRAM) throws IOException {    
    return getTermsIndex(reader, field, new DocTermsIndexCreator(field, 
        fasterButMoreRAM ? DocTermsIndexCreator.FASTER_BUT_MORE_RAM : 0));
  }

  @SuppressWarnings("unchecked")
  public DocTermsIndex getTermsIndex(IndexReader reader, String field, EntryCreator<DocTermsIndex> creator) throws IOException {
    return (DocTermsIndex)caches.get(DocTermsIndex.class).get(reader, new Entry(field, creator));
  }

  // TODO: this if DocTermsIndex was already created, we
  // should share it...
  public DocTerms getTerms(IndexReader reader, String field) throws IOException {
    return getTerms(reader, field, new DocTermsCreator(field));
  }

  public DocTerms getTerms(IndexReader reader, String field, boolean fasterButMoreRAM) throws IOException {
    return getTerms(reader, field, new DocTermsCreator(field,
        fasterButMoreRAM ? DocTermsCreator.FASTER_BUT_MORE_RAM : 0));
  }

  @SuppressWarnings("unchecked")
  public DocTerms getTerms(IndexReader reader, String field, EntryCreator<DocTerms> creator) throws IOException {
    return (DocTerms)caches.get(DocTerms.class).get(reader, new Entry(field, creator));
  }

  private volatile PrintStream infoStream;

  public void setInfoStream(PrintStream stream) {
    infoStream = stream;
  }

  public PrintStream getInfoStream() {
    return infoStream;
  }
}

