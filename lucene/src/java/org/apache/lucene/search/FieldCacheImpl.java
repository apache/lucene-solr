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
import java.util.*;

import org.apache.lucene.index.*;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.FieldCacheSanityChecker;

/**
 * Expert: The default cache implementation, storing all values in memory.
 * A WeakHashMap is used for storage.
 *
 * <p>Created: May 19, 2004 4:40:36 PM
 *
 * @since   lucene 1.4
 */
class FieldCacheImpl implements FieldCache {
	
  private Map<Class<?>,Cache> caches;
  FieldCacheImpl() {
    init();
  }
  private synchronized void init() {
    caches = new HashMap<Class<?>,Cache>(7);
    caches.put(Byte.TYPE, new ByteCache(this));
    caches.put(Short.TYPE, new ShortCache(this));
    caches.put(Integer.TYPE, new IntCache(this));
    caches.put(Float.TYPE, new FloatCache(this));
    caches.put(Long.TYPE, new LongCache(this));
    caches.put(Double.TYPE, new DoubleCache(this));
    caches.put(DocTermsIndex.class, new DocTermsIndexCache(this));
    caches.put(DocTerms.class, new DocTermsCache(this));
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

  /** Expert: Internal cache. */
  abstract static class Cache {
    Cache() {
      this.wrapper = null;
    }

    Cache(FieldCache wrapper) {
      this.wrapper = wrapper;
    }

    final FieldCache wrapper;

    final Map<Object,Map<Entry,Object>> readerCache = new WeakHashMap<Object,Map<Entry,Object>>();
    
    protected abstract Object createValue(IndexReader reader, Entry key)
        throws IOException;

    /** Remove this reader from the cache, if present. */
    public void purge(IndexReader r) {
      Object readerKey = r.getCoreCacheKey();
      synchronized(readerCache) {
        readerCache.remove(readerKey);
      }
    }

    public Object get(IndexReader reader, Entry key) throws IOException {
      Map<Entry,Object> innerCache;
      Object value;
      final Object readerKey = reader.getCoreCacheKey();
      synchronized (readerCache) {
        innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          innerCache = new HashMap<Entry,Object>();
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
      this.field = StringHelper.intern(field);
      this.custom = custom;
    }

    /** Two of these are equal iff they reference the same field and type. */
    @Override
    public boolean equals (Object o) {
      if (o instanceof Entry) {
        Entry other = (Entry) o;
        if (other.field == field) {
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
  public byte[] getBytes (IndexReader reader, String field) throws IOException {
    return getBytes(reader, field, null);
  }

  // inherit javadocs
  public byte[] getBytes(IndexReader reader, String field, ByteParser parser)
      throws IOException {
    return (byte[]) caches.get(Byte.TYPE).get(reader, new Entry(field, parser));
  }

  static final class ByteCache extends Cache {
    ByteCache(FieldCache wrapper) {
      super(wrapper);
    }
    @Override
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = entryKey;
      String field = entry.field;
      ByteParser parser = (ByteParser) entry.custom;
      if (parser == null) {
        return wrapper.getBytes(reader, field, FieldCache.DEFAULT_BYTE_PARSER);
      }
      final byte[] retArray = new byte[reader.maxDoc()];
      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final byte termval = parser.parseByte(term);
            docs = termsEnum.docs(delDocs, docs);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }
      return retArray;
    }
  }
  
  // inherit javadocs
  public short[] getShorts (IndexReader reader, String field) throws IOException {
    return getShorts(reader, field, null);
  }

  // inherit javadocs
  public short[] getShorts(IndexReader reader, String field, ShortParser parser)
      throws IOException {
    return (short[]) caches.get(Short.TYPE).get(reader, new Entry(field, parser));
  }

  static final class ShortCache extends Cache {
    ShortCache(FieldCache wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry =  entryKey;
      String field = entry.field;
      ShortParser parser = (ShortParser) entry.custom;
      if (parser == null) {
        return wrapper.getShorts(reader, field, FieldCache.DEFAULT_SHORT_PARSER);
      }
      final short[] retArray = new short[reader.maxDoc()];
      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
        DocsEnum docs = null;
        try {
          while(true) {
            final BytesRef term = termsEnum.next();
            if (term == null) {
              break;
            }
            final short termval = parser.parseShort(term);
            docs = termsEnum.docs(delDocs, docs);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }
      return retArray;
    }
  }
  
  // inherit javadocs
  public int[] getInts (IndexReader reader, String field) throws IOException {
    return getInts(reader, field, null);
  }

  // inherit javadocs
  public int[] getInts(IndexReader reader, String field, IntParser parser)
      throws IOException {
    return (int[]) caches.get(Integer.TYPE).get(reader, new Entry(field, parser));
  }

  static final class IntCache extends Cache {
    IntCache(FieldCache wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = entryKey;
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

      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
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
              retArray = new int[reader.maxDoc()];
            }

            docs = termsEnum.docs(delDocs, docs);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }

      if (retArray == null) {
        // no values
        retArray = new int[reader.maxDoc()];
      }
      return retArray;
    }
  }


  // inherit javadocs
  public float[] getFloats (IndexReader reader, String field)
    throws IOException {
    return getFloats(reader, field, null);
  }

  // inherit javadocs
  public float[] getFloats(IndexReader reader, String field, FloatParser parser)
    throws IOException {

    return (float[]) caches.get(Float.TYPE).get(reader, new Entry(field, parser));
  }

  static final class FloatCache extends Cache {
    FloatCache(FieldCache wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = entryKey;
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

      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
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
              retArray = new float[reader.maxDoc()];
            }
            
            docs = termsEnum.docs(delDocs, docs);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }

      if (retArray == null) {
        // no values
        retArray = new float[reader.maxDoc()];
      }
      return retArray;
    }
  }


  public long[] getLongs(IndexReader reader, String field) throws IOException {
    return getLongs(reader, field, null);
  }

  // inherit javadocs
  public long[] getLongs(IndexReader reader, String field, FieldCache.LongParser parser)
      throws IOException {
    return (long[]) caches.get(Long.TYPE).get(reader, new Entry(field, parser));
  }

  static final class LongCache extends Cache {
    LongCache(FieldCache wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(IndexReader reader, Entry entry)
        throws IOException {
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

      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
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
              retArray = new long[reader.maxDoc()];
            }

            docs = termsEnum.docs(delDocs, docs);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }

      if (retArray == null) {
        // no values
        retArray = new long[reader.maxDoc()];
      }
      return retArray;
    }
  }

  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field)
    throws IOException {
    return getDoubles(reader, field, null);
  }

  // inherit javadocs
  public double[] getDoubles(IndexReader reader, String field, FieldCache.DoubleParser parser)
      throws IOException {
    return (double[]) caches.get(Double.TYPE).get(reader, new Entry(field, parser));
  }

  static final class DoubleCache extends Cache {
    DoubleCache(FieldCache wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {
      Entry entry = entryKey;
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

      Terms terms = MultiFields.getTerms(reader, field);
      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
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
              retArray = new double[reader.maxDoc()];
            }

            docs = termsEnum.docs(delDocs, docs);
            while (true) {
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS) {
                break;
              }
              retArray[docID] = termval;
            }
          }
        } catch (StopFillCacheException stop) {
        }
      }
      if (retArray == null) // no values
        retArray = new double[reader.maxDoc()];
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
      return bytes.fillUsingLengthPrefix(ret, termOrdToBytesOffset.get(ord));
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
        currentBlockNumber = bytes.fillUsingLengthPrefix2(term, termOrdToBytesOffset.get(0));
        end = blockEnds[currentBlockNumber];
      }

      @Override
      public SeekStatus seek(BytesRef text, boolean useCache) throws IOException {
        // TODO - we can support with binary search
        throw new UnsupportedOperationException();
      }

      @Override
      public SeekStatus seek(long ord) throws IOException {
        assert(ord >= 0 && ord <= numOrd);
        // TODO: if gap is small, could iterate from current position?  Or let user decide that?
        currentBlockNumber = bytes.fillUsingLengthPrefix2(term, termOrdToBytesOffset.get((int)ord));
        end = blockEnds[currentBlockNumber];
        currentOrd = (int)ord;
        return SeekStatus.FOUND;
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
          term.length = (((int) (block[start] & 0x7f)) << 8) | (block[1+start] & 0xff);
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
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Comparator<BytesRef> getComparator() throws IOException {
        throw new UnsupportedOperationException();
      }
    }
  }

  private static boolean DEFAULT_FASTER_BUT_MORE_RAM = true;

  public DocTermsIndex getTermsIndex(IndexReader reader, String field) throws IOException {
    return getTermsIndex(reader, field, DEFAULT_FASTER_BUT_MORE_RAM);
  }

  public DocTermsIndex getTermsIndex(IndexReader reader, String field, boolean fasterButMoreRAM) throws IOException {
    return (DocTermsIndex) caches.get(DocTermsIndex.class).get(reader, new Entry(field, Boolean.valueOf(fasterButMoreRAM)));
  }

  static class DocTermsIndexCache extends Cache {
    DocTermsIndexCache(FieldCache wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {

      String field = StringHelper.intern(entryKey.field);
      Terms terms = MultiFields.getTerms(reader, field);

      final boolean fasterButMoreRAM = ((Boolean) entryKey.custom).booleanValue();

      final PagedBytes bytes = new PagedBytes(15);

      int startBytesBPV;
      int startTermsBPV;
      int startNumUniqueTerms;

      if (terms != null) {
        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = 0;
        try {
          numUniqueTerms = terms.getUniqueTermCount();
        } catch (UnsupportedOperationException uoe) {
          numUniqueTerms = -1;
        }
        if (numUniqueTerms != -1) {
          startBytesBPV = PackedInts.bitsRequired(numUniqueTerms*4);
          startTermsBPV = PackedInts.bitsRequired(numUniqueTerms);
          if (numUniqueTerms > Integer.MAX_VALUE-1) {
            throw new IllegalStateException("this field has too many (" + numUniqueTerms + ") unique terms");
          }
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
      final GrowableWriter docToTermOrd = new GrowableWriter(startTermsBPV, reader.maxDoc(), fasterButMoreRAM);

      // 0 is reserved for "unset"
      bytes.copyUsingLengthPrefix(new BytesRef());
      int termOrd = 1;

      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
        DocsEnum docs = null;

        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (termOrd == termOrdToBytesOffset.size()) {
            // NOTE: this code only runs if the incoming
            // reader impl doesn't implement
            // getUniqueTermCount (which should be uncommon)
            termOrdToBytesOffset = termOrdToBytesOffset.resize(ArrayUtil.oversize(1+termOrd, 1));
          }
          termOrdToBytesOffset.set(termOrd, bytes.copyUsingLengthPrefix(term));
          docs = termsEnum.docs(delDocs, docs);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocsEnum.NO_MORE_DOCS) {
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
      return bytes.fillUsingLengthPrefix(ret, pointer);
    }      
  }

  // TODO: this if DocTermsIndex was already created, we
  // should share it...
  public DocTerms getTerms(IndexReader reader, String field) throws IOException {
    return getTerms(reader, field, DEFAULT_FASTER_BUT_MORE_RAM);
  }

  public DocTerms getTerms(IndexReader reader, String field, boolean fasterButMoreRAM) throws IOException {
    return (DocTerms) caches.get(DocTerms.class).get(reader, new Entry(field, Boolean.valueOf(fasterButMoreRAM)));
  }

  static final class DocTermsCache extends Cache {
    DocTermsCache(FieldCache wrapper) {
      super(wrapper);
    }

    @Override
    protected Object createValue(IndexReader reader, Entry entryKey)
        throws IOException {

      String field = StringHelper.intern(entryKey.field);
      Terms terms = MultiFields.getTerms(reader, field);

      final boolean fasterButMoreRAM = ((Boolean) entryKey.custom).booleanValue();

      // Holds the actual term data, expanded.
      final PagedBytes bytes = new PagedBytes(15);

      int startBPV;

      if (terms != null) {
        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = 0;
        try {
          numUniqueTerms = terms.getUniqueTermCount();
        } catch (UnsupportedOperationException uoe) {
          numUniqueTerms = -1;
        }
        if (numUniqueTerms != -1) {
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
        final TermsEnum termsEnum = terms.iterator();
        final Bits delDocs = MultiFields.getDeletedDocs(reader);
        DocsEnum docs = null;
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          final long pointer = bytes.copyUsingLengthPrefix(term);
          docs = termsEnum.docs(delDocs, docs);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocsEnum.NO_MORE_DOCS) {
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
  private volatile PrintStream infoStream;

  public void setInfoStream(PrintStream stream) {
    infoStream = stream;
  }

  public PrintStream getInfoStream() {
    return infoStream;
  }
}

