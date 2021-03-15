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
package org.apache.solr.uninverting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Expert: The default cache implementation, storing all values in memory.
 * A WeakHashMap is used for storage.
 *
 * @lucene.internal
 */
public class FieldCacheImpl implements FieldCache {

  private Map<Class<?>,Cache> caches;
  FieldCacheImpl() {
    init();
  }

  private synchronized void init() {
    caches = new HashMap<>(6);
    caches.put(Long.TYPE, new LongCache(this));
    caches.put(BinaryDocValues.class, new BinaryDocValuesCache(this));
    caches.put(SortedDocValues.class, new SortedDocValuesCache(this));
    caches.put(DocTermOrds.class, new DocTermOrdsCache(this));
    caches.put(DocsWithFieldCache.class, new DocsWithFieldCache(this));
  }

  @Override
  public synchronized void purgeAllCaches() {
    init();
  }

  @Override
  public synchronized void purgeByCacheKey(IndexReader.CacheKey coreCacheKey) {
    for(Cache c : caches.values()) {
      c.purgeByCacheKey(coreCacheKey);
    }
  }

  @Override
  public synchronized CacheEntry[] getCacheEntries() {
    List<CacheEntry> result = new ArrayList<>(17);
    for(final Map.Entry<Class<?>,Cache> cacheEntry: caches.entrySet()) {
      final Cache cache = cacheEntry.getValue();
      final Class<?> cacheType = cacheEntry.getKey();
      synchronized(cache.readerCache) {
        for (final Map.Entry<IndexReader.CacheKey,Map<CacheKey, Accountable>> readerCacheEntry : cache.readerCache.entrySet()) {
          final IndexReader.CacheKey readerKey = readerCacheEntry.getKey();
          if (readerKey == null) continue;
          final Map<CacheKey, Accountable> innerCache = readerCacheEntry.getValue();
          for (final Map.Entry<CacheKey, Accountable> mapEntry : innerCache.entrySet()) {
            CacheKey entry = mapEntry.getKey();
            result.add(new CacheEntry(readerKey, entry.field,
                                      cacheType, entry.custom,
                                      mapEntry.getValue()));
          }
        }
      }
    }
    return result.toArray(new CacheEntry[result.size()]);
  }

  // per-segment fieldcaches don't purge until the shared core closes.
  final IndexReader.ClosedListener purgeCore = FieldCacheImpl.this::purgeByCacheKey;
  
  private void initReader(LeafReader reader) {
    IndexReader.CacheHelper cacheHelper = reader.getCoreCacheHelper();
    if (cacheHelper == null) {
      throw new IllegalStateException("Cannot cache on " + reader);
    }
    cacheHelper.addClosedListener(purgeCore);
  }

  /** Expert: Internal cache. */
  abstract static class Cache {

    Cache(FieldCacheImpl wrapper) {
      this.wrapper = wrapper;
    }

    final FieldCacheImpl wrapper;

    final Map<IndexReader.CacheKey,Map<CacheKey,Accountable>> readerCache = new WeakHashMap<>();
    
    protected abstract Accountable createValue(LeafReader reader, CacheKey key)
        throws IOException;

    /** Remove this reader from the cache, if present. */
    public void purgeByCacheKey(IndexReader.CacheKey coreCacheKey) {
      synchronized(readerCache) {
        readerCache.remove(coreCacheKey);
      }
    }

    /** Sets the key to the value for the provided reader;
     *  if the key is already set then this doesn't change it. */
    public void put(LeafReader reader, CacheKey key, Accountable value) {
      IndexReader.CacheHelper cacheHelper = reader.getCoreCacheHelper();
      if (cacheHelper == null) {
        throw new IllegalStateException("Cannot cache on " + reader);
      }
      final IndexReader.CacheKey readerKey = cacheHelper.getKey();
      synchronized (readerCache) {
        Map<CacheKey,Accountable> innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          // First time this reader is using FieldCache
          innerCache = new HashMap<>();
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

    public Object get(LeafReader reader, CacheKey key) throws IOException {
      Map<CacheKey,Accountable> innerCache;
      Accountable value;
      IndexReader.CacheHelper cacheHelper = reader.getCoreCacheHelper();
      if (cacheHelper == null) {
        reader.getCoreCacheHelper();
        throw new IllegalStateException("Cannot cache on " + reader);
      }
      final IndexReader.CacheKey readerKey = cacheHelper.getKey();
      synchronized (readerCache) {
        innerCache = readerCache.get(readerKey);
        if (innerCache == null) {
          // First time this reader is using FieldCache
          innerCache = new HashMap<>();
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
  static class CacheKey {
    final String field;        // which Field
    final Object custom;       // which custom comparator or parser

    /** Creates one of these objects for a custom comparator/parser. */
    CacheKey(String field, Object custom) {
      this.field = field;
      this.custom = custom;
    }

    /** Two of these are equal iff they reference the same field and type. */
    @Override
    public boolean equals (Object o) {
      if (o instanceof CacheKey) {
        CacheKey other = (CacheKey) o;
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

  private static abstract class Uninvert {

    public Bits docsWithField;
    final boolean points;
    
    // pass true to pull from points, otherwise postings.
    Uninvert(boolean points) {
      this.points = points;
    }

    final void uninvert(LeafReader reader, String field) throws IOException {
      if (points) {
        uninvertPoints(reader, field);
      } else {
        uninvertPostings(reader, field);
      }
    }
    
    final void uninvertPoints(LeafReader reader, String field) throws IOException {
      final int maxDoc = reader.maxDoc();
      PointValues values = reader.getPointValues(field);
      assert values != null;
      assert values.size() > 0;

      final boolean setDocsWithField;
      final int docCount = values.getDocCount();
      assert docCount <= maxDoc;
      if (docCount == maxDoc) {
        // Fast case: all docs have this field:
        this.docsWithField = new Bits.MatchAllBits(maxDoc);
        setDocsWithField = false;
      } else {
        setDocsWithField = true;
      }

      BytesRef scratch = new BytesRef();
      values.intersect(new IntersectVisitor() {
        @Override
        public void visit(int docID) throws IOException { 
          throw new AssertionError(); 
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
          scratch.bytes = packedValue;
          scratch.length = packedValue.length;
          visitTerm(scratch);
          visitDoc(docID);
          if (setDocsWithField) {
            if (docsWithField == null) {
              // Lazy init
              docsWithField = new FixedBitSet(maxDoc);
            }
            ((FixedBitSet)docsWithField).set(docID);
          }
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
          return Relation.CELL_CROSSES_QUERY; // inspect all byte-docid pairs
        }
      });
    }
    
    final void uninvertPostings(LeafReader reader, String field) throws IOException {
      final int maxDoc = reader.maxDoc();
      Terms terms = reader.terms(field);
      if (terms != null) {
        final boolean setDocsWithField;
        final int termsDocCount = terms.getDocCount();
        assert termsDocCount <= maxDoc;
        if (termsDocCount == maxDoc) {
          // Fast case: all docs have this field:
          this.docsWithField = new Bits.MatchAllBits(maxDoc);
          setDocsWithField = false;
        } else {
          setDocsWithField = true;
        }

        final TermsEnum termsEnum = termsEnum(terms);

        PostingsEnum docs = null;
        FixedBitSet docsWithField = null;
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          visitTerm(term);
          docs = termsEnum.postings(docs, PostingsEnum.NONE);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            visitDoc(docID);
            if (setDocsWithField) {
              if (docsWithField == null) {
                // Lazy init
                this.docsWithField = docsWithField = new FixedBitSet(maxDoc);
              }
              docsWithField.set(docID);
            }
          }
        }
      }
    }

    protected abstract TermsEnum termsEnum(Terms terms) throws IOException;
    protected abstract void visitTerm(BytesRef term);
    protected abstract void visitDoc(int docID);
  }

  // null Bits means no docs matched
  void setDocsWithField(LeafReader reader, String field, Bits docsWithField, Parser parser) {
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
    caches.get(DocsWithFieldCache.class).put(reader, new CacheKey(field, parser), new BitsEntry(bits));
  }

  private static class HoldsOneThing<T> {
    private T it;

    public void set(T it) {
      this.it = it;
    }

    public T get() {
      return it;
    }
  }

  private static class GrowableWriterAndMinValue {
    GrowableWriterAndMinValue(GrowableWriter array, long minValue) {
      this.writer = array;
      this.minValue = minValue;
    }
    public GrowableWriter writer;
    public long minValue;
  }

  @Override
  public Bits getDocsWithField(LeafReader reader, String field, Parser parser) throws IOException {
    final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
    if (fieldInfo == null) {
      // field does not exist or has no value
      return new Bits.MatchNoBits(reader.maxDoc());
    } 
    
    if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
      // doc values case
    } else if (parser instanceof PointParser) {
      // points case
    } else {
      // postings case
      if (fieldInfo.getIndexOptions() == IndexOptions.NONE) {
        return new Bits.MatchNoBits(reader.maxDoc());
      }
    }
    BitsEntry bitsEntry = (BitsEntry) caches.get(DocsWithFieldCache.class).get(reader, new CacheKey(field, parser));
    return bitsEntry.bits;
  }
  
  static class BitsEntry implements Accountable {
    final Bits bits;
    
    BitsEntry(Bits bits) {
      this.bits = bits;
    }
    
    @Override
    public long ramBytesUsed() {
      long base = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      if (bits instanceof Bits.MatchAllBits || bits instanceof Bits.MatchNoBits) {
        return base;
      } else {
        return base + (bits.length() >>> 3);
      }
    }
  }

  static final class DocsWithFieldCache extends Cache {
    DocsWithFieldCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }
    
    @Override
    protected BitsEntry createValue(LeafReader reader, CacheKey key) throws IOException {
      final String field = key.field;
      final Parser parser = (Parser) key.custom;
      FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
        return createValueDocValues(reader, field);
      } else if (parser instanceof PointParser) {
        return createValuePoints(reader, field);
      } else {
        return createValuePostings(reader, field);
      }
    }

    private BitsEntry createValueDocValues(LeafReader reader, String field) throws IOException {
      FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      
      DocValuesType dvType = fieldInfo.getDocValuesType();
      DocIdSetIterator iterator;
      switch(dvType) {
      case NUMERIC:
        iterator = reader.getNumericDocValues(field);
        break;
      case BINARY:
        iterator = reader.getBinaryDocValues(field);
        break;
      case SORTED:
        iterator = reader.getSortedDocValues(field);
        break;
      case SORTED_NUMERIC:
        iterator = reader.getSortedNumericDocValues(field);
        break;
      case SORTED_SET:
        iterator = reader.getSortedSetDocValues(field);
        break;
      default:
        throw new AssertionError();
      }

      FixedBitSet bits = new FixedBitSet(reader.maxDoc());
      while (true) {
        int docID = iterator.nextDoc();
        if (docID == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        bits.set(docID);
      }

      return new BitsEntry(bits);
    }

    private BitsEntry createValuePoints(LeafReader reader, String field) throws IOException {
      final int maxDoc = reader.maxDoc();
      PointValues values = reader.getPointValues(field);
      assert values != null;
      assert values.size() > 0;
      
      final int docCount = values.getDocCount();
      assert docCount <= maxDoc;
      if (docCount == maxDoc) {
        // Fast case: all docs have this field:
        return new BitsEntry(new Bits.MatchAllBits(maxDoc));
      }
      
      // otherwise a no-op uninvert!
      Uninvert u = new Uninvert(true) {
        @Override
        protected TermsEnum termsEnum(Terms terms) throws IOException {
          throw new AssertionError();
        }

        @Override
        protected void visitTerm(BytesRef term) {}

        @Override
        protected void visitDoc(int docID) {}
      };
      u.uninvert(reader, field);
      return new BitsEntry(u.docsWithField);
    }
    
    // TODO: it is dumb that uninverting code is duplicated here in this method!!
    private BitsEntry createValuePostings(LeafReader reader, String field) throws IOException {
      final int maxDoc = reader.maxDoc();

      // Visit all docs that have terms for this field
      FixedBitSet res = null;
      Terms terms = reader.terms(field);
      if (terms != null) {
        final int termsDocCount = terms.getDocCount();
        assert termsDocCount <= maxDoc;
        if (termsDocCount == maxDoc) {
          // Fast case: all docs have this field:
          return new BitsEntry(new Bits.MatchAllBits(maxDoc));
        }
        final TermsEnum termsEnum = terms.iterator();
        PostingsEnum docs = null;
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (res == null) {
            // lazy init
            res = new FixedBitSet(maxDoc);
          }

          docs = termsEnum.postings(docs, PostingsEnum.NONE);
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
        return new BitsEntry(new Bits.MatchNoBits(maxDoc));
      }
      final int numSet = res.cardinality();
      if (numSet >= maxDoc) {
        // The cardinality of the BitSet is maxDoc if all documents have a value.
        assert numSet == maxDoc;
        return new BitsEntry(new Bits.MatchAllBits(maxDoc));
      }
      return new BitsEntry(res);
    }
  }

  @Override
  public NumericDocValues getNumerics(LeafReader reader, String field, Parser parser) throws IOException {
    if (parser == null) {
      throw new NullPointerException();
    }
    final NumericDocValues valuesIn = reader.getNumericDocValues(field);
    if (valuesIn != null) {
      return valuesIn;
    } else {
      final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
      if (info == null) {
        return DocValues.emptyNumeric();
      } else if (info.getDocValuesType() != DocValuesType.NONE) {
        throw new IllegalStateException("Type mismatch: " + field + " was indexed as " + info.getDocValuesType());
      }
      
      if (parser instanceof PointParser) {
        // points case
        // no points in this segment
        if (info.getPointDimensionCount() == 0) {
          return DocValues.emptyNumeric();
        }
        if (info.getPointDimensionCount() != 1) {
          throw new IllegalStateException("Type mismatch: " + field + " was indexed with dimensions=" + info.getPointDimensionCount());
        }
        PointValues values = reader.getPointValues(field);
        // no actual points for this field (e.g. all points deleted)
        if (values == null || values.size() == 0) {
          return DocValues.emptyNumeric();
        }
        // not single-valued
        if (values.size() != values.getDocCount()) {
          throw new IllegalStateException("Type mismatch: " + field + " was indexed with multiple values, numValues=" + values.size() + ",numDocs=" + values.getDocCount());
        }
      } else {
        // postings case 
        // not indexed
        if (info.getIndexOptions() == IndexOptions.NONE) {
          return DocValues.emptyNumeric();
        }
      }

      return ((LongsFromArray) caches.get(Long.TYPE).get(reader, new CacheKey(field, parser))).iterator();
    }
  }

  public static class LongsFromArray implements Accountable {
    private final PackedInts.Reader values;
    private final long minValue;
    private final Bits docsWithField;
    private final String field;

    public LongsFromArray(String field, PackedInts.Reader values, long minValue, Bits docsWithField) { // TODO: accept null docsWithField?
      this.field = field;
      this.values = values;
      this.minValue = minValue;
      this.docsWithField = docsWithField;
    }
    
    @Override
    public long ramBytesUsed() {
      return values.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF + Long.BYTES;
    }

    public NumericDocValues iterator() {
      return new NumericDocValues() {
        int docID = -1;

        @Override
        public int docID() {
          return docID;
        }

        @Override
        public int nextDoc() {
          while (true) {
            docID++;
            if (docID >= values.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            if (docsWithField.get(docID)) {
              return docID;
            }
          }
        }

        @Override
        public int advance(int target) {
          if (target < values.size()) {
            docID = target;
            if (docsWithField.get(docID)) {
              return docID;
            } else{
              return nextDoc();
            }
          } else {
            docID = NO_MORE_DOCS;
            return docID;
          }
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          docID = target;
          return docsWithField.get(docID);
        }

        @Override
        public long cost() {
          return values.size();
        }

        @Override
        public long longValue() {
          return minValue + values.get(docID);
        }
      };
    }
  }

  static final class LongCache extends Cache {
    LongCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(final LeafReader reader, CacheKey key)
        throws IOException {

      final Parser parser = (Parser) key.custom;

      final HoldsOneThing<GrowableWriterAndMinValue> valuesRef = new HoldsOneThing<>();

      Uninvert u = new Uninvert(parser instanceof PointParser) {
          private long minValue;
          private long currentValue;
          private GrowableWriter values;

          @Override
          public void visitTerm(BytesRef term) {
            currentValue = parser.parseValue(term);
            if (values == null) {
              // Lazy alloc so for the numeric field case
              // (which will hit a NumberFormatException
              // when we first try the DEFAULT_INT_PARSER),
              // we don't double-alloc:
              int startBitsPerValue;
              // Make sure than missing values (0) can be stored without resizing
              if (currentValue < 0) {
                minValue = currentValue;
                startBitsPerValue = minValue == Long.MIN_VALUE ? 64 : PackedInts.bitsRequired(-minValue);
              } else {
                minValue = 0;
                startBitsPerValue = PackedInts.bitsRequired(currentValue);
              }
              values = new GrowableWriter(startBitsPerValue, reader.maxDoc(), PackedInts.FAST);
              if (minValue != 0) {
                values.fill(0, values.size(), -minValue); // default value must be 0
              }
              valuesRef.set(new GrowableWriterAndMinValue(values, minValue));
            }
          }

          @Override
          public void visitDoc(int docID) {
            values.set(docID, currentValue - minValue);
          }
          
          @Override
          protected TermsEnum termsEnum(Terms terms) throws IOException {
            return parser.termsEnum(terms);
          }
        };

      u.uninvert(reader, key.field);
      wrapper.setDocsWithField(reader, key.field, u.docsWithField, parser);
      GrowableWriterAndMinValue values = valuesRef.get();
      Bits docsWithField = u.docsWithField == null ? new Bits.MatchNoBits(reader.maxDoc()) : u.docsWithField;
      if (values == null) {
        return new LongsFromArray(key.field, new PackedInts.NullReader(reader.maxDoc()), 0L, docsWithField);
      }
      return new LongsFromArray(key.field, values.writer.getMutable(), values.minValue, docsWithField);
    }
  }

  public static class SortedDocValuesImpl implements Accountable {
    private final PagedBytes.Reader bytes;
    private final PackedLongValues termOrdToBytesOffset;
    private final PackedInts.Reader docToTermOrd;
    private final int numOrd;

    public SortedDocValuesImpl(PagedBytes.Reader bytes, PackedLongValues termOrdToBytesOffset, PackedInts.Reader docToTermOrd, int numOrd) {
      this.bytes = bytes;
      this.docToTermOrd = docToTermOrd;
      this.termOrdToBytesOffset = termOrdToBytesOffset;
      this.numOrd = numOrd;
    }

    public SortedDocValues iterator() {
      return new Iter();
    }

    public class Iter extends SortedDocValues {
      private int docID = -1;
      private final BytesRef term = new BytesRef();

      /** @lucene.internal Specific to this implementation and subject to change.  For internal optimization only. */
      public int getOrd(int docID) {
        // Subtract 1, matching the 1+ord we did when
        // storing, so that missing values, which are 0 in the
        // packed ints, are returned as -1 ord:
        return (int) docToTermOrd.get(docID)-1;
      }

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() {
        while (true) {
          docID++;
          if (docID >= docToTermOrd.size()) {
            docID = NO_MORE_DOCS;
            return docID;
          }
          if (docToTermOrd.get(docID) != 0) {
            return docID;
          }
        }
      }

      @Override
      public int advance(int target) {
        if (target < docToTermOrd.size()) {
          docID = target;
          if (docToTermOrd.get(docID) != 0) {
            return docID;
          } else{
            return nextDoc();
          }
        } else {
          docID = NO_MORE_DOCS;
          return docID;
        }
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        docID = target;
        return docToTermOrd.get(docID) != 0;
      }

      @Override
      public long cost() {
        return 0;
      }

      @Override
      public int ordValue() {
        // Subtract 1, matching the 1+ord we did when
        // storing, so that missing values, which are 0 in the
        // packed ints, are returned as -1 ord:
        return (int) docToTermOrd.get(docID)-1;
      }

      @Override
      public int getValueCount() {
        return numOrd;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        if (ord < 0) {
          throw new IllegalArgumentException("ord must be >=0 (got ord=" + ord + ")");
        }
        bytes.fill(term, termOrdToBytesOffset.get(ord));
        return term;
      }
    }

    @Override
    public long ramBytesUsed() {
      return bytes.ramBytesUsed() + 
             termOrdToBytesOffset.ramBytesUsed() + 
             docToTermOrd.ramBytesUsed() + 
             3*RamUsageEstimator.NUM_BYTES_OBJECT_REF +
             Integer.BYTES;
    }
    
    @Override
    public Collection<Accountable> getChildResources() {
      List<Accountable> resources = new ArrayList<>(3);
      resources.add(Accountables.namedAccountable("term bytes", bytes));
      resources.add(Accountables.namedAccountable("ord -> term", termOrdToBytesOffset));
      resources.add(Accountables.namedAccountable("doc -> ord", docToTermOrd));
      return Collections.unmodifiableList(resources);
    }
  }

  public SortedDocValues getTermsIndex(LeafReader reader, String field) throws IOException {
    return getTermsIndex(reader, field, PackedInts.FAST);
  }

  public SortedDocValues getTermsIndex(LeafReader reader, String field, float acceptableOverheadRatio) throws IOException {
    SortedDocValues valuesIn = reader.getSortedDocValues(field);
    if (valuesIn != null) {
      // Not cached here by FieldCacheImpl (cached instead
      // per-thread by SegmentReader):
      return valuesIn;
    } else {
      final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
      if (info == null) {
        return DocValues.emptySorted();
      } else if (info.getDocValuesType() != DocValuesType.NONE) {
        // we don't try to build a sorted instance from numeric/binary doc
        // values because dedup can be very costly
        throw new IllegalStateException("Type mismatch: " + field + " was indexed as " + info.getDocValuesType());
      } else if (info.getIndexOptions() == IndexOptions.NONE) {
        return DocValues.emptySorted();
      }
      SortedDocValuesImpl impl = (SortedDocValuesImpl) caches.get(SortedDocValues.class).get(reader, new CacheKey(field, acceptableOverheadRatio));
      return impl.iterator();
    }
  }

  static class SortedDocValuesCache extends Cache {
    SortedDocValuesCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(LeafReader reader, CacheKey key)
        throws IOException {

      final int maxDoc = reader.maxDoc();

      Terms terms = reader.terms(key.field);

      final float acceptableOverheadRatio = ((Float) key.custom).floatValue();

      final PagedBytes bytes = new PagedBytes(15);

      int startTermsBPV;

      // TODO: use Uninvert?
      if (terms != null) {
        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = terms.size();
        if (numUniqueTerms != -1L) {
          if (numUniqueTerms > maxDoc) {
            throw new IllegalStateException("Type mismatch: " + key.field + " was indexed with multiple values per document, use SORTED_SET instead");
          }

          startTermsBPV = PackedInts.bitsRequired(numUniqueTerms);
        } else {
          startTermsBPV = 1;
        }
      } else {
        startTermsBPV = 1;
      }

      PackedLongValues.Builder termOrdToBytesOffset = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
      final GrowableWriter docToTermOrd = new GrowableWriter(startTermsBPV, maxDoc, acceptableOverheadRatio);

      int termOrd = 0;

      // TODO: use Uninvert?

      if (terms != null) {
        final TermsEnum termsEnum = terms.iterator();
        PostingsEnum docs = null;

        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (termOrd >= maxDoc) {
            throw new IllegalStateException("Type mismatch: " + key.field + " was indexed with multiple values per document, use SORTED_SET instead");
          }

          termOrdToBytesOffset.add(bytes.copyUsingLengthPrefix(term));
          docs = termsEnum.postings(docs, PostingsEnum.NONE);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            // Store 1+ ord into packed bits
            docToTermOrd.set(docID, 1+termOrd);
          }
          termOrd++;
        }
      }

      // maybe an int-only impl?
      return new SortedDocValuesImpl(bytes.freeze(true), termOrdToBytesOffset.build(), docToTermOrd.getMutable(), termOrd);
    }
  }

  public static class BinaryDocValuesImpl implements Accountable {
    private final PagedBytes.Reader bytes;
    private final PackedInts.Reader docToOffset;
    private final Bits docsWithField;

    public BinaryDocValuesImpl(PagedBytes.Reader bytes, PackedInts.Reader docToOffset, Bits docsWithField) {
      this.bytes = bytes;
      this.docToOffset = docToOffset;
      this.docsWithField = docsWithField;
    }
    
    public BinaryDocValues iterator() {
      return new BinaryDocValues() {

        final BytesRef term = new BytesRef();
        
        int docID = -1;

        @Override
        public int docID() {
          return docID;
        }

        @Override
        public int nextDoc() {
          while (true) {
            docID++;
            if (docID >= docToOffset.size()) {
              docID = NO_MORE_DOCS;
              return docID;
            }
            if (docsWithField.get(docID)) {
              return docID;
            }
          }
        }

        @Override
        public int advance(int target) {
          if (target < docToOffset.size()) {
            docID = target;
            if (docsWithField.get(docID)) {
              return docID;
            } else{
              return nextDoc();
            }
          } else {
            docID = NO_MORE_DOCS;
            return docID;
          }
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          docID = target;
          return docsWithField.get(docID);
        }

        @Override
        public long cost() {
          return 0;
        }

        @Override
        public BytesRef binaryValue() {
          final long pointer = docToOffset.get(docID);
          if (pointer == 0) {
            term.length = 0;
          } else {
            bytes.fill(term, pointer);
          }
          return term;
        }   
      };
    }

    @Override
    public long ramBytesUsed() {
      return bytes.ramBytesUsed() + docToOffset.ramBytesUsed() + 2*RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      List<Accountable> resources = new ArrayList<>(2);
      resources.add(Accountables.namedAccountable("term bytes", bytes));
      resources.add(Accountables.namedAccountable("addresses", docToOffset));
      return Collections.unmodifiableList(resources);
    }
  }

  // TODO: this if DocTermsIndex was already created, we
  // should share it...
  public BinaryDocValues getTerms(LeafReader reader, String field) throws IOException {
    return getTerms(reader, field, PackedInts.FAST);
  }

  public BinaryDocValues getTerms(LeafReader reader, String field, float acceptableOverheadRatio) throws IOException {
    BinaryDocValues valuesIn = reader.getBinaryDocValues(field);
    if (valuesIn != null) {
      // Not cached here by FieldCacheImpl (cached instead
      // per-thread by SegmentReader):
      return valuesIn;
    }

    final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
    if (info == null) {
      return DocValues.emptyBinary();
    } else if (info.getDocValuesType() != DocValuesType.NONE) {
      throw new IllegalStateException("Type mismatch: " + field + " was indexed as " + info.getDocValuesType());
    } else if (info.getIndexOptions() == IndexOptions.NONE) {
      return DocValues.emptyBinary();
    }

    BinaryDocValuesImpl impl = (BinaryDocValuesImpl) caches.get(BinaryDocValues.class).get(reader, new CacheKey(field, acceptableOverheadRatio));
    return impl.iterator();
  }

  static final class BinaryDocValuesCache extends Cache {
    BinaryDocValuesCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(LeafReader reader, CacheKey key)
        throws IOException {

      // TODO: would be nice to first check if DocTermsIndex
      // was already cached for this field and then return
      // that instead, to avoid insanity

      final int maxDoc = reader.maxDoc();
      Terms terms = reader.terms(key.field);

      final float acceptableOverheadRatio = ((Float) key.custom).floatValue();

      final int termCountHardLimit = maxDoc;

      // Holds the actual term data, expanded.
      final PagedBytes bytes = new PagedBytes(15);

      int startBPV;

      if (terms != null) {
        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = terms.size();
        if (numUniqueTerms != -1L) {
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

      final GrowableWriter docToOffset = new GrowableWriter(startBPV, maxDoc, acceptableOverheadRatio);
      
      // pointer==0 means not set
      bytes.copyUsingLengthPrefix(new BytesRef());

      if (terms != null) {
        int termCount = 0;
        final TermsEnum termsEnum = terms.iterator();
        PostingsEnum docs = null;
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
          docs = termsEnum.postings(docs, PostingsEnum.NONE);
          while (true) {
            final int docID = docs.nextDoc();
            if (docID == DocIdSetIterator.NO_MORE_DOCS) {
              break;
            }
            docToOffset.set(docID, pointer);
          }
        }
      }

      final PackedInts.Reader offsetReader = docToOffset.getMutable();
      Bits docsWithField = new Bits() {
        @Override
        public boolean get(int index) {
          return offsetReader.get(index) != 0;
        }

        @Override
        public int length() {
          return maxDoc;
        }
      };

      wrapper.setDocsWithField(reader, key.field, docsWithField, null);
      // maybe an int-only impl?
      return new BinaryDocValuesImpl(bytes.freeze(true), offsetReader, docsWithField);
    }
  }

  // TODO: this if DocTermsIndex was already created, we
  // should share it...
  public SortedSetDocValues getDocTermOrds(LeafReader reader, String field, BytesRef prefix) throws IOException {
    // not a general purpose filtering mechanism...
    assert prefix == null || prefix == INT32_TERM_PREFIX || prefix == INT64_TERM_PREFIX;
    
    SortedSetDocValues dv = reader.getSortedSetDocValues(field);
    if (dv != null) {
      return dv;
    }
    
    SortedDocValues sdv = reader.getSortedDocValues(field);
    if (sdv != null) {
      return DocValues.singleton(sdv);
    }
    
    final FieldInfo info = reader.getFieldInfos().fieldInfo(field);
    if (info == null) {
      return DocValues.emptySortedSet();
    } else if (info.getDocValuesType() != DocValuesType.NONE) {
      throw new IllegalStateException("Type mismatch: " + field + " was indexed as " + info.getDocValuesType());
    } else if (info.getIndexOptions() == IndexOptions.NONE) {
      return DocValues.emptySortedSet();
    }
    
    // ok we need to uninvert. check if we can optimize a bit.
    
    Terms terms = reader.terms(field);
    if (terms == null) {
      return DocValues.emptySortedSet();
    } else {
      // if #postings = #docswithfield we know that the field is "single valued enough".
      // it's possible the same term might appear twice in the same document, but SORTED_SET discards frequency.
      // it's still ok with filtering (which we limit to numerics), it just means precisionStep = Inf
      long numPostings = terms.getSumDocFreq();
      if (numPostings != -1 && numPostings == terms.getDocCount()) {
        return DocValues.singleton(getTermsIndex(reader, field));
      }
    }
    
    DocTermOrds dto = (DocTermOrds) caches.get(DocTermOrds.class).get(reader, new CacheKey(field, prefix));
    return dto.iterator(reader);
  }

  static final class DocTermOrdsCache extends Cache {
    DocTermOrdsCache(FieldCacheImpl wrapper) {
      super(wrapper);
    }

    @Override
    protected Accountable createValue(LeafReader reader, CacheKey key)
        throws IOException {
      BytesRef prefix = (BytesRef) key.custom;
      return new DocTermOrds(reader, null, key.field, prefix);
    }
  }

}

