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
package org.apache.solr.request;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;

/** Utility class to compute facets on numeric fields. */
final class NumericFacets {

  NumericFacets() {}

  static class HashTable {

    static final float LOAD_FACTOR = 0.7f;

    long[] bits; // bits identifying a value
    int[] counts;
    int[] docIDs; //Will be null if HashTable is created with needsDocId=false
    int mask;
    int size;
    int threshold;

    HashTable(boolean needsDocId) {
      final int capacity = 64; // must be a power of 2
      bits = new long[capacity];
      counts = new int[capacity];
      if (needsDocId) {
        docIDs = new int[capacity];
      }
      mask = capacity - 1;
      size = 0;
      threshold = (int) (capacity * LOAD_FACTOR);
    }

    private int hash(long v) {
      int h = (int) (v ^ (v >>> 32));
      h = (31 * h) & mask; // * 31 to try to use the whole table, even if values are dense
      return h;
    }

    void add(int docID, long value, int count) {
      if (size >= threshold) {
        rehash();
      }
      final int h = hash(value);
      for (int slot = h; ; slot = (slot + 1) & mask) {
        if (counts[slot] == 0) {
          bits[slot] = value;
          docIDs[slot] = docID;
          ++size;
        } else if (bits[slot] != value) {
          continue;
        }
        counts[slot] += count;
        break;
      }
    }
    
    void add(long value, int count) {
      if (size >= threshold) {
        rehash();
      }
      final int h = hash(value);
      for (int slot = h; ; slot = (slot + 1) & mask) {
        if (counts[slot] == 0) {
          bits[slot] = value;
          ++size;
        } else if (bits[slot] != value) {
          continue;
        }
        counts[slot] += count;
        break;
      }
    }

    private void rehash() {
      final long[] oldBits = bits;
      final int[] oldCounts = counts;
      final int[] oldDocIDs = docIDs;

      final int newCapacity = bits.length * 2;
      bits = new long[newCapacity];
      counts = new int[newCapacity];
      if (oldDocIDs!= null) {
        docIDs = new int[newCapacity];
      }
      mask = newCapacity - 1;
      threshold = (int) (LOAD_FACTOR * newCapacity);
      size = 0;

      if (oldDocIDs!= null) {
        for (int i = 0; i < oldBits.length; ++i) {
          if (oldCounts[i] > 0) {
            add(oldDocIDs[i], oldBits[i], oldCounts[i]);
          }
        }
      } else {
        for (int i = 0; i < oldBits.length; ++i) {
          if (oldCounts[i] > 0) {
            add(oldBits[i], oldCounts[i]);
          }
        }
      }
    }

  }

  private static class Entry {
    int docID;
    int count;
    long bits;
  }

  public static NamedList<Integer> getCounts(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort) throws IOException {
    final SchemaField sf = searcher.getSchema().getField(fieldName);
    if (sf.multiValued()) {
      // TODO: evaluate using getCountsMultiValued for singleValued numerics with SingletonSortedNumericDocValues
      return getCountsMultiValued(searcher, docs, fieldName, offset, limit, mincount, missing, sort);
    }
    return getCountsSingleValue(searcher, docs, fieldName, offset, limit, mincount, missing, sort);
  }
  
  private static NamedList<Integer> getCountsSingleValue(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort) throws IOException {
    boolean zeros = mincount <= 0;
    mincount = Math.max(mincount, 1);
    final SchemaField sf = searcher.getSchema().getField(fieldName);
    final FieldType ft = sf.getType();
    final NumberType numericType = ft.getNumberType();
    if (numericType == null) {
      throw new IllegalStateException();
    }
    zeros = zeros && !ft.isPointField() && sf.indexed(); // We don't return zeros when using PointFields or when index=false
    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    // 1. accumulate
    final HashTable hashTable = new HashTable(true);
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    NumericDocValues longs = null;
    int missingCount = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
        do {
          ctx = ctxIt.next();
        } while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
        assert doc >= ctx.docBase;
        switch (numericType) {
          case LONG:
          case DATE:
          case INTEGER:
            // Long, Date and Integer
            longs = DocValues.getNumeric(ctx.reader(), fieldName);
            break;
          case FLOAT:
            // TODO: this bit flipping should probably be moved to tie-break in the PQ comparator
            longs = new FilterNumericDocValues(DocValues.getNumeric(ctx.reader(), fieldName)) {
              @Override
              public long longValue() throws IOException {
                long bits = super.longValue();
                if (bits < 0) bits ^= 0x7fffffffffffffffL;
                return bits;
              }
            };
            break;
          case DOUBLE:
            // TODO: this bit flipping should probably be moved to tie-break in the PQ comparator
            longs = new FilterNumericDocValues(DocValues.getNumeric(ctx.reader(), fieldName)) {
              @Override
              public long longValue() throws IOException {
                long bits = super.longValue();
                if (bits < 0) bits ^= 0x7fffffffffffffffL;
                return bits;
              }
            };
            break;
          default:
            throw new AssertionError("Unexpected type: " + numericType);
        }
      }
      int valuesDocID = longs.docID();
      if (valuesDocID < doc - ctx.docBase) {
        valuesDocID = longs.advance(doc - ctx.docBase);
      }
      if (valuesDocID == doc - ctx.docBase) {
        hashTable.add(doc, longs.longValue(), 1);
      } else {
        ++missingCount;
      }
    }

    final NamedList<Integer> result = new NamedList<>();
    if (limit == 0) {
      return finalize(result, missingCount, missing);
    }

    // 2. select top-k facet values
    final int pqSize = limit < 0 ? hashTable.size : Math.min(offset + limit, hashTable.size);
    final PriorityQueue<Entry> pq;
    if (FacetParams.FACET_SORT_COUNT.equals(sort) || FacetParams.FACET_SORT_COUNT_LEGACY.equals(sort)) {
      pq = new PriorityQueue<Entry>(pqSize) {
        @Override
        protected boolean lessThan(Entry a, Entry b) {
          if (a.count < b.count || (a.count == b.count && a.bits > b.bits)) {
            return true;
          } else {
            return false;
          }
        }
      };
    } else {
      pq = new PriorityQueue<Entry>(pqSize) {
        @Override
        protected boolean lessThan(Entry a, Entry b) {
          return a.bits > b.bits;
        }
      };
    }
    Entry e = null;
    for (int i = 0; i < hashTable.bits.length; ++i) {
      if (hashTable.counts[i] >= mincount) {
        if (e == null) {
          e = new Entry();
        }
        e.bits = hashTable.bits[i];
        e.count = hashTable.counts[i];
        e.docID = hashTable.docIDs[i];
        e = pq.insertWithOverflow(e);
      }
    }

    // 4. build the NamedList
    final ValueSource vs = ft.getValueSource(sf, null);

    // This stuff is complicated because if facet.mincount=0, the counts needs
    // to be merged with terms from the terms dict
    if (!zeros || FacetParams.FACET_SORT_COUNT.equals(sort) || FacetParams.FACET_SORT_COUNT_LEGACY.equals(sort)) {
      // Only keep items we're interested in
      final Deque<Entry> counts = new ArrayDeque<>();
      while (pq.size() > offset) {
        counts.addFirst(pq.pop());
      }
      
      // Entries from the PQ first, then using the terms dictionary
      for (Entry entry : counts) {
        final int readerIdx = ReaderUtil.subIndex(entry.docID, leaves);
        final FunctionValues values = vs.getValues(Collections.emptyMap(), leaves.get(readerIdx));
        result.add(values.strVal(entry.docID - leaves.get(readerIdx).docBase), entry.count);
      }

      if (zeros && (limit < 0 || result.size() < limit)) { // need to merge with the term dict
        if (!sf.indexed() && !sf.hasDocValues()) {
          throw new IllegalStateException("Cannot use " + FacetParams.FACET_MINCOUNT + "=0 on field " + sf.getName() + " which is neither indexed nor docValues");
        }
        // Add zeros until there are limit results
        final Set<String> alreadySeen = new HashSet<>();
        while (pq.size() > 0) {
          Entry entry = pq.pop();
          final int readerIdx = ReaderUtil.subIndex(entry.docID, leaves);
          final FunctionValues values = vs.getValues(Collections.emptyMap(), leaves.get(readerIdx));
          alreadySeen.add(values.strVal(entry.docID - leaves.get(readerIdx).docBase));
        }
        for (int i = 0; i < result.size(); ++i) {
          alreadySeen.add(result.getName(i));
        }
        final Terms terms = searcher.getSlowAtomicReader().terms(fieldName);
        if (terms != null) {
          final String prefixStr = TrieField.getMainValuePrefix(ft);
          final BytesRef prefix;
          if (prefixStr != null) {
            prefix = new BytesRef(prefixStr);
          } else {
            prefix = new BytesRef();
          }
          final TermsEnum termsEnum = terms.iterator();
          BytesRef term;
          switch (termsEnum.seekCeil(prefix)) {
            case FOUND:
            case NOT_FOUND:
              term = termsEnum.term();
              break;
            case END:
              term = null;
              break;
            default:
              throw new AssertionError();
          }
          final CharsRefBuilder spare = new CharsRefBuilder();
          for (int skipped = hashTable.size; skipped < offset && term != null && StringHelper.startsWith(term, prefix); ) {
            ft.indexedToReadable(term, spare);
            final String termStr = spare.toString();
            if (!alreadySeen.contains(termStr)) {
              ++skipped;
            }
            term = termsEnum.next();
          }
          for ( ; term != null && StringHelper.startsWith(term, prefix) && (limit < 0 || result.size() < limit); term = termsEnum.next()) {
            ft.indexedToReadable(term, spare);
            final String termStr = spare.toString();
            if (!alreadySeen.contains(termStr)) {
              result.add(termStr, 0);
            }
          }
        }
      }
    } else {
      // sort=index, mincount=0 and we have less than limit items
      // => Merge the PQ and the terms dictionary on the fly
      if (!sf.indexed()) {
        throw new IllegalStateException("Cannot use " + FacetParams.FACET_SORT + "=" + FacetParams.FACET_SORT_INDEX + " on a field which is not indexed");
      }
      final Map<String, Integer> counts = new HashMap<>();
      while (pq.size() > 0) {
        final Entry entry = pq.pop();
        final int readerIdx = ReaderUtil.subIndex(entry.docID, leaves);
        final FunctionValues values = vs.getValues(Collections.emptyMap(), leaves.get(readerIdx));
        counts.put(values.strVal(entry.docID - leaves.get(readerIdx).docBase), entry.count);
      }
      final Terms terms = searcher.getSlowAtomicReader().terms(fieldName);
      if (terms != null) {
        final String prefixStr = TrieField.getMainValuePrefix(ft);
        final BytesRef prefix;
        if (prefixStr != null) {
          prefix = new BytesRef(prefixStr);
        } else {
          prefix = new BytesRef();
        }
        final TermsEnum termsEnum = terms.iterator();
        BytesRef term;
        switch (termsEnum.seekCeil(prefix)) {
          case FOUND:
          case NOT_FOUND:
            term = termsEnum.term();
            break;
          case END:
            term = null;
            break;
          default:
            throw new AssertionError();
        }
        final CharsRefBuilder spare = new CharsRefBuilder();
        for (int i = 0; i < offset && term != null && StringHelper.startsWith(term, prefix); ++i) {
          term = termsEnum.next();
        }
        for ( ; term != null && StringHelper.startsWith(term, prefix) && (limit < 0 || result.size() < limit); term = termsEnum.next()) {
          ft.indexedToReadable(term, spare);
          final String termStr = spare.toString();
          Integer count = counts.get(termStr);
          if (count == null) {
            count = 0;
          }
          result.add(termStr, count);
        }
      }
    }

    return finalize(result, missingCount, missing);
  }

  private static NamedList<Integer> getCountsMultiValued(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort) throws IOException {
    // If facet.mincount=0 with PointFields the only option is to get the values from DocValues
    // not currently supported. See SOLR-11174
    mincount = Math.max(mincount, 1);
    final SchemaField sf = searcher.getSchema().getField(fieldName);
    final FieldType ft = sf.getType();
    assert sf.multiValued();
    final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    // 1. accumulate
    final HashTable hashTable = new HashTable(false);
    final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
    LeafReaderContext ctx = null;
    SortedNumericDocValues longs = null;
    int missingCount = 0;
    for (DocIterator docsIt = docs.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
        do {
          ctx = ctxIt.next();
        } while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
        assert doc >= ctx.docBase;
        longs = DocValues.getSortedNumeric(ctx.reader(), fieldName);
      }
      int valuesDocID = longs.docID();
      if (valuesDocID < doc - ctx.docBase) {
        valuesDocID = longs.advance(doc - ctx.docBase);
      }
      if (valuesDocID == doc - ctx.docBase) {
        long l = longs.nextValue(); // This document must have at least one value
        hashTable.add(l, 1);
        for (int i = 1, count = longs.docValueCount(); i < count; i++) {
          long lnew = longs.nextValue();
          if (lnew > l) { // Skip the value if it's equal to the last one, we don't want to double-count it
            hashTable.add(lnew, 1);
          }
          l = lnew;
         }
        
      } else {
        ++missingCount;
      }
    }

    if (limit == 0) {
      NamedList<Integer> result = new NamedList<>();
      return finalize(result, missingCount, missing);
    }

    // 2. select top-k facet values
    final int pqSize = limit < 0 ? hashTable.size : Math.min(offset + limit, hashTable.size);
    final PriorityQueue<Entry> pq;
    if (FacetParams.FACET_SORT_COUNT.equals(sort) || FacetParams.FACET_SORT_COUNT_LEGACY.equals(sort)) {
      pq = new PriorityQueue<Entry>(pqSize) {
        @Override
        protected boolean lessThan(Entry a, Entry b) {
          if (a.count < b.count || (a.count == b.count && a.bits > b.bits)) {
            return true;
          } else {
            return false;
          }
        }
      };
    } else {
      // sort=index
      pq = new PriorityQueue<Entry>(pqSize) {
        @Override
        protected boolean lessThan(Entry a, Entry b) {
          return a.bits > b.bits;
        }
      };
    }
    Entry e = null;
    for (int i = 0; i < hashTable.bits.length; ++i) {
      if (hashTable.counts[i] >= mincount) {
        if (e == null) {
          e = new Entry();
        }
        e.bits = hashTable.bits[i];
        e.count = hashTable.counts[i];
        e = pq.insertWithOverflow(e);
      }
    }

    // 4. build the NamedList
    final NamedList<Integer> result = new NamedList<>(Math.max(pq.size() - offset + 1, 1));
    final Deque<Entry> counts = new ArrayDeque<>(pq.size() - offset);
    while (pq.size() > offset) {
      counts.addFirst(pq.pop());
    }
    
    for (Entry entry : counts) {
      result.add(bitsToStringValue(ft, entry.bits), entry.count); // TODO: convert to correct value
    }
    
    // Once facet.mincount=0 is supported we'll need to add logic similar to the SingleValue case, but obtaining values
    // with count 0 from DocValues

    return finalize(result, missingCount, missing);
  }

  private static NamedList<Integer> finalize(NamedList<Integer> result, int missingCount, boolean missing) {
    if (missing) {
      result.add(null, missingCount);
    }
    return result;
  }

  private static String bitsToStringValue(FieldType fieldType, long bits) {
    switch (fieldType.getNumberType()) {
      case LONG:
      case INTEGER:
        return String.valueOf(bits);
      case FLOAT:
        return String.valueOf(NumericUtils.sortableIntToFloat((int)bits));
      case DOUBLE:
        return String.valueOf(NumericUtils.sortableLongToDouble(bits));
      case DATE:
        return new Date(bits).toInstant().toString();
      default:
        throw new AssertionError("Unsupported NumberType: " + fieldType.getNumberType());
    }
  }

}
