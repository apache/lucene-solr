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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.TermFacetCache.FacetCacheKey;
import org.apache.solr.request.TermFacetCache.SegmentCacheEntry;
import static org.apache.solr.request.TermFacetCache.mergeCachedSegmentCounts;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.QueryResultKey;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.facet.FacetDebugInfo;
import org.apache.solr.util.LongPriorityQueue;

/**
 * Computes term facets for docvalues field (single or multivalued).
 * <p>
 * This is basically a specialized case of the code in SimpleFacets.
 * Instead of working on a top-level reader view (binary-search per docid),
 * it collects per-segment, but maps ordinals to global ordinal space using
 * MultiDocValues' OrdinalMap.
 * <p>
 * This means the ordinal map is created per-reopen: O(nterms), but this may
 * perform better than PerSegmentSingleValuedFaceting which has to merge O(nterms)
 * per query. Additionally it works for multi-valued fields.
 */
public class DocValuesFacets {
  private DocValuesFacets() {}
  
  public static NamedList<Integer> getCounts(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort, String prefix, Predicate<BytesRef> termFilter, FacetDebugInfo fdebug, long facetCacheThreshold, ResponseBuilder rb) throws IOException {
    SchemaField schemaField = searcher.getSchema().getField(fieldName);
    FieldType ft = schemaField.getType();
    NamedList<Integer> res = new NamedList<>();
    
    // TODO: remove multiValuedFieldCache(), check dv type / uninversion type?
    final boolean multiValued = schemaField.multiValued() || ft.multiValuedFieldCache();

    final SortedSetDocValues si; // for term lookups only
    OrdinalMap ordinalMap = null; // for mapping per-segment ords to global ones
    if (multiValued) {
      si = searcher.getSlowAtomicReader().getSortedSetDocValues(fieldName);
      if (si instanceof MultiDocValues.MultiSortedSetDocValues) {
        ordinalMap = ((MultiSortedSetDocValues)si).mapping;
      }
    } else {
      SortedDocValues single = searcher.getSlowAtomicReader().getSortedDocValues(fieldName);
      si = single == null ? null : DocValues.singleton(single);
      if (single instanceof MultiDocValues.MultiSortedDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedDocValues)single).mapping;
      }
    }
    if (si == null) {
      return finalize(res, searcher, schemaField, docs, -1, missing);
    }
    if (si.getValueCount() >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Currently this faceting method is limited to " + Integer.MAX_VALUE + " unique terms");
    }

    final BytesRefBuilder prefixRef;
    if (prefix == null) {
      prefixRef = null;
    } else if (prefix.length()==0) {
      prefix = null;
      prefixRef = null;
    } else {
      prefixRef = new BytesRefBuilder();
      prefixRef.copyChars(prefix);
    }
    
    int startTermIndex, endTermIndex;
    final boolean doMissing;
    if (prefix!=null) {
      startTermIndex = (int) si.lookupTerm(prefixRef.get());
      if (startTermIndex<0) startTermIndex=-startTermIndex-1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = (int) si.lookupTerm(prefixRef.get());
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex-1;
      doMissing = false;
    } else {
      startTermIndex=0;
      endTermIndex=(int) si.getValueCount();
      doMissing = true;
    }

    final int nTerms=endTermIndex-startTermIndex;
    final int maxSlots = doMissing ? nTerms + 1 : nTerms;
    int missingCount = -1; 
    final CharsRefBuilder charsRef = new CharsRefBuilder();
    if (maxSlots>0 && docs.size() >= mincount) {
      Map<CacheKey, SegmentCacheEntry> segmentCache = null;
      if (facetCacheThreshold >= 0 && docs.size() > facetCacheThreshold) {
        SolrCache<FacetCacheKey, Map<CacheKey, SegmentCacheEntry>> facetCache = searcher.getCache(TermFacetCache.NAME);
        if (facetCache != null) {
          FacetCacheKey facetCacheKey = new FacetCacheKey(new QueryResultKey(rb.getQuery(), rb.getFilters(), null, 0), fieldName);
          segmentCache = facetCache.get(facetCacheKey);
          if (segmentCache == null) {
            segmentCache = new HashMap<>();
            facetCache.put(facetCacheKey, segmentCache);
          }
        }
      }

      // count collection array only needs to be as big as the number of terms we are
      // going to collect counts for.
      final int[] counts;
      if (fdebug != null) {
        fdebug.putInfoItem("numBuckets", maxSlots);
      }

      Filter filter = docs.getTopFilter();
      SegmentCacheEntry topLevelCacheEntry;
      CacheKey topLevelReaderKey = searcher.getIndexReader().getReaderCacheHelper().getKey();
      if (segmentCache != null && (topLevelCacheEntry = segmentCache.get(topLevelReaderKey)) != null) {
        counts = topLevelCacheEntry.topLevelCounts;
      } else {
        counts = new int[maxSlots];
        List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
        for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
          LeafReaderContext leaf = leaves.get(subIndex);
          CacheKey segKey = null;
          if (segmentCache != null) {
            segKey = leaf.reader().getReaderCacheHelper().getKey();
            SegmentCacheEntry cacheEntry = segmentCache.get(segKey);
            if (cacheEntry != null) {
              mergeCachedSegmentCounts(counts, cacheEntry.counts, ordinalMap.getGlobalOrds(subIndex));
              continue;
            }
          }
          DocIdSet dis = filter.getDocIdSet(leaf, null); // solr docsets already exclude any deleted docs
          DocIdSetIterator disi = null;
          if (dis != null) {
            disi = dis.iterator();
          }
          if (disi != null) {
            byte[] cacheSegCounts;
            if (multiValued) {
              SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(fieldName);
              if (sub == null) {
                sub = DocValues.emptySortedSet();
              }
              final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
              if (singleton != null) {
                // some codecs may optimize SORTED_SET storage for single-valued fields
                cacheSegCounts = accumSingle(counts, doMissing, startTermIndex, singleton, disi, subIndex, ordinalMap, segmentCache != null);
              } else {
                cacheSegCounts = accumMulti(counts, doMissing, startTermIndex, sub, disi, subIndex, ordinalMap, segmentCache != null);
              }
            } else {
              SortedDocValues sub = leaf.reader().getSortedDocValues(fieldName);
              if (sub == null) {
                sub = DocValues.emptySorted();
              }
              cacheSegCounts = accumSingle(counts, doMissing, startTermIndex, sub, disi, subIndex, ordinalMap, segmentCache != null);
            }
            if (cacheSegCounts != null) {
              segmentCache.put(segKey, new SegmentCacheEntry(cacheSegCounts));
            }
          }
        }

        if (segmentCache != null) {
          segmentCache.put(topLevelReaderKey, new SegmentCacheEntry(counts));
        }
      }

      if (doMissing) {
        missingCount = counts[counts.length - 1];
      }

      // IDEA: we could also maintain a count of "other"... everything that fell outside
      // of the top 'N'

      if (limit == 0) {
        return finalize(res, searcher, schemaField, docs, missingCount, missing);
      }

      int off=offset;
      int lim=limit>=0 ? limit : Integer.MAX_VALUE;

      if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
        maxsize = Math.min(maxsize, nTerms);
        LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

        int min=mincount-1;  // the smallest value in the top 'N' values
        for (int i=0; i<nTerms; i++) {
          int c = counts[i];
          if (c>min) {
            // NOTE: we use c>min rather than c>=min as an optimization because we are going in
            // index order, so we already know that the keys are ordered.  This can be very
            // important if a lot of the counts are repeated (like zero counts would be).

            if (termFilter != null) {
              final BytesRef term = si.lookupOrd(startTermIndex+i);
              if (!termFilter.test(term)) {
                continue;
              }
            }

            // smaller term numbers sort higher, so subtract the term number instead
            long pair = (((long)c)<<32) + (Integer.MAX_VALUE - i);
            boolean displaced = queue.insert(pair);
            if (displaced) min=(int)(queue.top() >>> 32);
          }
        }

        // if we are deep paging, we don't have to order the highest "offset" counts.
        int collectCount = Math.max(0, queue.size() - off);
        assert collectCount <= lim;

        // the start and end indexes of our list "sorted" (starting with the highest value)
        int sortedIdxStart = queue.size() - (collectCount - 1);
        int sortedIdxEnd = queue.size() + 1;
        final long[] sorted = queue.sort(collectCount);

        for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
          long pair = sorted[i];
          int c = (int)(pair >>> 32);
          int tnum = Integer.MAX_VALUE - (int)pair;
          final BytesRef term = si.lookupOrd(startTermIndex+tnum);
          ft.indexedToReadable(term, charsRef);
          res.add(charsRef.toString(), c);
        }
      
      } else {
        // add results in index order
        int i = 0;
        if (mincount<=0 && termFilter == null) {
          // if mincount<=0 and we're not examining the values for the term filter, then
          // we won't discard any terms and we know exactly where to start.
          i+=off;
          off=0;
        }

        for (; i<nTerms; i++) {          
          int c = counts[i];
          if (c<mincount) continue;
          BytesRef term = null;
          if (termFilter != null) {
            term = si.lookupOrd(startTermIndex+i);
            if (!termFilter.test(term)) {
              continue;
            }
          }
          if (--off>=0) continue;
          if (--lim<0) break;
          if (term == null) {
            term = si.lookupOrd(startTermIndex+i);
          }
          ft.indexedToReadable(term, charsRef);
          res.add(charsRef.toString(), c);
        }
      }
    }
    return finalize(res, searcher, schemaField, docs, missingCount, missing);
  }
  
  /** finalizes result: computes missing count if applicable */
  static NamedList<Integer> finalize(NamedList<Integer> res, SolrIndexSearcher searcher, SchemaField schemaField, DocSet docs, int missingCount, boolean missing) throws IOException {
    if (missing) {
      if (missingCount < 0) {
        missingCount = SimpleFacets.getFieldMissingCount(searcher,docs,schemaField.getName());
      }
      res.add(null, missingCount);
    }
    
    return res;
  }
  
  /** accumulates per-segment single-valued facet counts */
  static byte[] accumSingle(int counts[], boolean doMissing, int startTermIndex, SortedDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map, boolean cachePerSeg) throws IOException {
    if (doMissing && (map == null || cachePerSeg || si.getValueCount() < disi.cost()*10)) {
      // no prefixing, not too many unique values wrt matching docs (lucene/facets heuristic): 
      //   collect separately per-segment, then map to global ords
      return accumSingleSeg(counts, si, disi, subIndex, map, cachePerSeg);
    } else {
      // otherwise: do collect+map on the fly
      accumSingleGeneric(counts, doMissing, startTermIndex, si, disi, subIndex, map);
      return null;
    }
  }
  
  /** accumulates per-segment single-valued facet counts, mapping to global ordinal space on-the-fly */
  static void accumSingleGeneric(int counts[], boolean doMissing, int startTermIndex, SortedDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    final LongValues ordmap = map == null ? null : map.getGlobalOrds(subIndex);
    final int missingIdx = doMissing ? counts.length - 1 : -1;
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int term;
      if (si.advanceExact(doc)) {
        term = si.ordValue();
      } else {
        if (doMissing) {
          counts[missingIdx]++;
        }
        continue;
      }
      if (map != null) {
        term = (int) ordmap.get(term);
      }
      int arrIdx = term-startTermIndex;
      if (arrIdx>=0 && arrIdx<counts.length) counts[arrIdx]++;
    }
  }
  
  /** "typical" single-valued faceting: not too many unique values, no prefixing. maps to global ordinals as a separate step */
  static byte[] accumSingleSeg(int counts[], SortedDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map, boolean cachePerSeg) throws IOException {
    // First count in seg-ord space:
    final int segCounts[];
    if (map == null) {
      segCounts = counts;
    } else {
      segCounts = new int[1+si.getValueCount()];
    }
    
    final int missingIdx = segCounts.length - 1;
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (si.advanceExact(doc)) {
        segCounts[si.ordValue()]++;
      } else {
        segCounts[missingIdx]++;
      }
    }
    
    // migrate to global ords (if necessary)
    if (map != null) {
      migrateGlobal(counts, segCounts, map.getGlobalOrds(subIndex));
    }
    if (!cachePerSeg) {
      return null;
    } else {
      ByteBuffersDataOutput cachedSegCountsBuilder = new ByteBuffersDataOutput(segCounts.length * 2);
      return TermFacetCache.encodeCounts(segCounts, cachedSegCountsBuilder);
    }
  }
  
  /** accumulates per-segment multi-valued facet counts */
  static byte[] accumMulti(int counts[], boolean doMissing, int startTermIndex, SortedSetDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map, boolean cachePerSeg) throws IOException {
    if (doMissing && (map == null || cachePerSeg || si.getValueCount() < disi.cost()*10)) {
      // no prefixing, not too many unique values wrt matching docs (lucene/facets heuristic): 
      //   collect separately per-segment, then map to global ords
      return accumMultiSeg(counts, si, disi, subIndex, map, cachePerSeg);
    } else {
      // otherwise: do collect+map on the fly
      accumMultiGeneric(counts, doMissing, startTermIndex, si, disi, subIndex, map);
      return null;
    }
  }
    
  /** accumulates per-segment multi-valued facet counts, mapping to global ordinal space on-the-fly */
  static void accumMultiGeneric(int counts[], boolean doMissing, int startTermIndex, SortedSetDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    final LongValues ordMap = map == null ? null : map.getGlobalOrds(subIndex);
    final int missingIdx = doMissing ? counts.length - 1 : -1;
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (si.advanceExact(doc)) {
        // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
        int term = (int) si.nextOrd();
        do {
          if (map != null) {
            term = (int) ordMap.get(term);
          }
          int arrIdx = term-startTermIndex;
          if (arrIdx>=0 && arrIdx<counts.length) counts[arrIdx]++;
        } while ((term = (int) si.nextOrd()) >= 0);
      } else if (doMissing) {
        counts[missingIdx]++; // missing count
      }
    }
  }
  
  /** "typical" multi-valued faceting: not too many unique values, no prefixing. maps to global ordinals as a separate step */
  static byte[] accumMultiSeg(int counts[], SortedSetDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map, boolean cachePerSeg) throws IOException {
    // First count in seg-ord space:
    final int segCounts[];
    if (map == null) {
      segCounts = counts;
    } else {
      segCounts = new int[1+(int)si.getValueCount()];
    }
    
    final int missingIdx = segCounts.length - 1;
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (si.advanceExact(doc)) {
        int term = (int) si.nextOrd();
        do {
          segCounts[term]++;
        } while ((term = (int)si.nextOrd()) >= 0);
      } else {
        segCounts[missingIdx]++; // missing
      }
    }
    
    // migrate to global ords (if necessary)
    if (map != null) {
      migrateGlobal(counts, segCounts, map.getGlobalOrds(subIndex));
    }
    if (!cachePerSeg) {
      return null;
    } else {
      ByteBuffersDataOutput cachedSegCountsBuilder = new ByteBuffersDataOutput(segCounts.length * 2);
      return TermFacetCache.encodeCounts(segCounts, cachedSegCountsBuilder);
    }
  }
  
  /** folds counts in segment ordinal space (segCounts) into global ordinal space (counts) */
  public static void migrateGlobal(int counts[], int segCounts[], LongValues ordMap) {
    // missing count
    int ord = segCounts.length - 1;
    counts[counts.length - 1] += segCounts[ord];
    
    // migrate actual ordinals
    while (ord-- > 0) {
      int count = segCounts[ord];
      if (count != 0) {
        counts[(int) ordMap.get(ord)] += count;
      }
    }
  }
}
