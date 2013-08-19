package org.apache.solr.request;

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

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.SingletonSortedSetDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
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
  
  public static NamedList<Integer> getCounts(SolrIndexSearcher searcher, DocSet docs, String fieldName, int offset, int limit, int mincount, boolean missing, String sort, String prefix) throws IOException {
    SchemaField schemaField = searcher.getSchema().getField(fieldName);
    FieldType ft = schemaField.getType();
    NamedList<Integer> res = new NamedList<Integer>();

    final SortedSetDocValues si; // for term lookups only
    OrdinalMap ordinalMap = null; // for mapping per-segment ords to global ones
    if (schemaField.multiValued()) {
      si = searcher.getAtomicReader().getSortedSetDocValues(fieldName);
      if (si instanceof MultiSortedSetDocValues) {
        ordinalMap = ((MultiSortedSetDocValues)si).mapping;
      }
    } else {
      SortedDocValues single = searcher.getAtomicReader().getSortedDocValues(fieldName);
      si = single == null ? null : new SingletonSortedSetDocValues(single);
      if (single instanceof MultiSortedDocValues) {
        ordinalMap = ((MultiSortedDocValues)single).mapping;
      }
    }
    if (si == null) {
      return finalize(res, searcher, schemaField, docs, -1, missing);
    }
    if (si.getValueCount() >= Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Currently this faceting method is limited to " + Integer.MAX_VALUE + " unique terms");
    }

    final BytesRef br = new BytesRef();

    final BytesRef prefixRef;
    if (prefix == null) {
      prefixRef = null;
    } else if (prefix.length()==0) {
      prefix = null;
      prefixRef = null;
    } else {
      prefixRef = new BytesRef(prefix);
    }

    int startTermIndex, endTermIndex;
    if (prefix!=null) {
      startTermIndex = (int) si.lookupTerm(prefixRef);
      if (startTermIndex<0) startTermIndex=-startTermIndex-1;
      prefixRef.append(UnicodeUtil.BIG_TERM);
      endTermIndex = (int) si.lookupTerm(prefixRef);
      assert endTermIndex < 0;
      endTermIndex = -endTermIndex-1;
    } else {
      startTermIndex=-1;
      endTermIndex=(int) si.getValueCount();
    }

    final int nTerms=endTermIndex-startTermIndex;
    int missingCount = -1; 
    final CharsRef charsRef = new CharsRef(10);
    if (nTerms>0 && docs.size() >= mincount) {

      // count collection array only needs to be as big as the number of terms we are
      // going to collect counts for.
      final int[] counts = new int[nTerms];

      Filter filter = docs.getTopFilter();
      List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
      for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
        AtomicReaderContext leaf = leaves.get(subIndex);
        DocIdSet dis = filter.getDocIdSet(leaf, null); // solr docsets already exclude any deleted docs
        DocIdSetIterator disi = null;
        if (dis != null) {
          disi = dis.iterator();
        }
        if (disi != null) {
          if (schemaField.multiValued()) {
            SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(fieldName);
            if (sub == null) {
              sub = SortedSetDocValues.EMPTY;
            }
            accumMulti(counts, startTermIndex, sub, disi, subIndex, ordinalMap);
          } else {
            SortedDocValues sub = leaf.reader().getSortedDocValues(fieldName);
            if (sub == null) {
              sub = SortedDocValues.EMPTY;
            }
            accumSingle(counts, startTermIndex, sub, disi, subIndex, ordinalMap);
          }
        }
      }

      if (startTermIndex == -1) {
        missingCount = counts[0];
      }

      // IDEA: we could also maintain a count of "other"... everything that fell outside
      // of the top 'N'

      int off=offset;
      int lim=limit>=0 ? limit : Integer.MAX_VALUE;

      if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
        maxsize = Math.min(maxsize, nTerms);
        LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

        int min=mincount-1;  // the smallest value in the top 'N' values
        for (int i=(startTermIndex==-1)?1:0; i<nTerms; i++) {
          int c = counts[i];
          if (c>min) {
            // NOTE: we use c>min rather than c>=min as an optimization because we are going in
            // index order, so we already know that the keys are ordered.  This can be very
            // important if a lot of the counts are repeated (like zero counts would be).

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
          si.lookupOrd(startTermIndex+tnum, br);
          ft.indexedToReadable(br, charsRef);
          res.add(charsRef.toString(), c);
        }
      
      } else {
        // add results in index order
        int i=(startTermIndex==-1)?1:0;
        if (mincount<=0) {
          // if mincount<=0, then we won't discard any terms and we know exactly
          // where to start.
          i+=off;
          off=0;
        }

        for (; i<nTerms; i++) {          
          int c = counts[i];
          if (c<mincount || --off>=0) continue;
          if (--lim<0) break;
          si.lookupOrd(startTermIndex+i, br);
          ft.indexedToReadable(br, charsRef);
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
  
  /** accumulates per-segment single-valued facet counts, mapping to global ordinal space */
  // specialized since the single-valued case is different
  static void accumSingle(int counts[], int startTermIndex, SortedDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int term = si.getOrd(doc);
      if (map != null && term >= 0) {
        term = (int) map.getGlobalOrd(subIndex, term);
      }
      int arrIdx = term-startTermIndex;
      if (arrIdx>=0 && arrIdx<counts.length) counts[arrIdx]++;
    }
  }
  
  /** accumulates per-segment multi-valued facet counts, mapping to global ordinal space */
  static void accumMulti(int counts[], int startTermIndex, SortedSetDocValues si, DocIdSetIterator disi, int subIndex, OrdinalMap map) throws IOException {
    int doc;
    while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      si.setDocument(doc);
      // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
      int term = (int) si.nextOrd();
      if (term < 0) {
        if (startTermIndex == -1) {
          counts[0]++; // missing count
        }
        continue;
      }
      
      do {
        if (map != null) {
          term = (int) map.getGlobalOrd(subIndex, term);
        }
        int arrIdx = term-startTermIndex;
        if (arrIdx>=0 && arrIdx<counts.length) counts[arrIdx]++;
      } while ((term = (int) si.nextOrd()) >= 0);
    }
  }
}
