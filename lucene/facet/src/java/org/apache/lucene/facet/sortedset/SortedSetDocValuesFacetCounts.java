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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState.OrdRange;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

/** Compute facets counts from previously
 *  indexed {@link SortedSetDocValuesFacetField},
 *  without require a separate taxonomy index.  Faceting is
 *  a bit slower (~25%), and there is added cost on every
 *  {@link IndexReader} open to create a new {@link
 *  SortedSetDocValuesReaderState}.  Furthermore, this does
 *  not support hierarchical facets; only flat (dimension +
 *  label) facets, but it uses quite a bit less RAM to do
 *  so.
 *
 *  <p><b>NOTE</b>: this class should be instantiated and
 *  then used from a single thread, because it holds a
 *  thread-private instance of {@link SortedSetDocValues}.
 * 
 * <p><b>NOTE:</b>: tie-break is by unicode sort order
 *
 * @lucene.experimental */
public class SortedSetDocValuesFacetCounts extends Facets {

  final SortedSetDocValuesReaderState state;
  final SortedSetDocValues dv;
  final String field;
  final int[] counts;

  /** Sparse faceting: returns any dimension that had any
   *  hits, topCount labels per dimension. */
  public SortedSetDocValuesFacetCounts(SortedSetDocValuesReaderState state, FacetsCollector hits)
      throws IOException {
    this.state = state;
    this.field = state.getField();
    dv = state.getDocValues();    
    counts = new int[state.getSize()];
    //System.out.println("field=" + field);
    count(hits.getMatchingDocs());
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    if (topN <= 0) {
      throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
    }
    if (path.length > 0) {
      throw new IllegalArgumentException("path should be 0 length");
    }
    OrdRange ordRange = state.getOrdRange(dim);
    if (ordRange == null) {
      throw new IllegalArgumentException("dimension \"" + dim + "\" was not indexed");
    }
    return getDim(dim, ordRange, topN);
  }

  private final FacetResult getDim(String dim, OrdRange ordRange, int topN) {

    TopOrdAndIntQueue q = null;

    int bottomCount = 0;

    int dimCount = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;
    //System.out.println("getDim : " + ordRange.start + " - " + ordRange.end);
    for(int ord=ordRange.start; ord<=ordRange.end; ord++) {
      //System.out.println("  ord=" + ord + " count=" + counts[ord]);
      if (counts[ord] > 0) {
        dimCount += counts[ord];
        childCount++;
        if (counts[ord] > bottomCount) {
          if (reuse == null) {
            reuse = new TopOrdAndIntQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = counts[ord];
          if (q == null) {
            // Lazy init, so we don't create this for the
            // sparse case unnecessarily
            q = new TopOrdAndIntQueue(topN);
          }
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomCount = q.top().value;
          }
        }
      }
    }

    if (q == null) {
      return null;
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
      final BytesRef term = dv.lookupOrd(ordAndValue.ord);
      String[] parts = FacetsConfig.stringToPath(term.utf8ToString());
      labelValues[i] = new LabelAndValue(parts[1], ordAndValue.value);
    }

    return new FacetResult(dim, new String[0], dimCount, labelValues, childCount);
  }

  /** Does all the "real work" of tallying up the counts. */
  private final void count(List<MatchingDocs> matchingDocs) throws IOException {
    //System.out.println("ssdv count");

    MultiDocValues.OrdinalMap ordinalMap;

    // TODO: is this right?  really, we need a way to
    // verify that this ordinalMap "matches" the leaves in
    // matchingDocs...
    if (dv instanceof MultiSortedSetDocValues && matchingDocs.size() > 1) {
      ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
    } else {
      ordinalMap = null;
    }
    
    IndexReader origReader = state.getOrigReader();

    for(MatchingDocs hits : matchingDocs) {

      LeafReader reader = hits.context.reader();
      //System.out.println("  reader=" + reader);
      // LUCENE-5090: make sure the provided reader context "matches"
      // the top-level reader passed to the
      // SortedSetDocValuesReaderState, else cryptic
      // AIOOBE can happen:
      if (ReaderUtil.getTopLevelContext(hits.context).reader() != origReader) {
        throw new IllegalStateException("the SortedSetDocValuesReaderState provided to this class does not match the reader being searched; you must create a new SortedSetDocValuesReaderState every time you open a new IndexReader");
      }
      
      SortedSetDocValues segValues = reader.getSortedSetDocValues(field);
      if (segValues == null) {
        continue;
      }

      DocIdSetIterator docs = hits.bits.iterator();

      // TODO: yet another option is to count all segs
      // first, only in seg-ord space, and then do a
      // merge-sort-PQ in the end to only "resolve to
      // global" those seg ords that can compete, if we know
      // we just want top K?  ie, this is the same algo
      // that'd be used for merging facets across shards
      // (distributed faceting).  but this has much higher
      // temp ram req'ts (sum of number of ords across all
      // segs)
      if (ordinalMap != null) {
        final int segOrd = hits.context.ord;
        final LongValues ordMap = ordinalMap.getGlobalOrds(segOrd);

        int numSegOrds = (int) segValues.getValueCount();

        if (hits.totalHits < numSegOrds/10) {
          //System.out.println("    remap as-we-go");
          // Remap every ord to global ord as we iterate:
          int doc;
          while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            //System.out.println("    doc=" + doc);
            segValues.setDocument(doc);
            int term = (int) segValues.nextOrd();
            while (term != SortedSetDocValues.NO_MORE_ORDS) {
              //System.out.println("      segOrd=" + segOrd + " ord=" + term + " globalOrd=" + ordinalMap.getGlobalOrd(segOrd, term));
              counts[(int) ordMap.get(term)]++;
              term = (int) segValues.nextOrd();
            }
          }
        } else {
          //System.out.println("    count in seg ord first");

          // First count in seg-ord space:
          final int[] segCounts = new int[numSegOrds];
          int doc;
          while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            //System.out.println("    doc=" + doc);
            segValues.setDocument(doc);
            int term = (int) segValues.nextOrd();
            while (term != SortedSetDocValues.NO_MORE_ORDS) {
              //System.out.println("      ord=" + term);
              segCounts[term]++;
              term = (int) segValues.nextOrd();
            }
          }

          // Then, migrate to global ords:
          for(int ord=0;ord<numSegOrds;ord++) {
            int count = segCounts[ord];
            if (count != 0) {
              //System.out.println("    migrate segOrd=" + segOrd + " ord=" + ord + " globalOrd=" + ordinalMap.getGlobalOrd(segOrd, ord));
              counts[(int) ordMap.get(ord)] += count;
            }
          }
        }
      } else {
        // No ord mapping (e.g., single segment index):
        // just aggregate directly into counts:
        int doc;
        while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          segValues.setDocument(doc);
          int term = (int) segValues.nextOrd();
          while (term != SortedSetDocValues.NO_MORE_ORDS) {
            counts[term]++;
            term = (int) segValues.nextOrd();
          }
        }
      }
    }
  }

  @Override
  public Number getSpecificValue(String dim, String... path) {
    if (path.length != 1) {
      throw new IllegalArgumentException("path must be length=1");
    }
    int ord = (int) dv.lookupTerm(new BytesRef(FacetsConfig.pathToString(dim, path)));
    if (ord < 0) {
      return -1;
    }

    return counts[ord];
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {

    List<FacetResult> results = new ArrayList<>();
    for(Map.Entry<String,OrdRange> ent : state.getPrefixToOrdRange().entrySet()) {
      FacetResult fr = getDim(ent.getKey(), ent.getValue(), topN);
      if (fr != null) {
        results.add(fr);
      }
    }

    // Sort by highest count:
    Collections.sort(results,
                     new Comparator<FacetResult>() {
                       @Override
                       public int compare(FacetResult a, FacetResult b) {
                         if (a.value.intValue() > b.value.intValue()) {
                           return -1;
                         } else if (b.value.intValue() > a.value.intValue()) {
                           return 1;
                         } else {
                           return a.dim.compareTo(b.dim);
                         }
                       }
                     });

    return results;
  }
}
