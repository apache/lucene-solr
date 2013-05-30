package org.apache.lucene.facet.sortedset;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/** A {@link FacetsAccumulator} that uses previously
 *  indexed {@link SortedSetDocValuesFacetFields} to perform faceting,
 *  without require a separate taxonomy index.  Faceting is
 *  a bit slower (~25%), and there is added cost on every
 *  {@link IndexReader} open to create a new {@link
 *  SortedSetDocValuesReaderState}.  Furthermore, this does
 *  not support hierarchical facets; only flat (dimension +
 *  label) facets, but it uses quite a bit less RAM to do so. */
public class SortedSetDocValuesAccumulator extends FacetsAccumulator {

  final SortedSetDocValuesReaderState state;
  final SortedSetDocValues dv;
  final String field;

  public SortedSetDocValuesAccumulator(FacetSearchParams fsp, SortedSetDocValuesReaderState state) throws IOException {
    super(fsp, null, null, new FacetArrays((int) state.getDocValues().getValueCount()));
    this.state = state;
    this.field = state.getField();
    dv = state.getDocValues();

    // Check params:
    for(FacetRequest request : fsp.facetRequests) {
      if (!(request instanceof CountFacetRequest)) {
        throw new IllegalArgumentException("this collector only supports CountFacetRequest; got " + request);
      }
      if (request.categoryPath.length != 1) {
        throw new IllegalArgumentException("this collector only supports depth 1 CategoryPath; got " + request.categoryPath);
      }
      if (request.getDepth() != 1) {
        throw new IllegalArgumentException("this collector only supports depth=1; got " + request.getDepth());
      }
      String dim = request.categoryPath.components[0];

      SortedSetDocValuesReaderState.OrdRange ordRange = state.getOrdRange(dim);
      if (ordRange == null) {
        throw new IllegalArgumentException("dim \"" + dim + "\" does not exist");
      }
    }
  }

  @Override
  public FacetsAggregator getAggregator() {

    return new FacetsAggregator() {

      @Override
      public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException {

        SortedSetDocValues segValues = matchingDocs.context.reader().getSortedSetDocValues(field);
        if (segValues == null) {
          return;
        }

        final int[] counts = facetArrays.getIntArray();
        final int maxDoc = matchingDocs.context.reader().maxDoc();
        assert maxDoc == matchingDocs.bits.length();

        if (dv instanceof MultiSortedSetDocValues) {
          MultiDocValues.OrdinalMap ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
          int segOrd = matchingDocs.context.ord;

          int numSegOrds = (int) segValues.getValueCount();

          if (matchingDocs.totalHits < numSegOrds/10) {
            // Remap every ord to global ord as we iterate:
            int doc = 0;
            while (doc < maxDoc && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
              segValues.setDocument(doc);
              int term = (int) segValues.nextOrd();
              while (term != SortedSetDocValues.NO_MORE_ORDS) {
                counts[(int) ordinalMap.getGlobalOrd(segOrd, term)]++;
                term = (int) segValues.nextOrd();
              }
              ++doc;
            }
          } else {

            // First count in seg-ord space:
            final int[] segCounts = new int[numSegOrds];
            int doc = 0;
            while (doc < maxDoc && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
              segValues.setDocument(doc);
              int term = (int) segValues.nextOrd();
              while (term != SortedSetDocValues.NO_MORE_ORDS) {
                segCounts[term]++;
                term = (int) segValues.nextOrd();
              }
              ++doc;
            }

            // Then, migrate to global ords:
            for(int ord=0;ord<numSegOrds;ord++) {
              int count = segCounts[ord];
              if (count != 0) {
                counts[(int) ordinalMap.getGlobalOrd(segOrd, ord)] += count;
              }
            }
          }
        } else {
          // No ord mapping (e.g., single segment index):
          // just aggregate directly into counts:

          int doc = 0;
          while (doc < maxDoc && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
            segValues.setDocument(doc);
            int term = (int) segValues.nextOrd();
            while (term != SortedSetDocValues.NO_MORE_ORDS) {
              counts[term]++;
              term = (int) segValues.nextOrd();
            }
            ++doc;
          }
        }
      }

      @Override
      public void rollupValues(FacetRequest fr, int ordinal, int[] children, int[] siblings, FacetArrays facetArrays) {
        // Nothing to do here: we only support flat (dim +
        // label) facets, and in accumulate we sum up the
        // count for the dimension.
      }

      @Override
      public boolean requiresDocScores() {
        return false;
      }
    };
  }

  /** Keeps highest count results. */
  static class TopCountPQ extends PriorityQueue<FacetResultNode> {
    public TopCountPQ(int topN) {
      super(topN, false);
    }

    @Override
    protected boolean lessThan(FacetResultNode a, FacetResultNode b) {
      if (a.value < b.value) {
        return true;
      } else if (a.value > b.value) {
        return false;
      } else {
        return a.ordinal > b.ordinal;
      }
    }
  }

  @Override
  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {

    FacetsAggregator aggregator = getAggregator();
    for (CategoryListParams clp : getCategoryLists()) {
      for (MatchingDocs md : matchingDocs) {
        aggregator.aggregate(md, clp, facetArrays);
      }
    }

    // compute top-K
    List<FacetResult> results = new ArrayList<FacetResult>();

    int[] counts = facetArrays.getIntArray();

    BytesRef scratch = new BytesRef();

    for(FacetRequest request : searchParams.facetRequests) {
      String dim = request.categoryPath.components[0];
      SortedSetDocValuesReaderState.OrdRange ordRange = state.getOrdRange(dim);
      // checked in ctor:
      assert ordRange != null;

      if (request.numResults >= ordRange.end - ordRange.start + 1) {
        // specialize this case, user is interested in all available results
        ArrayList<FacetResultNode> nodes = new ArrayList<FacetResultNode>();
        int dimCount = 0;
        for(int ord=ordRange.start; ord<=ordRange.end; ord++) {
          //System.out.println("  ord=" + ord + " count= "+ counts[ord] + " bottomCount=" + bottomCount);
          if (counts[ord] != 0) {
            dimCount += counts[ord];
            FacetResultNode node = new FacetResultNode(ord, counts[ord]);
            dv.lookupOrd(ord, scratch);
            node.label = new CategoryPath(scratch.utf8ToString().split(state.separatorRegex, 2));
            nodes.add(node);
          }
        }

        Collections.sort(nodes, new Comparator<FacetResultNode>() {
            @Override
            public int compare(FacetResultNode o1, FacetResultNode o2) {
              // First by highest count
              int value = (int) (o2.value - o1.value);
              if (value == 0) {
                // ... then by lowest ord:
                value = o1.ordinal - o2.ordinal;
              }
              return value;
            }
          });
      
        CategoryListParams.OrdinalPolicy op = searchParams.indexingParams.getCategoryListParams(request.categoryPath).getOrdinalPolicy(dim);
        if (op == CategoryListParams.OrdinalPolicy.ALL_BUT_DIMENSION) {
          dimCount = 0;
        }

        FacetResultNode rootNode = new FacetResultNode(-1, dimCount);
        rootNode.label = new CategoryPath(new String[] {dim});
        rootNode.subResults = nodes;
        results.add(new FacetResult(request, rootNode, nodes.size()));
        continue;
      }

      TopCountPQ q = new TopCountPQ(request.numResults);

      int bottomCount = 0;

      //System.out.println("collect");
      int dimCount = 0;
      int childCount = 0;
      FacetResultNode reuse = null;
      for(int ord=ordRange.start; ord<=ordRange.end; ord++) {
        //System.out.println("  ord=" + ord + " count= "+ counts[ord] + " bottomCount=" + bottomCount);
        if (counts[ord] > 0) {
          childCount++;
          if (counts[ord] > bottomCount) {
            dimCount += counts[ord];
            //System.out.println("    keep");
            if (reuse == null) {
              reuse = new FacetResultNode(ord, counts[ord]);
            } else {
              reuse.ordinal = ord;
              reuse.value = counts[ord];
            }
            reuse = q.insertWithOverflow(reuse);
            if (q.size() == request.numResults) {
              bottomCount = (int) q.top().value;
              //System.out.println("    new bottom=" + bottomCount);
            }
          }
        }
      }

      CategoryListParams.OrdinalPolicy op = searchParams.indexingParams.getCategoryListParams(request.categoryPath).getOrdinalPolicy(dim);
      if (op == CategoryListParams.OrdinalPolicy.ALL_BUT_DIMENSION) {
        dimCount = 0;
      }

      FacetResultNode rootNode = new FacetResultNode(-1, dimCount);
      rootNode.label = new CategoryPath(new String[] {dim});

      FacetResultNode[] childNodes = new FacetResultNode[q.size()];
      for(int i=childNodes.length-1;i>=0;i--) {
        childNodes[i] = q.pop();
        dv.lookupOrd(childNodes[i].ordinal, scratch);
        childNodes[i].label = new CategoryPath(scratch.utf8ToString().split(state.separatorRegex, 2));
      }
      rootNode.subResults = Arrays.asList(childNodes);
      
      results.add(new FacetResult(request, rootNode, childCount));
    }

    return results;
  }
}
