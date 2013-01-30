package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest.SortBy;
import org.apache.lucene.facet.search.params.FacetRequest.SortOrder;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.ParallelTaxonomyArrays;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.Source;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.encoding.DGapVInt8IntDecoder;

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

/**
 * A {@link Collector} which counts facets associated with matching documents.
 * This {@link Collector} can be used only in the following conditions:
 * <ul>
 * <li>All {@link FacetRequest requests} must be {@link CountFacetRequest}, with
 * their {@link FacetRequest#getDepth() depth} equals to 1, and
 * {@link FacetRequest#getNumLabel()} must be &ge; than
 * {@link FacetRequest#getNumResults()}. Also, their sorting options must be
 * {@link SortOrder#DESCENDING} and {@link SortBy#VALUE} (although ties are
 * broken by ordinals).
 * <li>Partitions should be disabled (
 * {@link FacetIndexingParams#getPartitionSize()} should return
 * Integer.MAX_VALUE).
 * <li>There can be only one {@link CategoryListParams} in the
 * {@link FacetIndexingParams}, with {@link DGapVInt8IntDecoder}.
 * </ul>
 * 
 * <p>
 * <b>NOTE:</b> this colletro uses {@link DocValues#getSource()} by default,
 * which pre-loads the values into memory. If your application cannot afford the
 * RAM, you should use
 * {@link #CountingFacetsCollector(FacetSearchParams, TaxonomyReader, FacetArrays, boolean)}
 * and specify to use a direct source (corresponds to
 * {@link DocValues#getDirectSource()}).
 * 
 * <p>
 * <b>NOTE:</b> this collector supports category lists that were indexed with
 * {@link OrdinalPolicy#NO_PARENTS}, by counting up the parents too, after
 * resolving the leafs counts. Note though that it is your responsibility to
 * guarantee that indeed a document wasn't indexed with two categories that
 * share a common parent, or otherwise the parent's count will be wrong.
 * 
 * @lucene.experimental
 */
public class CountingFacetsCollector extends FacetsCollector {
  
  private final FacetSearchParams fsp;
  private final CategoryListParams clp;
  private final TaxonomyReader taxoReader;
  private final BytesRef buf = new BytesRef(32);
  private final FacetArrays facetArrays;
  private final int[] counts;
  private final String facetsField;
  private final boolean useDirectSource;
  private final HashMap<Source,FixedBitSet> matchingDocs = new HashMap<Source,FixedBitSet>();
  
  private DocValues facetsValues;
  private FixedBitSet bits;
  
  public CountingFacetsCollector(FacetSearchParams fsp, TaxonomyReader taxoReader) {
    this(fsp, taxoReader, new FacetArrays(taxoReader.getSize()), false);
  }
  
  public CountingFacetsCollector(FacetSearchParams fsp, TaxonomyReader taxoReader, FacetArrays facetArrays, 
      boolean useDirectSource) {
    assert facetArrays.arrayLength >= taxoReader.getSize() : "too small facet array";
    assert assertParams(fsp) == null : assertParams(fsp);
    
    this.fsp = fsp;
    this.clp = fsp.indexingParams.getCategoryListParams(fsp.facetRequests.get(0).categoryPath);
    this.facetsField = clp.field;
    this.taxoReader = taxoReader;
    this.facetArrays = facetArrays;
    this.counts = facetArrays.getIntArray();
    this.useDirectSource = useDirectSource;
  }
  
  /**
   * Asserts that this {@link FacetsCollector} can handle the given
   * {@link FacetSearchParams}. Returns {@code null} if true, otherwise an error
   * message.
   */
  static String assertParams(FacetSearchParams fsp) {
    // verify that all facet requests are CountFacetRequest
    for (FacetRequest fr : fsp.facetRequests) {
      if (!(fr instanceof CountFacetRequest)) {
        return "all FacetRequests must be CountFacetRequest";
      }
      if (fr.getDepth() != 1) {
        return "all requests must be of depth 1";
      }
      if (fr.getNumLabel() < fr.getNumResults()) {
        return "this Collector always labels all requested results";
      }
      if (fr.getSortOrder() != SortOrder.DESCENDING) {
        return "this Collector always sorts results in descending order";
      }
      if (fr.getSortBy() != SortBy.VALUE) {
        return "this Collector always sorts by results' values";
      }
    }
    
    // verify that there's only one CategoryListParams for all FacetRequests
    CategoryListParams clp = null;
    for (FacetRequest fr : fsp.facetRequests) {
      CategoryListParams cpclp = fsp.indexingParams.getCategoryListParams(fr.categoryPath);
      if (clp == null) {
        clp = cpclp;
      } else if (clp != cpclp) {
        return "all FacetRequests must belong to the same CategoryListParams";
      }
    }
    if (clp == null) {
      return "at least one FacetRequest must be defined";
    }
    
    // verify DGapVInt decoder
    if (clp.createEncoder().createMatchingDecoder().getClass() != DGapVInt8IntDecoder.class) {
      return "this Collector supports only DGap + VInt encoding";
    }
    
    // verify that partitions are disabled
    if (fsp.indexingParams.getPartitionSize() != Integer.MAX_VALUE) {
      return "this Collector does not support partitions";
    }
    
    return null;
  }
  
  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    facetsValues = context.reader().docValues(facetsField);
    if (facetsValues != null) {
      Source facetSource = useDirectSource ? facetsValues.getDirectSource() : facetsValues.getSource();
      bits = new FixedBitSet(context.reader().maxDoc());
      matchingDocs.put(facetSource, bits);
    }
  }
  
  @Override
  public void collect(int doc) throws IOException {
    if (facetsValues == null) {
      return;
    }
    
    bits.set(doc);
  }
  
  private void countFacets() {
    for (Entry<Source,FixedBitSet> entry : matchingDocs.entrySet()) {
      Source facetsSource = entry.getKey();
      FixedBitSet bits = entry.getValue();
      int doc = 0;
      int length = bits.length();
      while (doc < length && (doc = bits.nextSetBit(doc)) != -1) {
        facetsSource .getBytes(doc, buf);
        if (buf.length > 0) {
          // this document has facets
          int upto = buf.offset + buf.length;
          int ord = 0;
          int offset = buf.offset;
          int prev = 0;
          while (offset < upto) {
            byte b = buf.bytes[offset++];
            if (b >= 0) {
              prev = ord = ((ord << 7) | b) + prev;
              counts[ord]++;
              ord = 0;
            } else {
              ord = (ord << 7) | (b & 0x7F);
            }
          }
        }
        ++doc;
      }
    }
  }

  /**
   * Computes the counts of ordinals under the given ordinal's tree, by
   * recursively going down to leaf nodes and rollin up their counts (called
   * only with categories are indexing with OrdinalPolicy.NO_PARENTS).
   */
  private int rollupCounts(int ordinal, int[] children, int[] siblings) {
    int count = 0;
    while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
      int childCount = counts[ordinal];
      childCount += rollupCounts(children[ordinal], children, siblings);
      counts[ordinal] = childCount;
      count += childCount;
      ordinal = siblings[ordinal];
    }
    return count;
  }

  @Override
  public synchronized List<FacetResult> getFacetResults() throws IOException {
    try {
      // first, count matching documents' facets
      countFacets();
      
      ParallelTaxonomyArrays arrays = taxoReader.getParallelTaxonomyArrays();

      // compute top-K
      final int[] children = arrays.children();
      final int[] siblings = arrays.siblings();
      List<FacetResult> res = new ArrayList<FacetResult>();
      for (FacetRequest fr : fsp.facetRequests) {
        int rootOrd = taxoReader.getOrdinal(fr.categoryPath);
        if (rootOrd == TaxonomyReader.INVALID_ORDINAL) { // category does not exist
          continue;
        }
        OrdinalPolicy ordinalPolicy = clp.getOrdinalPolicy(fr.categoryPath.components[0]);
        if (ordinalPolicy == OrdinalPolicy.NO_PARENTS) {
          // need to count parents
          counts[rootOrd] += rollupCounts(children[rootOrd], children, siblings);
        }
        
        FacetResultNode root = new FacetResultNode();
        root.ordinal = rootOrd;
        root.label = fr.categoryPath;
        root.value = counts[rootOrd];
        if (fr.getNumResults() > taxoReader.getSize()) {
          // specialize this case, user is interested in all available results
          ArrayList<FacetResultNode> nodes = new ArrayList<FacetResultNode>();
          int child = children[rootOrd];
          while (child != TaxonomyReader.INVALID_ORDINAL) {
            int count = counts[child];
            if (count > 0) {
              FacetResultNode node = new FacetResultNode();
              node.label = taxoReader.getPath(child);
              node.value = count;
              nodes.add(node);
            }
            child = siblings[child];
          }
          Collections.sort(nodes, new Comparator<FacetResultNode>() {
            @Override
            public int compare(FacetResultNode o1, FacetResultNode o2) {
              int value = (int) (o2.value - o1.value);
              if (value == 0) {
                value = o2.ordinal - o1.ordinal;
              }
              return value;
            }
          });
          
          root.subResults = nodes;
          res.add(new FacetResult(fr, root, nodes.size()));
          continue;
        }
        
        // since we use sentinel objects, we cannot reuse PQ. but that's ok because it's not big
        FacetResultNodeQueue pq = new FacetResultNodeQueue(fr.getNumResults(), true);
        FacetResultNode top = pq.top();
        int child = children[rootOrd];
        int numResults = 0; // count the number of results
        while (child != TaxonomyReader.INVALID_ORDINAL) {
          int count = counts[child];
          if (count > top.value) {
            top.value = count;
            top.ordinal = child;
            top = pq.updateTop();
            ++numResults;
          }
          child = siblings[child];
        }

        // pop() the least (sentinel) elements
        int pqsize = pq.size();
        int size = numResults < pqsize ? numResults : pqsize;
        for (int i = pqsize - size; i > 0; i--) { pq.pop(); }

        // create the FacetResultNodes.
        FacetResultNode[] subResults = new FacetResultNode[size];
        for (int i = size - 1; i >= 0; i--) {
          FacetResultNode node = pq.pop();
          node.label = taxoReader.getPath(node.ordinal);
          subResults[i] = node;
        }
        root.subResults = Arrays.asList(subResults);
        res.add(new FacetResult(fr, root, size));
      }
      return res;
    } finally {
      facetArrays.free();
    }
  }
  
  @Override
  public boolean acceptsDocsOutOfOrder() {
    // the actual work is done post-collection, so we always support out-of-order.
    return true;
  }
  
  @Override
  public void setScorer(Scorer scorer) throws IOException {
  }
  
  // TODO: review ResultSortUtils queues and check if we can reuse any of them here
  // and then alleviate the SortOrder/SortBy constraint
  private static class FacetResultNodeQueue extends PriorityQueue<FacetResultNode> {
    
    public FacetResultNodeQueue(int maxSize, boolean prepopulate) {
      super(maxSize, prepopulate);
    }
    
    @Override
    protected FacetResultNode getSentinelObject() {
      return new FacetResultNode();
    }
    
    @Override
    protected boolean lessThan(FacetResultNode a, FacetResultNode b) {
      if (a.value < b.value) return true;
      if (a.value > b.value) return false;
      // both have the same value, break tie by ordinal
      return a.ordinal < b.ordinal;
    }
    
  }
  
}
