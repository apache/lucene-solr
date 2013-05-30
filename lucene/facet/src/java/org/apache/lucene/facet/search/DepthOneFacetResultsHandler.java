package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.lucene.facet.search.FacetRequest.SortOrder;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.util.PriorityQueue;

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
 * A {@link FacetResultsHandler} which counts the top-K facets at depth 1 only
 * and always labels all result categories. The results are always sorted by
 * value, in descending order. Sub-classes are responsible to pull the values
 * from the corresponding {@link FacetArrays}.
 * 
 * @lucene.experimental
 */
public abstract class DepthOneFacetResultsHandler extends FacetResultsHandler {
  
  private static class FacetResultNodeQueue extends PriorityQueue<FacetResultNode> {
    
    public FacetResultNodeQueue(int maxSize, boolean prepopulate) {
      super(maxSize, prepopulate);
    }
    
    @Override
    protected FacetResultNode getSentinelObject() {
      return new FacetResultNode(TaxonomyReader.INVALID_ORDINAL, 0);
    }
    
    @Override
    protected boolean lessThan(FacetResultNode a, FacetResultNode b) {
      if (a.value < b.value) return true;
      if (a.value > b.value) return false;
      // both have the same value, break tie by ordinal
      return a.ordinal < b.ordinal;
    }
    
  }

  public DepthOneFacetResultsHandler(TaxonomyReader taxonomyReader, FacetRequest facetRequest, FacetArrays facetArrays) {
    super(taxonomyReader, facetRequest, facetArrays);
    assert facetRequest.getDepth() == 1 : "this handler only computes the top-K facets at depth 1";
    assert facetRequest.numResults == facetRequest.getNumLabel() : "this handler always labels all top-K results";
    assert facetRequest.getSortOrder() == SortOrder.DESCENDING : "this handler always sorts results in descending order";
  }

  /** Returnt the value of the requested ordinal. Called once for the result root. */
  protected abstract double valueOf(int ordinal);
  
  /**
   * Add the siblings of {@code ordinal} to the given list. This is called
   * whenever the number of results is too high (&gt; taxonomy size), instead of
   * adding them to a {@link PriorityQueue}.
   */
  protected abstract void addSiblings(int ordinal, int[] siblings, ArrayList<FacetResultNode> nodes) throws IOException;
  
  /**
   * Add the siblings of {@code ordinal} to the given {@link PriorityQueue}. The
   * given {@link PriorityQueue} is already filled with sentinel objects, so
   * implementations are encouraged to use {@link PriorityQueue#top()} and
   * {@link PriorityQueue#updateTop()} for best performance.  Returns the total
   * number of siblings.
   */
  protected abstract int addSiblings(int ordinal, int[] siblings, PriorityQueue<FacetResultNode> pq);
  
  @Override
  public final FacetResult compute() throws IOException {
    ParallelTaxonomyArrays arrays = taxonomyReader.getParallelTaxonomyArrays();
    final int[] children = arrays.children();
    final int[] siblings = arrays.siblings();
    
    int rootOrd = taxonomyReader.getOrdinal(facetRequest.categoryPath);
        
    FacetResultNode root = new FacetResultNode(rootOrd, valueOf(rootOrd));
    root.label = facetRequest.categoryPath;
    if (facetRequest.numResults > taxonomyReader.getSize()) {
      // specialize this case, user is interested in all available results
      ArrayList<FacetResultNode> nodes = new ArrayList<FacetResultNode>();
      int child = children[rootOrd];
      addSiblings(child, siblings, nodes);
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
      return new FacetResult(facetRequest, root, nodes.size());
    }
    
    // since we use sentinel objects, we cannot reuse PQ. but that's ok because it's not big
    PriorityQueue<FacetResultNode> pq = new FacetResultNodeQueue(facetRequest.numResults, true);
    int numSiblings = addSiblings(children[rootOrd], siblings, pq);

    // pop() the least (sentinel) elements
    int pqsize = pq.size();
    int size = numSiblings < pqsize ? numSiblings : pqsize;
    for (int i = pqsize - size; i > 0; i--) { pq.pop(); }

    // create the FacetResultNodes.
    FacetResultNode[] subResults = new FacetResultNode[size];
    for (int i = size - 1; i >= 0; i--) {
      FacetResultNode node = pq.pop();
      node.label = taxonomyReader.getPath(node.ordinal);
      subResults[i] = node;
    }
    root.subResults = Arrays.asList(subResults);
    return new FacetResult(facetRequest, root, numSiblings);
  }
  
}
