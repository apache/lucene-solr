package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.lucene.facet.search.FacetRequest.SortOrder;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.util.CollectionUtil;
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
 * value, in descending order.
 * 
 * @lucene.experimental
 */
public class DepthOneFacetResultsHandler extends FacetResultsHandler {
  
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
      return a.compareTo(b)  < 0;
    }
    
  }

  public DepthOneFacetResultsHandler(TaxonomyReader taxonomyReader, FacetRequest facetRequest, FacetArrays facetArrays, 
      OrdinalValueResolver resolver) {
    super(taxonomyReader, facetRequest, resolver, facetArrays);
    assert facetRequest.getDepth() == 1 : "this handler only computes the top-K facets at depth 1";
    assert facetRequest.numResults == facetRequest.getNumLabel() : "this handler always labels all top-K results";
    assert facetRequest.getSortOrder() == SortOrder.DESCENDING : "this handler always sorts results in descending order";
  }

  @Override
  public final FacetResult compute() throws IOException {
    ParallelTaxonomyArrays arrays = taxonomyReader.getParallelTaxonomyArrays();
    final int[] children = arrays.children();
    final int[] siblings = arrays.siblings();
    
    int rootOrd = taxonomyReader.getOrdinal(facetRequest.categoryPath);
        
    FacetResultNode root = new FacetResultNode(rootOrd, resolver.valueOf(rootOrd));
    root.label = facetRequest.categoryPath;
    if (facetRequest.numResults > taxonomyReader.getSize()) {
      // specialize this case, user is interested in all available results
      ArrayList<FacetResultNode> nodes = new ArrayList<FacetResultNode>();
      int ordinal = children[rootOrd];
      while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
        double value = resolver.valueOf(ordinal);
        if (value > 0) {
          FacetResultNode node = new FacetResultNode(ordinal, value);
          node.label = taxonomyReader.getPath(ordinal);
          nodes.add(node);
        }
        ordinal = siblings[ordinal];
      }

      CollectionUtil.introSort(nodes, Collections.reverseOrder(new Comparator<FacetResultNode>() {
        @Override
        public int compare(FacetResultNode o1, FacetResultNode o2) {
          return o1.compareTo(o2);
        }
      }));
      
      root.subResults = nodes;
      return new FacetResult(facetRequest, root, nodes.size());
    }
    
    // since we use sentinel objects, we cannot reuse PQ. but that's ok because it's not big
    PriorityQueue<FacetResultNode> pq = new FacetResultNodeQueue(facetRequest.numResults, true);
    int ordinal = children[rootOrd];
    FacetResultNode top = pq.top();
    int numSiblings = 0;
    while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
      double value = resolver.valueOf(ordinal);
      if (value > 0) {
        ++numSiblings;
        if (value > top.value) {
          top.value = value;
          top.ordinal = ordinal;
          top = pq.updateTop();
        }
      }
      ordinal = siblings[ordinal];
    }

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
