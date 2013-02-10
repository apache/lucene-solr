package org.apache.lucene.facet.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.lucene.util.PriorityQueue;

import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.Heap;
import org.apache.lucene.facet.search.FacetRequest.SortOrder;

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
 * Utilities for generating facet results sorted as required
 * 
 * @lucene.experimental
 */
public class ResultSortUtils {

  /**
   * Create a suitable heap according to facet request being served. 
   * @return heap for maintaining results for specified request.
   * @throws IllegalArgumentException is provided facet request is not supported 
   */
  public static Heap<FacetResultNode> createSuitableHeap(FacetRequest facetRequest) {
    int nresults = facetRequest.numResults;
    boolean accending = (facetRequest.getSortOrder() == SortOrder.ASCENDING);

    if (nresults == Integer.MAX_VALUE) {
      return new AllValueHeap(accending);
    }

    if (accending) {
      return new MaxValueHeap(nresults);
    } else {
      return new MinValueHeap(nresults);
    }
  }

  private static class MinValueHeap extends PriorityQueue<FacetResultNode> implements Heap<FacetResultNode> {
    public MinValueHeap(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(FacetResultNode arg0, FacetResultNode arg1) {
      double value0 = arg0.value;
      double value1 = arg1.value;

      int valueCompare = Double.compare(value0, value1);
      if (valueCompare == 0) { 
        return arg0.ordinal < arg1.ordinal;
      }

      return valueCompare < 0;
    }

  }

  private static class MaxValueHeap extends PriorityQueue<FacetResultNode> implements Heap<FacetResultNode> {
    public MaxValueHeap(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(FacetResultNode arg0, FacetResultNode arg1) {
      double value0 = arg0.value;
      double value1 = arg1.value;

      int valueCompare = Double.compare(value0, value1);
      if (valueCompare == 0) { 
        return arg0.ordinal > arg1.ordinal;
      }

      return valueCompare > 0;
    }
  }

  /**
   * Create a Heap-Look-Alike, which implements {@link Heap}, but uses a
   * regular <code>ArrayList</code> for holding <b>ALL</b> the objects given,
   * only sorting upon the first call to {@link #pop()}.
   */
  private static class AllValueHeap implements Heap<FacetResultNode> {
    private ArrayList<FacetResultNode> resultNodes = new ArrayList<FacetResultNode>();
    final boolean accending;
    private boolean isReady = false;
    public AllValueHeap(boolean accending) {
      this.accending = accending;
    }

    @Override
    public FacetResultNode insertWithOverflow(FacetResultNode node) {
      resultNodes.add(node);
      return null;
    }

    @Override
    public FacetResultNode pop() {
      if (!isReady) {
        Collections.sort(resultNodes, new Comparator<FacetResultNode>() {
          @Override
          public int compare(FacetResultNode o1, FacetResultNode o2) {
            int value = Double.compare(o1.value, o2.value);
            if (value == 0) {
              value = o1.ordinal - o2.ordinal;
            }
            if (accending) {
              value = -value;
            }
            return value;
          }
        });
        isReady = true;
      }

      return resultNodes.remove(0);
    }

    @Override
    public int size() {
      return resultNodes.size();
    }

    @Override
    public FacetResultNode top() {
      if (resultNodes.size() > 0) {
        return resultNodes.get(0);
      }

      return null;
    }

    @Override
    public FacetResultNode add(FacetResultNode frn) {
      resultNodes.add(frn);
      return null;
    }

    @Override
    public void clear() {
      resultNodes.clear();
    }
  }
  
}
