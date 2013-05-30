package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.facet.partitions.IntermediateFacetResult;
import org.apache.lucene.facet.partitions.PartitionsFacetResultsHandler;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.util.ResultSortUtils;

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
 * Generate Top-K results for a particular {@link FacetRequest}. K is global
 * (among all results) and is defined by {@link FacetRequest#numResults}.
 * 
 * @lucene.experimental
 */
public class TopKFacetResultsHandler extends PartitionsFacetResultsHandler {
  
  /**
   * Construct top-K results handler.
   * 
   * @param taxonomyReader
   *          taxonomy reader
   * @param facetRequest
   *          facet request being served
   */
  public TopKFacetResultsHandler(TaxonomyReader taxonomyReader, FacetRequest facetRequest, FacetArrays facetArrays) {
    super(taxonomyReader, facetRequest, facetArrays);
  }
  
  // fetch top K for specific partition. 
  @Override
  public IntermediateFacetResult fetchPartitionResult(int offset)
  throws IOException {
    TopKFacetResult res = null;
    int ordinal = taxonomyReader.getOrdinal(facetRequest.categoryPath);
    if (ordinal != TaxonomyReader.INVALID_ORDINAL) {
      double value = 0;  
      if (isSelfPartition(ordinal, facetArrays, offset)) {
        int partitionSize = facetArrays.arrayLength;
        value = facetRequest.getValueOf(facetArrays, ordinal % partitionSize);
      }
      
      FacetResultNode parentResultNode = new FacetResultNode(ordinal, value);
      
      Heap<FacetResultNode> heap = ResultSortUtils.createSuitableHeap(facetRequest);
      int totalFacets = heapDescendants(ordinal, heap, parentResultNode, offset);
      res = new TopKFacetResult(facetRequest, parentResultNode, totalFacets);
      res.setHeap(heap);
    }
    return res;
  }
  
  // merge given top K results into current 
  @Override
  public IntermediateFacetResult mergeResults(IntermediateFacetResult... tmpResults) throws IOException {
    
    int ordinal = taxonomyReader.getOrdinal(facetRequest.categoryPath);
    FacetResultNode resNode = new FacetResultNode(ordinal, 0);
    
    int totalFacets = 0;
    Heap<FacetResultNode> heap = null;
    
    // merge other results in queue
    for (IntermediateFacetResult tmpFres : tmpResults) {
      // cast should succeed
      TopKFacetResult fres = (TopKFacetResult) tmpFres;
      totalFacets += fres.getNumValidDescendants();
      // set the value for the result node representing the facet request
      resNode.value += fres.getFacetResultNode().value;
      Heap<FacetResultNode> tmpHeap = fres.getHeap();
      if (heap == null) {
        heap = tmpHeap;
        continue;
      }
      // bring sub results from heap of tmp res into result heap
      for (int i = tmpHeap.size(); i > 0; i--) {
        heap.insertWithOverflow(tmpHeap.pop());
      }
    }
    
    TopKFacetResult res = new TopKFacetResult(facetRequest, resNode, totalFacets);
    res.setHeap(heap);
    return res;
  }
  
  /**
   * Finds the top K descendants of ordinal, which are at most facetRequest.getDepth()
   * deeper than facetRequest.getCategoryPath (whose ordinal is input parameter ordinal). 
   * Candidates are restricted to current "counting list" and current "partition",
   * they join the overall priority queue pq of size K.  
   * @return total number of descendants considered here by pq, excluding ordinal itself.
   */
  private int heapDescendants(int ordinal, Heap<FacetResultNode> pq, FacetResultNode parentResultNode, 
      int offset) throws IOException {
    int partitionSize = facetArrays.arrayLength;
    int endOffset = offset + partitionSize;
    ParallelTaxonomyArrays childrenArray = taxonomyReader.getParallelTaxonomyArrays();
    int[] children = childrenArray.children();
    int[] siblings = childrenArray.siblings();
    FacetResultNode reusable = null;
    int localDepth = 0;
    int depth = facetRequest.getDepth();
    int[] ordinalStack = new int[2+Math.min(Short.MAX_VALUE, depth)];
    int childrenCounter = 0;
    
    int tosOrdinal; // top of stack element
    
    int yc = children[ordinal];
    while (yc >= endOffset) {
      yc = siblings[yc];
    }
    // make use of the fact that TaxonomyReader.INVALID_ORDINAL == -1, < endOffset
    // and it, too, can stop the loop.
    ordinalStack[++localDepth] = yc;
    
    /*
     * stack holds input parameter ordinal in position 0.
     * Other elements are < endoffset.
     * Only top of stack can be TaxonomyReader.INVALID_ORDINAL, and this if and only if
     * the element below it exhausted all its children: has them all processed.
     * 
     * stack elements are processed (counted and accumulated) only if they 
     * belong to current partition (between offset and endoffset) and first time
     * they are on top of stack 
     * 
     * loop as long as stack is not empty of elements other than input ordinal, or for a little while -- it sibling
     */
    while (localDepth > 0) {
      tosOrdinal = ordinalStack[localDepth];
      if (tosOrdinal == TaxonomyReader.INVALID_ORDINAL) {
        // element below tos has all its children, and itself, all processed
        // need to proceed to its sibling
        localDepth--;
        // change element now on top of stack to its sibling.
        ordinalStack[localDepth] = siblings[ordinalStack[localDepth]];
        continue;
      }
      // top of stack is not invalid, this is the first time we see it on top of stack.
      // collect it, if belongs to current partition, and then push its kids on itself, if applicable
      if (tosOrdinal >= offset) { // tosOrdinal resides in current partition
        int relativeOrdinal = tosOrdinal % partitionSize;
        double value = facetRequest.getValueOf(facetArrays, relativeOrdinal);
        if (value != 0 && !Double.isNaN(value)) {
          // Count current ordinal -- the TOS
          if (reusable == null) {
            reusable = new FacetResultNode(tosOrdinal, value);
          } else {
            // it is safe to cast since reusable was created here.
            reusable.ordinal = tosOrdinal;
            reusable.value = value;
            reusable.subResults.clear();
            reusable.label = null;
          }
          ++childrenCounter;
          reusable = pq.insertWithOverflow(reusable);
        }
      }
      if (localDepth < depth) {
        // push kid of current tos
        yc = children[tosOrdinal];
        while (yc >= endOffset) {
          yc = siblings[yc];
        }
        ordinalStack[++localDepth] = yc;
      } else { // localDepth == depth; current tos exhausted its possible children, mark this by pushing INVALID_ORDINAL
        ordinalStack[++localDepth] = TaxonomyReader.INVALID_ORDINAL;
      }
    } // endof while stack is not empty
    
    return childrenCounter; // we're done
  }
  
  @Override
  public FacetResult renderFacetResult(IntermediateFacetResult tmpResult) {
    TopKFacetResult res = (TopKFacetResult) tmpResult; // cast is safe by contract of this class
    if (res != null) {
      Heap<FacetResultNode> heap = res.getHeap();
      FacetResultNode resNode = res.getFacetResultNode();
      if (resNode.subResults == FacetResultNode.EMPTY_SUB_RESULTS) {
        resNode.subResults = new ArrayList<FacetResultNode>();
      }
      for (int i = heap.size(); i > 0; i--) {
        resNode.subResults.add(0, heap.pop());
      }
    }
    return res;
  }
  
  @Override
  public FacetResult rearrangeFacetResult(FacetResult facetResult) {
    TopKFacetResult res = (TopKFacetResult) facetResult; // cast is safe by contract of this class
    Heap<FacetResultNode> heap = res.getHeap();
    heap.clear(); // just to be safe
    FacetResultNode topFrn = res.getFacetResultNode();
    for (FacetResultNode frn : topFrn.subResults) {
      heap.add(frn);
    }
    int size = heap.size();
    ArrayList<FacetResultNode> subResults = new ArrayList<FacetResultNode>(size);
    for (int i = heap.size(); i > 0; i--) {
      subResults.add(0,heap.pop());
    }
    topFrn.subResults = subResults;
    return res;
  }
  
  @Override
  public void labelResult(FacetResult facetResult) throws IOException {
    if (facetResult != null) { // any result to label?
      FacetResultNode facetResultNode = facetResult.getFacetResultNode();
      if (facetResultNode != null) { // any result to label?
        facetResultNode.label = taxonomyReader.getPath(facetResultNode.ordinal);
        int num2label = facetRequest.getNumLabel();
        for (FacetResultNode frn : facetResultNode.subResults) {
          if (--num2label < 0) {
            break;
          }
          frn.label = taxonomyReader.getPath(frn.ordinal);
        }
      }
    }
  }
  
  ////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////
  
  /**
   * Private Mutable implementation of result of faceted search.
   */
  private static class TopKFacetResult extends FacetResult implements IntermediateFacetResult {
    
    // TODO (Facet): is it worth to override PriorityQueue.getSentinelObject()
    // for any of our PQs?
    private Heap<FacetResultNode> heap; 
    
    /**
     * Create a Facet Result.
     * @param facetRequest Request for which this result was obtained.
     * @param facetResultNode top result node for this facet result.
     */
    TopKFacetResult(FacetRequest facetRequest, FacetResultNode facetResultNode, int totalFacets) {
      super(facetRequest, facetResultNode, totalFacets);
    }
    
    /**
     * @return the heap
     */
    public Heap<FacetResultNode> getHeap() {
      return heap;
    }
    
    /**
     * Set the heap for this result.
     * @param heap heap top be set.
     */
    public void setHeap(Heap<FacetResultNode> heap) {
      this.heap = heap;
    }
    
  }
  
  //////////////////////////////////////////////////////
}
