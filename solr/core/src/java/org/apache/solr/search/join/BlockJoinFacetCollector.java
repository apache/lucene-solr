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
package org.apache.solr.search.join;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.join.ToParentBlockJoinQuery.ChildrenMatchesScorer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.join.BlockJoinFieldFacetAccumulator.AggregatableDocIter;
import org.apache.solr.search.join.BlockJoinFieldFacetAccumulator.SortedIntsAggDocIterator;

/**
 * For each collected parent document creates matched block, which is a docSet with matched children and parent doc
 * itself. Then updates each BlockJoinFieldFacetAccumulator with the created matched block.
 */
class BlockJoinFacetCollector extends DelegatingCollector {
  private BlockJoinFieldFacetAccumulator[] blockJoinFieldFacetAccumulators;
  private boolean firstSegment = true;
  private ChildrenMatchesScorer blockJoinScorer;
  private int[] childDocs = new int[0];
  
  BlockJoinFacetCollector(SolrQueryRequest req) throws IOException {
    String[] facetFieldNames = BlockJoinFacetComponent.getChildFacetFields(req);
    assert facetFieldNames != null;
    blockJoinFieldFacetAccumulators = new BlockJoinFieldFacetAccumulator[facetFieldNames.length];
    for (int i = 0; i < facetFieldNames.length; i++) {
      blockJoinFieldFacetAccumulators[i] = new BlockJoinFieldFacetAccumulator(facetFieldNames[i], req.getSearcher());
    }
  }
  
  @Override
  public void setScorer(Scorer scorer) throws IOException {
    super.setScorer(scorer);
    blockJoinScorer = getToParentScorer(scorer, new LinkedList<Scorer>());
    if (blockJoinScorer != null) {
      // instruct scorer to keep track of the child docIds for retrieval purposes.
      blockJoinScorer.trackPendingChildHits();
    }
  }
  
  private ChildrenMatchesScorer getToParentScorer(Scorer scorer, Queue<Scorer> queue) {
    if (scorer == null || scorer instanceof ChildrenMatchesScorer) {
      return (ChildrenMatchesScorer) scorer;
    } else {
      for (Scorer.ChildScorer child : scorer.getChildren()) {
        queue.add(child.child);
      }
      return getToParentScorer(queue.poll(), queue);
    }
  }
  
  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    for (BlockJoinFieldFacetAccumulator blockJoinFieldFacetAccumulator : blockJoinFieldFacetAccumulators) {
      if(!firstSegment){
        blockJoinFieldFacetAccumulator.migrateGlobal();
      }
      blockJoinFieldFacetAccumulator.setNextReader(context);
    }
    firstSegment = false;
    super.doSetNextReader(context);
  }
  
  @Override
  public void collect(int doc) throws IOException {
    incrementFacets(doc);
    super.collect(doc);
  }
  
  @Override
  public void finish() throws IOException {
    for (BlockJoinFieldFacetAccumulator blockJoinFieldFacetAccumulator : blockJoinFieldFacetAccumulators) {
        blockJoinFieldFacetAccumulator.migrateGlobal();
    }
    super.finish();
  }

  protected void incrementFacets(int parent) throws IOException {
    final int[] docNums = blockJoinScorer.swapChildDocs(childDocs);
    // now we don't
    //includeParentDoc(parent);
    //final int childCountPlusParent = childTracking.getChildCount()+1;
    final int childCountNoParent = blockJoinScorer.getChildCount();
    final SortedIntsAggDocIterator iter = new SortedIntsAggDocIterator(docNums, childCountNoParent, parent);
    countFacets(iter);
  }

  /** is not used 
  protected int[] includeParentDoc(int parent) {
    final int[] docNums = ArrayUtil.grow(childTracking.getChildDocs(), childTracking.getChildCount()+1);
    childTracking.setChildDocs(docNums); // we include parent into block, I'm not sure whether it makes sense
    docNums[childTracking.getChildCount()]=parent;
    return docNums;
  }*/

  protected void countFacets(final AggregatableDocIter iter) throws IOException {
    for (BlockJoinFieldFacetAccumulator blockJoinFieldFacetAccumulator : blockJoinFieldFacetAccumulators) {
      blockJoinFieldFacetAccumulator.updateCountsWithMatchedBlock( iter);
    }
  }
  
  NamedList getFacets() {
    NamedList<NamedList<Integer>> facets = new NamedList<>(blockJoinFieldFacetAccumulators.length);
    for (BlockJoinFieldFacetAccumulator state : blockJoinFieldFacetAccumulators) {
      facets.add(state.getFieldName(), state.getFacetValue());
    }
    return facets;
  }
  
  
}
