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
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.facet.BlockJoin;
import org.apache.solr.search.join.BlockJoinFieldFacetAccumulator.AggregatableDocIter;

/**
 * Calculates facets on children documents and aggregates hits by parent documents.
 * Enables when child.facet.field parameter specifies a field name for faceting. 
 * So far it supports string fields only. It requires to search by {@link ToParentBlockJoinQuery}.
 * 
 * @deprecated This functionality is considered deprecated and will be removed at 9.0
 * Users are encouraged to use <code>"uniqueBlock(\_root_)"</code> aggregation 
 * under <code>"terms"</code> facet and <code>"domain": { "blockChildren":...}</code>  */
@Deprecated
public class BlockJoinDocSetFacetComponent extends BlockJoinFacetComponentSupport {
  
  private final String bjqKey = this.getClass().getSimpleName()+".bjq";
  
  private static final class SegmentChildren implements AggregatableDocIter {
    
    private final BitDocSet allParentsBitsDocSet;
    private int nextDoc = DocIdSetIterator.NO_MORE_DOCS;
    private DocIdSetIterator disi;
    private int currentParent=-1;
    final LeafReaderContext segment;
    final DocIdSet childrenMatches;
    
    private SegmentChildren(LeafReaderContext subCtx, DocIdSet dis, BitDocSet allParentsBitsDocSet) {
      this.allParentsBitsDocSet = allParentsBitsDocSet;
      this.childrenMatches = dis;
      this.segment = subCtx;
      reset();
    }
    
    @Override
    public Integer next() {
      return nextDoc();
    }
    
    @Override
    public boolean hasNext() {
      return nextDoc != DocIdSetIterator.NO_MORE_DOCS;
    }
    
    @Override
    public float score() {
      return 0;
    }
    
    @Override
    public int nextDoc() {
      int lastDoc = nextDoc;
      assert nextDoc != DocIdSetIterator.NO_MORE_DOCS;
      if (lastDoc>currentParent) { // we passed the previous block, and need to reevaluate a parent
        currentParent = allParentsBitsDocSet.getBits().nextSetBit(lastDoc+segment.docBase)-segment.docBase;
      }
      try {
        nextDoc = disi.nextDoc();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return lastDoc;
    }
    
    @Override
    public void reset() {
      currentParent=-1;
      try {
        disi = childrenMatches.iterator();
        if (disi != null) {
          nextDoc = disi.nextDoc();
        }else{
          nextDoc = DocIdSetIterator.NO_MORE_DOCS;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public int getAggKey() {
      return currentParent;
    }
  }

  public BlockJoinDocSetFacetComponent() {}
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    
    if (getChildFacetFields(rb.req) != null) {
      validateQuery(rb.getQuery());
      rb.setNeedDocSet(true);
      rb.req.getContext().put(bjqKey, extractChildQuery(rb.getQuery()));
    }
  }
  
  private ToParentBlockJoinQuery extractChildQuery(Query query) {
    if (!(query instanceof ToParentBlockJoinQuery)) {
      if (query instanceof BooleanQuery) {
        List<BooleanClause> clauses = ((BooleanQuery) query).clauses();
        ToParentBlockJoinQuery once = null;
        for (BooleanClause clause : clauses) {
          if (clause.getQuery() instanceof ToParentBlockJoinQuery) {
            if (once==null) {
              once = (ToParentBlockJoinQuery) clause.getQuery(); 
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "can't choose between " +
                         once + " and " + clause.getQuery());
            }
          }
        }
        if (once!=null) {
          return once;
        }
      }
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, NO_TO_PARENT_BJQ_MESSAGE);
    }
    else{
      return (ToParentBlockJoinQuery) query;
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    final BlockJoinParentQParser.AllParentsAware bjq = 
        (BlockJoinParentQParser.AllParentsAware) rb.req.getContext().get(bjqKey);
    if(bjq!=null){
      final DocSet parentResult = rb.getResults().docSet;
      final BitDocSet allParentsBitsDocSet = rb.req.getSearcher().getDocSetBits(bjq.getParentQuery());
      final DocSet allChildren = BlockJoin.toChildren(parentResult, 
          allParentsBitsDocSet,
          rb.req.getSearcher().getDocSetBits( new MatchAllDocsQuery() ), 
          QueryContext.newContext(rb.req.getSearcher()));
      
      final DocSet childQueryDocSet = rb.req.getSearcher().getDocSet(bjq.getChildQuery());
      final DocSet selectedChildren = allChildren.intersection(childQueryDocSet);
      
      // don't include parent into facet counts
      //childResult = childResult.union(parentResult);// just to mimic the current logic
      
      final List<LeafReaderContext> leaves = rb.req.getSearcher().getIndexReader().leaves();
      
      Filter filter = selectedChildren.getTopFilter();

      final BlockJoinFacetAccsHolder facetCounter = new BlockJoinFacetAccsHolder(rb.req);
      
      for (int subIdx = 0; subIdx < leaves.size(); subIdx++) {
        LeafReaderContext subCtx = leaves.get(subIdx);
        DocIdSet dis = filter.getDocIdSet(subCtx, null); // solr docsets already exclude any deleted docs
        
        AggregatableDocIter iter = new SegmentChildren(subCtx, dis, allParentsBitsDocSet);
        
        if (iter.hasNext()){
          facetCounter.doSetNextReader(subCtx);
          facetCounter.countFacets(iter);
        }
      }
      facetCounter.finish();
      
      rb.req.getContext().put(COLLECTOR_CONTEXT_PARAM,facetCounter);
      super.process(rb);
    }
    
  }
}
