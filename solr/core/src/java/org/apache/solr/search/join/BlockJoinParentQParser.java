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
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.BitsFilteredDocIdSet;
import org.apache.solr.search.Filter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SyntaxError;

public class BlockJoinParentQParser extends QParser {
  /** implementation detail subject to change */
  public static final String CACHE_NAME="perSegFilter";

  protected String getParentFilterLocalParamName() {
    return "which";
  }

  BlockJoinParentQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  
  @Override
  public Query parse() throws SyntaxError {
    String filter = localParams.get(getParentFilterLocalParamName());
    String scoreMode = localParams.get("score", ScoreMode.None.name());
    QParser parentParser = subQuery(filter, null);
    Query parentQ = parentParser.getQuery();

    String queryText = localParams.get(QueryParsing.V);
    // there is no child query, return parent filter from cache
    if (queryText == null || queryText.length()==0) {
                  SolrConstantScoreQuery wrapped = new SolrConstantScoreQuery(getFilter(parentQ));
                  wrapped.setCache(false);
                  return wrapped;
    }
    QParser childrenParser = subQuery(queryText, null);
    Query childrenQuery = childrenParser.getQuery();
    return createQuery(parentQ, childrenQuery, scoreMode);
  }

  protected Query createQuery(final Query parentList, Query query, String scoreMode) throws SyntaxError {
    return new AllParentsAware(query, getFilter(parentList).filter, ScoreModeParser.parse(scoreMode), parentList);
  }

  BitDocIdSetFilterWrapper getFilter(Query parentList) {
    return getCachedFilter(req, parentList);
  }

  static BitDocIdSetFilterWrapper getCachedFilter(final SolrQueryRequest request, Query parentList) {
    SolrCache parentCache = request.getSearcher().getCache(CACHE_NAME);
    // lazily retrieve from solr cache
    Filter filter = null;
    if (parentCache != null) {
      filter = (Filter) parentCache.get(parentList);
    }
    BitDocIdSetFilterWrapper result;
    if (filter instanceof BitDocIdSetFilterWrapper) {
      result = (BitDocIdSetFilterWrapper) filter;
    } else {
      result = new BitDocIdSetFilterWrapper(createParentFilter(parentList));
      if (parentCache != null) {
        parentCache.put(parentList, result);
      }
    }
    return result;
  }

  private static BitSetProducer createParentFilter(Query parentQ) {
    return new QueryBitSetProducer(parentQ);
  }

  static final class AllParentsAware extends ToParentBlockJoinQuery {
    private final Query parentQuery;
    
    private AllParentsAware(Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode,
        Query parentList) {
      super(childQuery, parentsFilter, scoreMode);
      parentQuery = parentList;
    }
    
    public Query getParentQuery(){
      return parentQuery;
    }
  }

  // We need this wrapper since BitDocIdSetFilter does not extend Filter
  static class BitDocIdSetFilterWrapper extends Filter {

    final BitSetProducer filter;

    BitDocIdSetFilterWrapper(BitSetProducer filter) {
      this.filter = filter;
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      BitSet set = filter.getBitSet(context);
      if (set == null) {
        return null;
      }
      return BitsFilteredDocIdSet.wrap(new BitDocIdSet(set), acceptDocs);
    }

    @Override
    public String toString(String field) {
      return getClass().getSimpleName() + "(" + filter + ")";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             Objects.equals(filter, getClass().cast(other).filter);
    }

    @Override
    public int hashCode() {
      return classHash() + filter.hashCode();
    }

  }

}






