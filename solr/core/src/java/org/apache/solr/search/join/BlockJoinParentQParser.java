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
import java.io.UncheckedIOException;
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
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.apache.solr.search.SyntaxError;

public class BlockJoinParentQParser extends FiltersQParser {
  /** implementation detail subject to change */
  public static final String CACHE_NAME="perSegFilter";

  protected String getParentFilterLocalParamName() {
    return "which";
  }

  @Override
  protected String getFiltersParamName() {
    return "filters";
  }

  BlockJoinParentQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  protected Query parseParentFilter() throws SyntaxError {
    String filter = localParams.get(getParentFilterLocalParamName());
    QParser parentParser = subQuery(filter, null);
    Query parentQ = parentParser.getQuery();
    return parentQ;
  }

  @Override
  protected Query wrapSubordinateClause(Query subordinate) throws SyntaxError {
    String scoreMode = localParams.get("score", ScoreMode.None.name());
    Query parentQ = parseParentFilter();
    return createQuery(parentQ, subordinate, scoreMode);
  }

  @Override
  protected Query noClausesQuery() throws SyntaxError {
    SolrConstantScoreQuery wrapped = new SolrConstantScoreQuery(getFilter(parseParentFilter()));
    wrapped.setCache(false);
    return wrapped;
  }

  protected Query createQuery(final Query parentList, Query query, String scoreMode) throws SyntaxError {
    return new AllParentsAware(query, getFilter(parentList).filter, ScoreModeParser.parse(scoreMode), parentList);
  }

  BitDocIdSetFilterWrapper getFilter(Query parentList) {
    return getCachedFilter(req, parentList);
  }

  public static BitDocIdSetFilterWrapper getCachedFilter(final SolrQueryRequest request, Query parentList) {
    SolrCache<Query, Filter> parentCache = request.getSearcher().getCache(CACHE_NAME);
    // lazily retrieve from solr cache
    BitDocIdSetFilterWrapper result;
    if (parentCache != null) {
      try {
        Filter filter = parentCache.computeIfAbsent(parentList,
            query -> new BitDocIdSetFilterWrapper(createParentFilter(query)));
        if (filter instanceof BitDocIdSetFilterWrapper) {
          result = (BitDocIdSetFilterWrapper) filter;
        } else {
          result = new BitDocIdSetFilterWrapper(createParentFilter(parentList));
          // non-atomic update of existing entry to ensure strong-typing
          parentCache.put(parentList, result);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e); // Shouldn't happen because nothing in here throws
      }
    } else {
      result = new BitDocIdSetFilterWrapper(createParentFilter(parentList));
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
  public static class BitDocIdSetFilterWrapper extends Filter {

    private final BitSetProducer filter;

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

    public BitSetProducer getFilter() {
      return filter;
    }

    @Override
    public String toString(String field) {
      return getClass().getSimpleName() + "(" + filter + ")";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             Objects.equals(filter, getClass().cast(other).getFilter());
    }

    @Override
    public int hashCode() {
      return classHash() + filter.hashCode();
    }
  }

}






