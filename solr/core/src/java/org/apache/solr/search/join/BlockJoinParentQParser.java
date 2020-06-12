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
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrCache;
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
    return new BitSetProducerQuery(getBitSetProducer(parseParentFilter()));
  }

  protected Query createQuery(final Query parentList, Query query, String scoreMode) throws SyntaxError {
    return new AllParentsAware(query, getBitSetProducer(parentList), ScoreModeParser.parse(scoreMode), parentList);
  }

  BitSetProducer getBitSetProducer(Query query) {
    return getCachedBitSetProducer(req, query);
  }

  public static BitSetProducer getCachedBitSetProducer(final SolrQueryRequest request, Query query) {
    @SuppressWarnings("unchecked")
    SolrCache<Query, BitSetProducer> parentCache = request.getSearcher().getCache(CACHE_NAME);
    // lazily retrieve from solr cache
    if (parentCache != null) {
      return parentCache.computeIfAbsent(query, QueryBitSetProducer::new);
    } else {
      return new QueryBitSetProducer(query);
    }
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

  /** A constant score query based on a {@link BitSetProducer}. */
  static class BitSetProducerQuery extends ExtendedQueryBase {

    final BitSetProducer bitSetProducer;

    public BitSetProducerQuery(BitSetProducer bitSetProducer) {
      this.bitSetProducer = bitSetProducer;
      setCache(false); // because we assume the bitSetProducer is itself cached
    }

    @Override
    public String toString(String field) {
      return getClass().getSimpleName() + "(" + bitSetProducer + ")";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && Objects.equals(bitSetProducer, getClass().cast(other).bitSetProducer);
    }

    @Override
    public int hashCode() {
      return classHash() + bitSetProducer.hashCode();
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost) throws IOException {
      return new ConstantScoreWeight(BitSetProducerQuery.this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          BitSet bitSet = bitSetProducer.getBitSet(context);
          if (bitSet == null) {
            return null;
          }
          DocIdSetIterator disi = new BitSetIterator(bitSet, bitSet.approximateCardinality());
          return new ConstantScoreScorer(this, boost, scoreMode, disi);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return getCache();
        }
      };
    }
  }

}
