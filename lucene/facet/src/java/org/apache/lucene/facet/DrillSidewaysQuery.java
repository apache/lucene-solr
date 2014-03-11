package org.apache.lucene.facet;

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
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

/** Only purpose is to punch through and return a
 *  DrillSidewaysScorer */ 

class DrillSidewaysQuery extends Query {
  final Query baseQuery;
  final Collector drillDownCollector;
  final Collector[] drillSidewaysCollectors;
  final Query[] drillDownQueries;
  final boolean scoreSubDocsAtOnce;

  DrillSidewaysQuery(Query baseQuery, Collector drillDownCollector, Collector[] drillSidewaysCollectors, Query[] drillDownQueries, boolean scoreSubDocsAtOnce) {
    this.baseQuery = baseQuery;
    this.drillDownCollector = drillDownCollector;
    this.drillSidewaysCollectors = drillSidewaysCollectors;
    this.drillDownQueries = drillDownQueries;
    this.scoreSubDocsAtOnce = scoreSubDocsAtOnce;
  }

  @Override
  public String toString(String field) {
    return "DrillSidewaysQuery";
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query newQuery = baseQuery;
    while(true) {
      Query rewrittenQuery = newQuery.rewrite(reader);
      if (rewrittenQuery == newQuery) {
        break;
      }
      newQuery = rewrittenQuery;
    }
    if (newQuery == baseQuery) {
      return this;
    } else {
      return new DrillSidewaysQuery(newQuery, drillDownCollector, drillSidewaysCollectors, drillDownQueries, scoreSubDocsAtOnce);
    }
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    final Weight baseWeight = baseQuery.createWeight(searcher);
    final Object[] drillDowns = new Object[drillDownQueries.length];
    for(int dim=0;dim<drillDownQueries.length;dim++) {
      Query query = drillDownQueries[dim];
      Filter filter = DrillDownQuery.getFilter(query);
      if (filter != null) {
        drillDowns[dim] = filter;
      } else {
        // TODO: would be nice if we could say "we will do no
        // scoring" here....
        drillDowns[dim] = searcher.rewrite(query).createWeight(searcher);
      }
    }

    return new Weight() {
      @Override
      public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
        return baseWeight.explain(context, doc);
      }

      @Override
      public Query getQuery() {
        return baseQuery;
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return baseWeight.getValueForNormalization();
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {
        baseWeight.normalize(norm, topLevelBoost);
      }

      @Override
      public boolean scoresDocsOutOfOrder() {
        // TODO: would be nice if AssertingIndexSearcher
        // confirmed this for us
        return false;
      }

      @Override
      public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        // We can only run as a top scorer:
        throw new UnsupportedOperationException();
      }

      @Override
      public BulkScorer bulkScorer(AtomicReaderContext context, boolean scoreDocsInOrder, Bits acceptDocs) throws IOException {

        // TODO: it could be better if we take acceptDocs
        // into account instead of baseScorer?
        Scorer baseScorer = baseWeight.scorer(context, acceptDocs);

        DrillSidewaysScorer.DocsAndCost[] dims = new DrillSidewaysScorer.DocsAndCost[drillDowns.length];
        int nullCount = 0;
        for(int dim=0;dim<dims.length;dim++) {
          dims[dim] = new DrillSidewaysScorer.DocsAndCost();
          dims[dim].sidewaysCollector = drillSidewaysCollectors[dim];
          if (drillDowns[dim] instanceof Filter) {
            // Pass null for acceptDocs because we already
            // passed it to baseScorer and baseScorer is
            // MUST'd here
            DocIdSet dis = ((Filter) drillDowns[dim]).getDocIdSet(context, null);

            if (dis == null) {
              continue;
            }

            Bits bits = dis.bits();

            if (bits != null) {
              // TODO: this logic is too naive: the
              // existence of bits() in DIS today means
              // either "I'm a cheap FixedBitSet so apply me down
              // low as you decode the postings" or "I'm so
              // horribly expensive so apply me after all
              // other Query/Filter clauses pass"

              // Filter supports random access; use that to
              // prevent .advance() on costly filters:
              dims[dim].bits = bits;

              // TODO: Filter needs to express its expected
              // cost somehow, before pulling the iterator;
              // we should use that here to set the order to
              // check the filters:

            } else {
              DocIdSetIterator disi = dis.iterator();
              if (disi == null) {
                nullCount++;
                continue;
              }
              dims[dim].disi = disi;
            }
          } else {
            DocIdSetIterator disi = ((Weight) drillDowns[dim]).scorer(context, null);
            if (disi == null) {
              nullCount++;
              continue;
            }
            dims[dim].disi = disi;
          }
        }

        // If more than one dim has no matches, then there
        // are no hits nor drill-sideways counts.  Or, if we
        // have only one dim and that dim has no matches,
        // same thing.
        //if (nullCount > 1 || (nullCount == 1 && dims.length == 1)) {
        if (nullCount > 1) {
          return null;
        }

        // Sort drill-downs by most restrictive first:
        Arrays.sort(dims);

        if (baseScorer == null) {
          return null;
        }

        return new DrillSidewaysScorer(context,
                                       baseScorer,
                                       drillDownCollector, dims,
                                       scoreSubDocsAtOnce);
      }
    };
  }

  // TODO: these should do "deeper" equals/hash on the 2-D drillDownTerms array

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((baseQuery == null) ? 0 : baseQuery.hashCode());
    result = prime * result
        + ((drillDownCollector == null) ? 0 : drillDownCollector.hashCode());
    result = prime * result + Arrays.hashCode(drillDownQueries);
    result = prime * result + Arrays.hashCode(drillSidewaysCollectors);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    DrillSidewaysQuery other = (DrillSidewaysQuery) obj;
    if (baseQuery == null) {
      if (other.baseQuery != null) return false;
    } else if (!baseQuery.equals(other.baseQuery)) return false;
    if (drillDownCollector == null) {
      if (other.drillDownCollector != null) return false;
    } else if (!drillDownCollector.equals(other.drillDownCollector)) return false;
    if (!Arrays.equals(drillDownQueries, other.drillDownQueries)) return false;
    if (!Arrays.equals(drillSidewaysCollectors, other.drillSidewaysCollectors)) return false;
    return true;
  }
}
