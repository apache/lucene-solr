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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.facet.DrillSidewaysScorer.DocsAndCost;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** Only purpose is to punch through and return a
 *  DrillSidewaysScorer*/ 

// TODO change the way DrillSidewaysScorer is used, this query does not work
// with filter caching
class DrillSidewaysQuery extends Query {
  final Query baseQuery;
  final Collector drillDownCollector;
  final Collector[] drillSidewaysCollectors;
  final Query[] drillDownQueries;
  final boolean scoreSubDocsAtOnce;

  DrillSidewaysQuery(Query baseQuery, Collector drillDownCollector, Collector[] drillSidewaysCollectors, Query[] drillDownQueries, boolean scoreSubDocsAtOnce) {
    this.baseQuery = Objects.requireNonNull(baseQuery);
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
      return super.rewrite(reader);
    } else {
      return new DrillSidewaysQuery(newQuery, drillDownCollector, drillSidewaysCollectors, drillDownQueries, scoreSubDocsAtOnce);
    }
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    final Weight baseWeight = baseQuery.createWeight(searcher, needsScores, boost);
    final Weight[] drillDowns = new Weight[drillDownQueries.length];
    for(int dim=0;dim<drillDownQueries.length;dim++) {
      drillDowns[dim] = searcher.createWeight(searcher.rewrite(drillDownQueries[dim]), false, 1);
    }

    return new Weight(DrillSidewaysQuery.this) {
      @Override
      public void extractTerms(Set<Term> terms) {}

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return baseWeight.explain(context, doc);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        // We can only run as a top scorer:
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        if (baseWeight.isCacheable(ctx) == false)
          return false;
        for (Weight w : drillDowns) {
          if (w.isCacheable(ctx) == false)
            return false;
        }
        return true;
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        Scorer baseScorer = baseWeight.scorer(context);

        DrillSidewaysScorer.DocsAndCost[] dims = new DrillSidewaysScorer.DocsAndCost[drillDowns.length];
        int nullCount = 0;
        for(int dim=0;dim<dims.length;dim++) {
          Scorer scorer = drillDowns[dim].scorer(context);
          if (scorer == null) {
            nullCount++;
            scorer = new ConstantScoreScorer(drillDowns[dim], 0f, DocIdSetIterator.empty());
          }

          dims[dim] = new DrillSidewaysScorer.DocsAndCost(scorer, drillSidewaysCollectors[dim]);
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
        Arrays.sort(dims, new Comparator<DrillSidewaysScorer.DocsAndCost>() {
          @Override
          public int compare(DocsAndCost o1, DocsAndCost o2) {
            return Long.compare(o1.approximation.cost(), o2.approximation.cost());
          }
        });

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
    int result = classHash();
    result = prime * result + Objects.hashCode(baseQuery);
    result = prime * result + Objects.hashCode(drillDownCollector);
    result = prime * result + Arrays.hashCode(drillDownQueries);
    result = prime * result + Arrays.hashCode(drillSidewaysCollectors);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(DrillSidewaysQuery other) {
    return Objects.equals(baseQuery, other.baseQuery) &&
           Objects.equals(drillDownCollector, other.drillDownCollector) &&
           Arrays.equals(drillDownQueries, other.drillDownQueries) &&
           Arrays.equals(drillSidewaysCollectors, other.drillSidewaysCollectors);
  }
}
