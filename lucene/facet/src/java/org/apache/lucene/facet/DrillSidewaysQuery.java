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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.facet.DrillSidewaysScorer.DocsAndCost;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** Only purpose is to punch through and return a
 *  DrillSidewaysScorer*/ 

// TODO change the way DrillSidewaysScorer is used, this query does not work
// with filter caching
class DrillSidewaysQuery extends Query {
  final Query baseQuery;
  final FacetsCollectorManager drillDownCollectorManager;
  final FacetsCollectorManager[] drillSidewaysCollectorManagers;
  final List<FacetsCollector> managedDrillDownCollectors;
  final List<FacetsCollector[]> managedDrillSidewaysCollectors;
  final Query[] drillDownQueries;
  final boolean scoreSubDocsAtOnce;

  /**
   * Construct a new {@code DrillSidewaysQuery} that will create new {@link FacetsCollector}s for
   * each {@link LeafReaderContext} using the provided {@link FacetsCollectorManager}s. The caller
   * can access the created {@link FacetsCollector}s through {@link #managedDrillDownCollectors} and
   * {@link #managedDrillSidewaysCollectors}.
   */
  DrillSidewaysQuery(
      Query baseQuery,
      FacetsCollectorManager drillDownCollectorManager,
      FacetsCollectorManager[] drillSidewaysCollectorManagers,
      Query[] drillDownQueries,
      boolean scoreSubDocsAtOnce) {
    // Note that the "managed" facet collector lists are synchronized here since bulkScorer()
    // can be invoked concurrently and needs to remain thread-safe. We're OK with synchronizing
    // on the whole list as contention is expected to remain very low:
    this(
        baseQuery,
        drillDownCollectorManager,
        drillSidewaysCollectorManagers,
        Collections.synchronizedList(new ArrayList<>()),
        Collections.synchronizedList(new ArrayList<>()),
        drillDownQueries,
        scoreSubDocsAtOnce);
  }

  /**
   * Needed for {@link #rewrite(IndexReader)}. Ensures the same "managed" lists get used since
   * {@link DrillSideways} accesses references to these through the original {@code
   * DrillSidewaysQuery}.
   */
  private DrillSidewaysQuery(
      Query baseQuery,
      FacetsCollectorManager drillDownCollectorManager,
      FacetsCollectorManager[] drillSidewaysCollectorManagers,
      List<FacetsCollector> managedDrillDownCollectors,
      List<FacetsCollector[]> managedDrillSidewaysCollectors,
      Query[] drillDownQueries,
      boolean scoreSubDocsAtOnce) {
    this.baseQuery = Objects.requireNonNull(baseQuery);
    this.drillDownCollectorManager = drillDownCollectorManager;
    this.drillSidewaysCollectorManagers = drillSidewaysCollectorManagers;
    this.managedDrillDownCollectors = managedDrillDownCollectors;
    this.managedDrillSidewaysCollectors = managedDrillSidewaysCollectors;
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
      return new DrillSidewaysQuery(
          newQuery,
          drillDownCollectorManager,
          drillSidewaysCollectorManagers,
          managedDrillDownCollectors,
          managedDrillSidewaysCollectors,
          drillDownQueries,
          scoreSubDocsAtOnce);
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final Weight baseWeight = baseQuery.createWeight(searcher, scoreMode, boost);
    final Weight[] drillDowns = new Weight[drillDownQueries.length];
    for(int dim=0;dim<drillDownQueries.length;dim++) {
      drillDowns[dim] = searcher.createWeight(searcher.rewrite(drillDownQueries[dim]), ScoreMode.COMPLETE_NO_SCORES, 1);
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
        // We can never cache DSQ instances. It's critical that the BulkScorer produced by this
        // Weight runs through the "normal" execution path so that it has access to an
        // "acceptDocs" instance that accurately reflects deleted docs. During caching,
        // "acceptDocs" is null so that caching over-matches (since the final BulkScorer would
        // account for deleted docs). The problem is that this BulkScorer has a side-effect of
        // populating the "sideways" FacetsCollectors, so it will use deleted docs in its
        // sideways counting if caching kicks in. See LUCENE-10060:
        return false;
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        Scorer baseScorer = baseWeight.scorer(context);

        int drillDownCount = drillDowns.length;

        FacetsCollector[] sidewaysCollectors = new FacetsCollector[drillDownCount];
        managedDrillSidewaysCollectors.add(sidewaysCollectors);

        DrillSidewaysScorer.DocsAndCost[] dims =
            new DrillSidewaysScorer.DocsAndCost[drillDownCount];

        int nullCount = 0;
        for(int dim=0;dim<dims.length;dim++) {
          Scorer scorer = drillDowns[dim].scorer(context);
          if (scorer == null) {
            nullCount++;
            scorer = new ConstantScoreScorer(drillDowns[dim], 0f, scoreMode, DocIdSetIterator.empty());
          }

          FacetsCollector sidewaysCollector = drillSidewaysCollectorManagers[dim].newCollector();
          sidewaysCollectors[dim] = sidewaysCollector;

          dims[dim] = new DrillSidewaysScorer.DocsAndCost(scorer, sidewaysCollector);
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

        FacetsCollector drillDownCollector;
        if (drillDownCollectorManager != null) {
          drillDownCollector = drillDownCollectorManager.newCollector();
          managedDrillDownCollectors.add(drillDownCollector);
        } else {
          drillDownCollector = null;
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
    result = prime * result + Objects.hashCode(drillDownCollectorManager);
    result = prime * result + Arrays.hashCode(drillDownQueries);
    result = prime * result + Arrays.hashCode(drillSidewaysCollectorManagers);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(DrillSidewaysQuery other) {
    return Objects.equals(baseQuery, other.baseQuery)
        && Objects.equals(drillDownCollectorManager, other.drillDownCollectorManager)
        && Arrays.equals(drillDownQueries, other.drillDownQueries)
        && Arrays.equals(drillSidewaysCollectorManagers, other.drillSidewaysCollectorManagers);
  }
}
