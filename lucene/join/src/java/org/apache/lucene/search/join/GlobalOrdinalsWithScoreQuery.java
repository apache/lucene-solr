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
package org.apache.lucene.search.join;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;

final class GlobalOrdinalsWithScoreQuery extends Query implements Accountable {
  private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(GlobalOrdinalsWithScoreQuery.class);

  private final GlobalOrdinalsWithScoreCollector collector;
  private final String joinField;
  private final OrdinalMap globalOrds;
  // Is also an approximation of the docs that will match. Can be all docs that have toField or something more specific.
  private final Query toQuery;

  // just for hashcode and equals:
  private final ScoreMode scoreMode;
  private final Query fromQuery;
  private final int min;
  private final int max;
  // id of the context rather than the context itself in order not to hold references to index readers
  private final Object indexReaderContextId;

  private final long ramBytesUsed; // cache

  GlobalOrdinalsWithScoreQuery(GlobalOrdinalsWithScoreCollector collector, ScoreMode scoreMode, String joinField,
                               OrdinalMap globalOrds, Query toQuery, Query fromQuery, int min, int max,
                               Object indexReaderContextId) {
    this.collector = collector;
    this.joinField = joinField;
    this.globalOrds = globalOrds;
    this.toQuery = toQuery;
    this.scoreMode = scoreMode;
    this.fromQuery = fromQuery;
    this.min = min;
    this.max = max;
    this.indexReaderContextId = indexReaderContextId;

    this.ramBytesUsed = BASE_RAM_BYTES +
        RamUsageEstimator.sizeOfObject(this.fromQuery, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED) +
        RamUsageEstimator.sizeOfObject(this.globalOrds) +
        RamUsageEstimator.sizeOfObject(this.joinField) +
        RamUsageEstimator.sizeOfObject(this.toQuery, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost) throws IOException {
    if (searcher.getTopReaderContext().id() != indexReaderContextId) {
      throw new IllegalStateException("Creating the weight against a different index reader than this query has been built for.");
    }
    boolean doNoMinMax = min <= 0 && max == Integer.MAX_VALUE;
    if (scoreMode.needsScores() == false && doNoMinMax) {
      // We don't need scores then quickly change the query to not uses the scores:
      GlobalOrdinalsQuery globalOrdinalsQuery = new GlobalOrdinalsQuery(collector.collectedOrds, joinField, globalOrds,
          toQuery, fromQuery, indexReaderContextId);
      return globalOrdinalsQuery.createWeight(searcher, org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, boost);
    }
    return new W(this, toQuery.createWeight(searcher, org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1f));
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(GlobalOrdinalsWithScoreQuery other) {
    return min == other.min &&
           max == other.max &&
           scoreMode.equals(other.scoreMode) &&
           joinField.equals(other.joinField) &&
           fromQuery.equals(other.fromQuery) &&
           toQuery.equals(other.toQuery) &&
           indexReaderContextId.equals(other.indexReaderContextId);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + scoreMode.hashCode();
    result = 31 * result + joinField.hashCode();
    result = 31 * result + toQuery.hashCode();
    result = 31 * result + fromQuery.hashCode();
    result = 31 * result + min;
    result = 31 * result + max;
    result = 31 * result + indexReaderContextId.hashCode();
    return result;
  }

  @Override
  public String toString(String field) {
    return "GlobalOrdinalsQuery{" +
          "joinField=" + joinField +
          "min=" + min +
          "max=" + max +
          "fromQuery=" + fromQuery +
        '}';
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  final class W extends FilterWeight {

    W(Query query, Weight approximationWeight) {
      super(query, approximationWeight);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      SortedDocValues values = DocValues.getSorted(context.reader(), joinField);
      if (values == null) {
        return Explanation.noMatch("Not a match");
      }
      if (values.advance(doc) != doc) {
        return Explanation.noMatch("Not a match");
      }

      int segmentOrd = values.ordValue();
      BytesRef joinValue = values.lookupOrd(segmentOrd);

      int ord;
      if (globalOrds != null) {
        ord = (int) globalOrds.getGlobalOrds(context.ord).get(segmentOrd);
      } else {
        ord = segmentOrd;
      }
      if (collector.match(ord) == false) {
        return Explanation.noMatch("Not a match, join value " + Term.toString(joinValue));
      }

      float score = collector.score(ord);
      return Explanation.match(score, "A match, join value " + Term.toString(joinValue));
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      SortedDocValues values = DocValues.getSorted(context.reader(), joinField);
      if (values == null) {
        return null;
      }

      Scorer approximationScorer = in.scorer(context);
      if (approximationScorer == null) {
        return null;
      } else if (globalOrds != null) {
        return new OrdinalMapScorer(this, collector, values, approximationScorer.iterator(), globalOrds.getGlobalOrds(context.ord));
      } else {
        return new SegmentOrdinalScorer(this, collector, values, approximationScorer.iterator());
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      // disable caching because this query relies on a top reader context
      // and holds a bitset of matching ordinals that cannot be accounted in
      // the memory used by the cache
      return false;
    }
  }

  final static class OrdinalMapScorer extends BaseGlobalOrdinalScorer {

    final LongValues segmentOrdToGlobalOrdLookup;
    final GlobalOrdinalsWithScoreCollector collector;

    public OrdinalMapScorer(Weight weight, GlobalOrdinalsWithScoreCollector collector, SortedDocValues values, DocIdSetIterator approximation, LongValues segmentOrdToGlobalOrdLookup) {
      super(weight, values, approximation);
      this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
      this.collector = collector;
    }

    @Override
    protected TwoPhaseIterator createTwoPhaseIterator(DocIdSetIterator approximation) {
      return new TwoPhaseIterator(approximation) {

        @Override
        public boolean matches() throws IOException {
          if (values.advanceExact(approximation.docID())) {
            final long segmentOrd = values.ordValue();
            final int globalOrd = (int) segmentOrdToGlobalOrdLookup.get(segmentOrd);
            if (collector.match(globalOrd)) {
              score = collector.score(globalOrd);
              return true;
            }
          }
          return false;
        }

        @Override
        public float matchCost() {
          return 100; // TODO: use cost of values.getOrd() and collector.score()
        }
      };
    }
  }

  final static class SegmentOrdinalScorer extends BaseGlobalOrdinalScorer {

    final GlobalOrdinalsWithScoreCollector collector;

    public SegmentOrdinalScorer(Weight weight, GlobalOrdinalsWithScoreCollector collector, SortedDocValues values, DocIdSetIterator approximation) {
      super(weight, values, approximation);
      this.collector = collector;
    }

    @Override
    protected TwoPhaseIterator createTwoPhaseIterator(DocIdSetIterator approximation) {
      return new TwoPhaseIterator(approximation) {

        @Override
        public boolean matches() throws IOException {
          if (values.advanceExact(approximation.docID())) {
            final int segmentOrd = values.ordValue();
            if (collector.match(segmentOrd)) {
              score = collector.score(segmentOrd);
              return true;
            }
          }
          return false;
        }

        @Override
        public float matchCost() {
          return 100; // TODO: use cost.getOrd() of values and collector.score()
        }
      };
    }
  }
}
