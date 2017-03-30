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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

import java.io.IOException;
import java.util.Set;

final class GlobalOrdinalsWithScoreQuery extends Query {

  private final GlobalOrdinalsWithScoreCollector collector;
  private final String joinField;
  private final MultiDocValues.OrdinalMap globalOrds;
  // Is also an approximation of the docs that will match. Can be all docs that have toField or something more specific.
  private final Query toQuery;

  // just for hashcode and equals:
  private final Query fromQuery;
  private final int min;
  private final int max;
  // id of the context rather than the context itself in order not to hold references to index readers
  private final Object indexReaderContextId;

  GlobalOrdinalsWithScoreQuery(GlobalOrdinalsWithScoreCollector collector, String joinField, MultiDocValues.OrdinalMap globalOrds, Query toQuery, Query fromQuery, int min, int max, IndexReaderContext context) {
    this.collector = collector;
    this.joinField = joinField;
    this.globalOrds = globalOrds;
    this.toQuery = toQuery;
    this.fromQuery = fromQuery;
    this.min = min;
    this.max = max;
    this.indexReaderContextId = context.id();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    if (searcher.getTopReaderContext().id() != indexReaderContextId) {
      throw new IllegalStateException("Creating the weight against a different index reader than this query has been built for.");
    }
    return new W(this, toQuery.createWeight(searcher, false));
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(GlobalOrdinalsWithScoreQuery other) {
    return min == other.min &&
           max == other.max &&
           joinField.equals(other.joinField) &&
           fromQuery.equals(other.fromQuery) &&
           toQuery.equals(other.toQuery) &&
           indexReaderContextId.equals(other.indexReaderContextId);
  }

  @Override
  public int hashCode() {
    int result = classHash();
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

  final class W extends FilterWeight {

    W(Query query, Weight approximationWeight) {
      super(query, approximationWeight);
    }

    @Override
    public void extractTerms(Set<Term> terms) {}

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      SortedDocValues values = DocValues.getSorted(context.reader(), joinField);
      if (values == null) {
        return Explanation.noMatch("Not a match");
      }

      int segmentOrd = values.getOrd(doc);
      if (segmentOrd == -1) {
        return Explanation.noMatch("Not a match");
      }
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
    public float getValueForNormalization() throws IOException {
      return 1f;
    }

    @Override
    public void normalize(float norm, float boost) {
      // no normalization, we ignore the normalization process
      // and produce scores based on the join
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
          final long segmentOrd = values.getOrd(approximation.docID());
          if (segmentOrd != -1) {
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
          final int segmentOrd = values.getOrd(approximation.docID());
          if (segmentOrd != -1) {
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
