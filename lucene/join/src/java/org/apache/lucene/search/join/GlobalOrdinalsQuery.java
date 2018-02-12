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
import java.util.Set;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;

final class GlobalOrdinalsQuery extends Query {

  // All the ords of matching docs found with OrdinalsCollector.
  private final LongBitSet foundOrds;
  private final String joinField;
  private final OrdinalMap globalOrds;
  // Is also an approximation of the docs that will match. Can be all docs that have toField or something more specific.
  private final Query toQuery;

  // just for hashcode and equals:
  private final Query fromQuery;
  // id of the context rather than the context itself in order not to hold references to index readers
  private final Object indexReaderContextId;

  GlobalOrdinalsQuery(LongBitSet foundOrds, String joinField, OrdinalMap globalOrds, Query toQuery,
                      Query fromQuery, Object indexReaderContextId) {
    this.foundOrds = foundOrds;
    this.joinField = joinField;
    this.globalOrds = globalOrds;
    this.toQuery = toQuery;
    this.fromQuery = fromQuery;
    this.indexReaderContextId = indexReaderContextId;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    if (searcher.getTopReaderContext().id() != indexReaderContextId) {
      throw new IllegalStateException("Creating the weight against a different index reader than this query has been built for.");
    }
    return new W(this, toQuery.createWeight(searcher, false, 1f), boost);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(GlobalOrdinalsQuery other) {
    return fromQuery.equals(other.fromQuery) &&
           joinField.equals(other.joinField) &&
           toQuery.equals(other.toQuery) &&
           indexReaderContextId.equals(other.indexReaderContextId);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result = 31 * result + joinField.hashCode();
    result = 31 * result + toQuery.hashCode();
    result = 31 * result + fromQuery.hashCode();
    result = 31 * result + indexReaderContextId.hashCode();
    return result;
  }

  @Override
  public String toString(String field) {
    return "GlobalOrdinalsQuery{" +
        "joinField=" + joinField +
        '}';
  }

  final class W extends ConstantScoreWeight {

    private final Weight approximationWeight;

    W(Query query, Weight approximationWeight, float boost) {
      super(query, boost);
      this.approximationWeight = approximationWeight;
    }

    @Override
    public void extractTerms(Set<Term> terms) {}

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
      if (foundOrds.get(ord) == false) {
        return Explanation.noMatch("Not a match, join value " + Term.toString(joinValue));
      }

      return Explanation.match(score(), "A match, join value " + Term.toString(joinValue));
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      SortedDocValues values = DocValues.getSorted(context.reader(), joinField);
      if (values == null) {
        return null;
      }

      Scorer approximationScorer = approximationWeight.scorer(context);
      if (approximationScorer == null) {
        return null;
      }
      if (globalOrds != null) {
        return new OrdinalMapScorer(this, score(), foundOrds, values, approximationScorer.iterator(), globalOrds.getGlobalOrds(context.ord));
      } {
        return new SegmentOrdinalScorer(this, score(), foundOrds, values, approximationScorer.iterator());
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

    final LongBitSet foundOrds;
    final LongValues segmentOrdToGlobalOrdLookup;

    public OrdinalMapScorer(Weight weight, float score, LongBitSet foundOrds, SortedDocValues values, DocIdSetIterator approximationScorer, LongValues segmentOrdToGlobalOrdLookup) {
      super(weight, values, approximationScorer);
      this.score = score;
      this.foundOrds = foundOrds;
      this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
    }

    @Override
    protected TwoPhaseIterator createTwoPhaseIterator(DocIdSetIterator approximation) {
      return new TwoPhaseIterator(approximation) {

        @Override
        public boolean matches() throws IOException {
          if (values.advanceExact(approximation.docID())) {
            final long segmentOrd = values.ordValue();
            final long globalOrd = segmentOrdToGlobalOrdLookup.get(segmentOrd);
            if (foundOrds.get(globalOrd)) {
              return true;
            }
          }
          return false;
        }

        @Override
        public float matchCost() {
          return 100; // TODO: use cost of values.getOrd() and foundOrds.get()
        }
      };
    }
  }

  final static class SegmentOrdinalScorer extends BaseGlobalOrdinalScorer {

    final LongBitSet foundOrds;

    public SegmentOrdinalScorer(Weight weight, float score, LongBitSet foundOrds, SortedDocValues values, DocIdSetIterator approximationScorer) {
      super(weight, values, approximationScorer);
      this.score = score;
      this.foundOrds = foundOrds;
    }

    @Override
    protected TwoPhaseIterator createTwoPhaseIterator(DocIdSetIterator approximation) {
      return new TwoPhaseIterator(approximation) {

        @Override
        public boolean matches() throws IOException {
          if (values.advanceExact(approximation.docID()) && foundOrds.get(values.ordValue())) {
            return true;
          }
          return false;
        }

        @Override
        public float matchCost() {
          return 100; // TODO: use cost of values.getOrd() and foundOrds.get()
        }
      };
    }

  }
}
