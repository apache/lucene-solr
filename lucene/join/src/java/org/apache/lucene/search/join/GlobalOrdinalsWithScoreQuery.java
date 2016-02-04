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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.ToStringUtils;

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
  private final IndexReader indexReader;

  GlobalOrdinalsWithScoreQuery(GlobalOrdinalsWithScoreCollector collector, String joinField, MultiDocValues.OrdinalMap globalOrds, Query toQuery, Query fromQuery, int min, int max, IndexReader indexReader) {
    this.collector = collector;
    this.joinField = joinField;
    this.globalOrds = globalOrds;
    this.toQuery = toQuery;
    this.fromQuery = fromQuery;
    this.min = min;
    this.max = max;
    this.indexReader = indexReader;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new W(this, toQuery.createWeight(searcher, false));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    GlobalOrdinalsWithScoreQuery that = (GlobalOrdinalsWithScoreQuery) o;

    if (min != that.min) return false;
    if (max != that.max) return false;
    if (!joinField.equals(that.joinField)) return false;
    if (!fromQuery.equals(that.fromQuery)) return false;
    if (!toQuery.equals(that.toQuery)) return false;
    if (!indexReader.equals(that.indexReader)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + joinField.hashCode();
    result = 31 * result + toQuery.hashCode();
    result = 31 * result + fromQuery.hashCode();
    result = 31 * result + min;
    result = 31 * result + max;
    result = 31 * result + indexReader.hashCode();
    return result;
  }

  @Override
  public String toString(String field) {
    return "GlobalOrdinalsQuery{" +
          "joinField=" + joinField +
          "min=" + min +
          "max=" + max +
          "fromQuery=" + fromQuery +
        '}' + ToStringUtils.boost(getBoost());
  }

  final class W extends Weight {

    private final Weight approximationWeight;

    W(Query query, Weight approximationWeight) {
      super(query);
      this.approximationWeight = approximationWeight;
    }

    @Override
    public void extractTerms(Set<Term> terms) {}

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      SortedDocValues values = DocValues.getSorted(context.reader(), joinField);
      if (values != null) {
        int segmentOrd = values.getOrd(doc);
        if (segmentOrd != -1) {
          final float score;
          if (globalOrds != null) {
            long globalOrd = globalOrds.getGlobalOrds(context.ord).get(segmentOrd);
            score = collector.score((int) globalOrd);
          } else {
            score = collector.score(segmentOrd);
          }
          BytesRef joinValue = values.lookupOrd(segmentOrd);
          return Explanation.match(score, "Score based on join value " + joinValue.utf8ToString());
        }
      }
      return Explanation.noMatch("Not a match");
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

      Scorer approximationScorer = approximationWeight.scorer(context);
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
