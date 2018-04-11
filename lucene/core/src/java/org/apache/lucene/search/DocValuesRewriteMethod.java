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
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.LongBitSet;

/**
 * Rewrites MultiTermQueries into a filter, using DocValues for term enumeration.
 * <p>
 * This can be used to perform these queries against an unindexed docvalues field.
 * @lucene.experimental
 */
public final class DocValuesRewriteMethod extends MultiTermQuery.RewriteMethod {
  
  @Override
  public Query rewrite(IndexReader reader, MultiTermQuery query) {
    return new ConstantScoreQuery(new MultiTermQueryDocValuesWrapper(query));
  }
  
  static class MultiTermQueryDocValuesWrapper extends Query {
    
    protected final MultiTermQuery query;
    
    /**
     * Wrap a {@link MultiTermQuery} as a Filter.
     */
    protected MultiTermQueryDocValuesWrapper(MultiTermQuery query) {
      this.query = query;
    }
    
    @Override
    public String toString(String field) {
      // query.toString should be ok for the filter, too, if the query boost is 1.0f
      return query.toString(field);
    }
    
    @Override
    public final boolean equals(final Object other) {
      return sameClassAs(other) &&
             query.equals(((MultiTermQueryDocValuesWrapper) other).query);
    }

    @Override
    public final int hashCode() {
      return 31 * classHash() + query.hashCode();
    }
    
    /** Returns the field name for this query */
    public final String getField() { return query.getField(); }
    
    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      return new ConstantScoreWeight(this, boost) {

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
          final SortedSetDocValues fcsi = DocValues.getSortedSet(context.reader(), query.field);
          return Matches.forField(query.field, () -> DisjunctionMatchesIterator.fromTermsEnum(context, doc, query.field, getTermsEnum(fcsi)));
        }

        private TermsEnum getTermsEnum(SortedSetDocValues fcsi) throws IOException {
          return query.getTermsEnum(new Terms() {

            @Override
            public TermsEnum iterator() throws IOException {
              return fcsi.termsEnum();
            }

            @Override
            public long getSumTotalTermFreq() {
              return -1;
            }

            @Override
            public long getSumDocFreq() {
              return -1;
            }

            @Override
            public int getDocCount() {
              return -1;
            }

            @Override
            public long size() {
              return -1;
            }

            @Override
            public boolean hasFreqs() {
              return false;
            }

            @Override
            public boolean hasOffsets() {
              return false;
            }

            @Override
            public boolean hasPositions() {
              return false;
            }

            @Override
            public boolean hasPayloads() {
              return false;
            }
          });
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          final SortedSetDocValues fcsi = DocValues.getSortedSet(context.reader(), query.field);
          TermsEnum termsEnum = getTermsEnum(fcsi);
          assert termsEnum != null;
          if (termsEnum.next() == null) {
            // no matching terms
            return null;
          }
          // fill into a bitset
          // Cannot use FixedBitSet because we require long index (ord):
          final LongBitSet termSet = new LongBitSet(fcsi.getValueCount());
          do {
            long ord = termsEnum.ord();
            if (ord >= 0) {
              termSet.set(ord);
            }
          } while (termsEnum.next() != null);

          return new ConstantScoreScorer(this, score(), new TwoPhaseIterator(fcsi) {

            @Override
            public boolean matches() throws IOException {
              for (long ord = fcsi.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = fcsi.nextOrd()) {
                if (termSet.get(ord)) {
                  return true;
                }
              }
              return false;
            }

            @Override
            public float matchCost() {
              return 3; // lookup in a bitset
            }
          });
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return DocValues.isCacheable(ctx, query.field);
        }

      };
    }
  }

  @Override
  public boolean equals(Object other) {
    return other != null &&
           getClass() == other.getClass();
  }

  @Override
  public int hashCode() {
    return 641;
  }
}
