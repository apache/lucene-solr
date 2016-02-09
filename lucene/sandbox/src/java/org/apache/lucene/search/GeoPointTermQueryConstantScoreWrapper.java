package org.apache.lucene.search;

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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Custom ConstantScoreWrapper for {@code GeoPointTermQuery} that cuts over to DocValues
 * for post filtering boundary ranges. Multi-valued GeoPoint documents are supported.
 *
 * @lucene.experimental
 */
final class GeoPointTermQueryConstantScoreWrapper <Q extends GeoPointTermQuery> extends Query {
  protected final Q query;

  protected GeoPointTermQueryConstantScoreWrapper(Q query) {
    this.query = query;
  }

  @Override
  public String toString(String field) {
    return query.toString();
  }

  @Override
  public final boolean equals(final Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    final GeoPointTermQueryConstantScoreWrapper<?> that = (GeoPointTermQueryConstantScoreWrapper<?>) o;
    return this.query.equals(that.query);
  }

  @Override
  public final int hashCode() {
    return 31 * super.hashCode() + query.hashCode();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final Terms terms = context.reader().terms(query.field);
        if (terms == null) {
          return null;
        }

        final GeoPointTermsEnum termsEnum = (GeoPointTermsEnum)(query.getTermsEnum(terms));
        assert termsEnum != null;

        LeafReader reader = context.reader();
        DocIdSetBuilder builder = new DocIdSetBuilder(reader.maxDoc());
        // subset of documents that need no postfiltering, this is purely an optimization
        final BitSet preApproved;
        // dumb heuristic: if the field is really sparse, use a sparse impl
        if (terms.getDocCount() * 100L < reader.maxDoc()) {
          preApproved = new SparseFixedBitSet(reader.maxDoc());
        } else {
          preApproved = new FixedBitSet(reader.maxDoc());
        }
        PostingsEnum docs = null;

        while (termsEnum.next() != null) {
          docs = termsEnum.postings(docs, PostingsEnum.NONE);
          // boundary terms need post filtering by
          if (termsEnum.boundaryTerm()) {
            builder.add(docs);
          } else {
            int docId;
            while ((docId = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              builder.add(docId);
              preApproved.set(docId);
            }
          }
        }

        DocIdSet set = builder.build();
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }

        // return two-phase iterator using docvalues to postfilter candidates
        final SortedNumericDocValues sdv = reader.getSortedNumericDocValues(query.getField());
        TwoPhaseIterator iterator = new TwoPhaseIterator(disi) {
          @Override
          public boolean matches() throws IOException {
            int docId = disi.docID();
            if (preApproved.get(docId)) {
              return true;
            } else {
              sdv.setDocument(docId);
              int count = sdv.count();
              for (int i = 0; i < count; i++) {
                long hash = sdv.valueAt(i);
                if (termsEnum.postFilter(GeoUtils.mortonUnhashLon(hash), GeoUtils.mortonUnhashLat(hash))) {
                  return true;
                }
              }
              return false;
            }
          }

          @Override
          public float matchCost() {
            return 20; // TODO: make this fancier
          }
        };
        return new ConstantScoreScorer(this, score(), iterator);
      }
    };
  }
}
