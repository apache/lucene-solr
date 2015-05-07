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
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;

/**
 * A wrapper for {@link MultiTermQuery}, that exposes its
 * functionality as a {@link Filter}.
 * <P>
 * <code>MultiTermQueryWrapperFilter</code> is not designed to
 * be used by itself. Normally you subclass it to provide a Filter
 * counterpart for a {@link MultiTermQuery} subclass.
 * <P>
 * This class also provides the functionality behind
 * {@link MultiTermQuery#CONSTANT_SCORE_REWRITE};
 * this is why it is not abstract.
 */
final class MultiTermQueryConstantScoreWrapper<Q extends MultiTermQuery> extends Query {

  protected final Q query;

  /**
   * Wrap a {@link MultiTermQuery} as a Filter.
   */
  protected MultiTermQueryConstantScoreWrapper(Q query) {
      this.query = query;
  }

  @Override
  public String toString(String field) {
    // query.toString should be ok for the filter, too, if the query boost is 1.0f
    return query.toString(field);
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (o==null) return false;
    if (this.getClass().equals(o.getClass())) {
      final MultiTermQueryConstantScoreWrapper that = (MultiTermQueryConstantScoreWrapper) o;
      return this.query.equals(that.query) && this.getBoost() == that.getBoost();
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return Objects.hash(getClass(), query, getBoost());
  }

  /** Returns the field name for this query */
  public final String getField() { return query.getField(); }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {
      @Override
      public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final Terms terms = context.reader().terms(query.field);
        if (terms == null) {
          // field does not exist
          return null;
        }

        final TermsEnum termsEnum = query.getTermsEnum(terms);
        assert termsEnum != null;

        BitDocIdSet.Builder builder = new BitDocIdSet.Builder(context.reader().maxDoc());
        PostingsEnum docs = null;
        while (termsEnum.next() != null) {
          docs = termsEnum.postings(acceptDocs, docs, PostingsEnum.NONE);
          builder.or(docs);
        }
        final BitDocIdSet set = builder.build();
        if (set == null) {
          return null;
        }
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), disi);
      }
    };
  }
}
