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
import java.util.Locale;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

class TermsIncludingScoreQuery extends Query implements Accountable {
  protected static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(TermsIncludingScoreQuery.class);

  private final ScoreMode scoreMode;
  private final String toField;
  private final boolean multipleValuesPerDocument;
  private final BytesRefHash terms;
  private final float[] scores;
  private final int[] ords;

  // These fields are used for equals() and hashcode() only
  private final Query fromQuery;
  private final String fromField;
  // id of the context rather than the context itself in order not to hold references to index
  // readers
  private final Object topReaderContextId;

  private final long ramBytesUsed; // cache

  TermsIncludingScoreQuery(
      ScoreMode scoreMode,
      String toField,
      boolean multipleValuesPerDocument,
      BytesRefHash terms,
      float[] scores,
      String fromField,
      Query fromQuery,
      Object indexReaderContextId) {
    this.scoreMode = scoreMode;
    this.toField = toField;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.terms = terms;
    this.scores = scores;
    this.ords = terms.sort();

    this.fromField = fromField;
    this.fromQuery = fromQuery;
    this.topReaderContextId = indexReaderContextId;

    this.ramBytesUsed =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(fromField)
            + RamUsageEstimator.sizeOfObject(
                fromQuery, RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED)
            + RamUsageEstimator.sizeOfObject(ords)
            + RamUsageEstimator.sizeOfObject(scores)
            + RamUsageEstimator.sizeOfObject(terms)
            + RamUsageEstimator.sizeOfObject(toField);
  }

  @Override
  public String toString(String string) {
    return String.format(
        Locale.ROOT, "TermsIncludingScoreQuery{field=%s;fromQuery=%s}", toField, fromQuery);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(toField)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(TermsIncludingScoreQuery other) {
    return Objects.equals(scoreMode, other.scoreMode)
        && Objects.equals(toField, other.toField)
        && Objects.equals(fromField, other.fromField)
        && Objects.equals(fromQuery, other.fromQuery)
        && Objects.equals(topReaderContextId, other.topReaderContextId);
  }

  @Override
  public int hashCode() {
    return classHash() + Objects.hash(scoreMode, toField, fromField, fromQuery, topReaderContextId);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Weight createWeight(
      IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost)
      throws IOException {
    if (scoreMode.needsScores() == false) {
      // We don't need scores then quickly change the query:
      TermsQuery termsQuery =
          new TermsQuery(toField, terms, fromField, fromQuery, topReaderContextId);
      return searcher
          .rewrite(termsQuery)
          .createWeight(searcher, org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, boost);
    }
    return new Weight(TermsIncludingScoreQuery.this) {

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Terms terms = context.reader().terms(toField);
        if (terms != null) {
          TermsEnum segmentTermsEnum = terms.iterator();
          BytesRef spare = new BytesRef();
          PostingsEnum postingsEnum = null;
          for (int i = 0; i < TermsIncludingScoreQuery.this.terms.size(); i++) {
            if (segmentTermsEnum.seekExact(
                TermsIncludingScoreQuery.this.terms.get(ords[i], spare))) {
              postingsEnum = segmentTermsEnum.postings(postingsEnum, PostingsEnum.NONE);
              if (postingsEnum.advance(doc) == doc) {
                final float score = TermsIncludingScoreQuery.this.scores[ords[i]];
                return Explanation.match(
                    score, "Score based on join value " + segmentTermsEnum.term().utf8ToString());
              }
            }
          }
        }
        return Explanation.noMatch("Not a match");
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        Terms terms = context.reader().terms(toField);
        if (terms == null) {
          return null;
        }

        // what is the runtime...seems ok?
        final long cost = context.reader().maxDoc() * terms.size();

        TermsEnum segmentTermsEnum = terms.iterator();
        if (multipleValuesPerDocument) {
          return new MVInOrderScorer(this, segmentTermsEnum, context.reader().maxDoc(), cost);
        } else {
          return new SVInOrderScorer(this, segmentTermsEnum, context.reader().maxDoc(), cost);
        }
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  class SVInOrderScorer extends Scorer {

    final DocIdSetIterator matchingDocsIterator;
    final float[] scores;
    final long cost;

    SVInOrderScorer(Weight weight, TermsEnum termsEnum, int maxDoc, long cost) throws IOException {
      super(weight);
      FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
      this.scores = new float[maxDoc];
      fillDocsAndScores(matchingDocs, termsEnum);
      this.matchingDocsIterator = new BitSetIterator(matchingDocs, cost);
      this.cost = cost;
    }

    protected void fillDocsAndScores(FixedBitSet matchingDocs, TermsEnum termsEnum)
        throws IOException {
      BytesRef spare = new BytesRef();
      PostingsEnum postingsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
          postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = postingsEnum.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = postingsEnum.nextDoc()) {
            matchingDocs.set(doc);
            // In the case the same doc is also related to a another doc, a score might be
            // overwritten. I think this
            // can only happen in a many-to-many relation
            scores[doc] = score;
          }
        }
      }
    }

    @Override
    public float score() throws IOException {
      return scores[docID()];
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }

    @Override
    public int docID() {
      return matchingDocsIterator.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
      return matchingDocsIterator;
    }
  }

  // This scorer deals with the fact that a document can have more than one score from multiple
  // related documents.
  class MVInOrderScorer extends SVInOrderScorer {

    MVInOrderScorer(Weight weight, TermsEnum termsEnum, int maxDoc, long cost) throws IOException {
      super(weight, termsEnum, maxDoc, cost);
    }

    @Override
    protected void fillDocsAndScores(FixedBitSet matchingDocs, TermsEnum termsEnum)
        throws IOException {
      BytesRef spare = new BytesRef();
      PostingsEnum postingsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
          postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = postingsEnum.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = postingsEnum.nextDoc()) {
            // I prefer this:
            /*if (scores[doc] < score) {
              scores[doc] = score;
              matchingDocs.set(doc);
            }*/
            // But this behaves the same as MVInnerScorer and only then the tests will pass:
            if (!matchingDocs.get(doc)) {
              scores[doc] = score;
              matchingDocs.set(doc);
            }
          }
        }
      }
    }
  }
}
