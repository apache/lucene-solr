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
import java.io.PrintStream;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

class TermsIncludingScoreQuery extends Query {

  final String field;
  final boolean multipleValuesPerDocument;
  final BytesRefHash terms;
  final float[] scores;
  final int[] ords;
  final Query originalQuery;
  final Query unwrittenOriginalQuery;

  TermsIncludingScoreQuery(String field, boolean multipleValuesPerDocument, BytesRefHash terms, float[] scores, Query originalQuery) {
    this.field = field;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.terms = terms;
    this.scores = scores;
    this.originalQuery = originalQuery;
    this.ords = terms.sort(BytesRef.getUTF8SortedAsUnicodeComparator());
    this.unwrittenOriginalQuery = originalQuery;
  }

  private TermsIncludingScoreQuery(String field, boolean multipleValuesPerDocument, BytesRefHash terms, float[] scores, int[] ords, Query originalQuery, Query unwrittenOriginalQuery) {
    this.field = field;
    this.multipleValuesPerDocument = multipleValuesPerDocument;
    this.terms = terms;
    this.scores = scores;
    this.originalQuery = originalQuery;
    this.ords = ords;
    this.unwrittenOriginalQuery = unwrittenOriginalQuery;
  }

  @Override
  public String toString(String string) {
    return String.format(Locale.ROOT, "TermsIncludingScoreQuery{field=%s;originalQuery=%s}", field, unwrittenOriginalQuery);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    final Query originalQueryRewrite = originalQuery.rewrite(reader);
    if (originalQueryRewrite != originalQuery) {
      return new TermsIncludingScoreQuery(field, multipleValuesPerDocument, terms, scores,
          ords, originalQueryRewrite, originalQuery);
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } if (!super.equals(obj)) {
      return false;
    } if (getClass() != obj.getClass()) {
      return false;
    }

    TermsIncludingScoreQuery other = (TermsIncludingScoreQuery) obj;
    if (!field.equals(other.field)) {
      return false;
    }
    if (!unwrittenOriginalQuery.equals(other.unwrittenOriginalQuery)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result += prime * field.hashCode();
    result += prime * unwrittenOriginalQuery.hashCode();
    return result;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight originalWeight = originalQuery.createWeight(searcher, needsScores);
    return new Weight(TermsIncludingScoreQuery.this) {

      @Override
      public void extractTerms(Set<Term> terms) {}

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        Terms terms = context.reader().terms(field);
        if (terms != null) {
          TermsEnum segmentTermsEnum = terms.iterator();
          BytesRef spare = new BytesRef();
          PostingsEnum postingsEnum = null;
          for (int i = 0; i < TermsIncludingScoreQuery.this.terms.size(); i++) {
            if (segmentTermsEnum.seekExact(TermsIncludingScoreQuery.this.terms.get(ords[i], spare))) {
              postingsEnum = segmentTermsEnum.postings(postingsEnum, PostingsEnum.NONE);
              if (postingsEnum.advance(doc) == doc) {
                final float score = TermsIncludingScoreQuery.this.scores[ords[i]];
                return Explanation.match(score, "Score based on join value " + segmentTermsEnum.term().utf8ToString());
              }
            }
          }
        }
        return Explanation.noMatch("Not a match");
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return originalWeight.getValueForNormalization();
      }

      @Override
      public void normalize(float norm, float boost) {
        originalWeight.normalize(norm, boost);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        Terms terms = context.reader().terms(field);
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

    protected void fillDocsAndScores(FixedBitSet matchingDocs, TermsEnum termsEnum) throws IOException {
      BytesRef spare = new BytesRef();
      PostingsEnum postingsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
          postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = postingsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postingsEnum.nextDoc()) {
            matchingDocs.set(doc);
            // In the case the same doc is also related to a another doc, a score might be overwritten. I think this
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
    public int freq() throws IOException {
      return 1;
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

  // This scorer deals with the fact that a document can have more than one score from multiple related documents.
  class MVInOrderScorer extends SVInOrderScorer {

    MVInOrderScorer(Weight weight, TermsEnum termsEnum, int maxDoc, long cost) throws IOException {
      super(weight, termsEnum, maxDoc, cost);
    }

    @Override
    protected void fillDocsAndScores(FixedBitSet matchingDocs, TermsEnum termsEnum) throws IOException {
      BytesRef spare = new BytesRef();
      PostingsEnum postingsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
          postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = postingsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postingsEnum.nextDoc()) {
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
  
  void dump(PrintStream out){
    out.println(field+":");
    final BytesRef ref = new BytesRef();
    for (int i = 0; i < terms.size(); i++) {
      terms.get(ords[i], ref);
      out.print(ref+" "+ref.utf8ToString()+" ");
      try {
        out.print(Long.toHexString(NumericUtils.prefixCodedToLong(ref))+"L");
      } catch (Exception e) {
        try {
          out.print(Integer.toHexString(NumericUtils.prefixCodedToInt(ref))+"i");
        } catch (Exception ee) {
        }
      }
      out.println(" score="+scores[ords[i]]);
      out.println("");
    }
  }
}
