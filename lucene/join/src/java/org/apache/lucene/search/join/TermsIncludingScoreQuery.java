package org.apache.lucene.search.join;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

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
  public void extractTerms(Set<Term> terms) {
    originalQuery.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query originalQueryRewrite = originalQuery.rewrite(reader);
    if (originalQueryRewrite != originalQuery) {
      Query rewritten = new TermsIncludingScoreQuery(field, multipleValuesPerDocument, terms, scores,
          ords, originalQueryRewrite, originalQuery);
      rewritten.setBoost(getBoost());
      return rewritten;
    } else {
      return this;
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
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    final Weight originalWeight = originalQuery.createWeight(searcher);
    return new Weight() {

      private TermsEnum segmentTermsEnum;

      @Override
      public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
        SVInnerScorer scorer = (SVInnerScorer) scorer(context, false, false, context.reader().getLiveDocs());
        if (scorer != null) {
          if (scorer.advanceForExplainOnly(doc) == doc) {
            return scorer.explain();
          }
        }
        return new ComplexExplanation(false, 0.0f, "Not a match");
      }

      @Override
      public Query getQuery() {
        return TermsIncludingScoreQuery.this;
      }

      @Override
      public float getValueForNormalization() throws IOException {
        return originalWeight.getValueForNormalization() * TermsIncludingScoreQuery.this.getBoost() * TermsIncludingScoreQuery.this.getBoost();
      }

      @Override
      public void normalize(float norm, float topLevelBoost) {
        originalWeight.normalize(norm, topLevelBoost * TermsIncludingScoreQuery.this.getBoost());
      }

      @Override
      public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(field);
        if (terms == null) {
          return null;
        }
        
        // what is the runtime...seems ok?
        final long cost = context.reader().maxDoc() * terms.size();

        segmentTermsEnum = terms.iterator(segmentTermsEnum);
        if (scoreDocsInOrder) {
          if (multipleValuesPerDocument) {
            return new MVInOrderScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
          } else {
            return new SVInOrderScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
          }
        } else if (multipleValuesPerDocument) {
          return new MVInnerScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
        } else {
          return new SVInnerScorer(this, acceptDocs, segmentTermsEnum, cost);
        }
      }
    };
  }

  // This impl assumes that the 'join' values are used uniquely per doc per field. Used for one to many relations.
  class SVInnerScorer extends Scorer {

    final BytesRef spare = new BytesRef();
    final Bits acceptDocs;
    final TermsEnum termsEnum;
    final long cost;

    int upto;
    DocsEnum docsEnum;
    DocsEnum reuse;
    int scoreUpto;
    int doc;

    SVInnerScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum, long cost) {
      super(weight);
      this.acceptDocs = acceptDocs;
      this.termsEnum = termsEnum;
      this.cost = cost;
      this.doc = -1;
    }

    @Override
    public void score(Collector collector) throws IOException {
      collector.setScorer(this);
      for (int doc = nextDocOutOfOrder(); doc != NO_MORE_DOCS; doc = nextDocOutOfOrder()) {
        collector.collect(doc);
      }
    }

    @Override
    public float score() throws IOException {
      return scores[ords[scoreUpto]];
    }

    public Explanation explain() throws IOException {
      return new ComplexExplanation(true, score(), "Score based on join value " + termsEnum.term().utf8ToString());
    }

    @Override
    public int docID() {
      return doc;
    }

    int nextDocOutOfOrder() throws IOException {
      if (docsEnum != null) {
        int docId = docsEnum.nextDoc();
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          docsEnum = null;
        } else {
          return doc = docId;
        }
      }

      do {
        if (upto == terms.size()) {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        }

        scoreUpto = upto;
        if (termsEnum.seekExact(terms.get(ords[upto++], spare), true)) {
          docsEnum = reuse = termsEnum.docs(acceptDocs, reuse, DocsEnum.FLAG_NONE);
        }
      } while (docsEnum == null);

      return doc = docsEnum.nextDoc();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException("nextDoc() isn't supported because doc ids are emitted out of order");
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException("advance() isn't supported because doc ids are emitted out of order");
    }

    private int advanceForExplainOnly(int target) throws IOException {
      int docId;
      do {
        docId = nextDocOutOfOrder();
        if (docId < target) {
          int tempDocId = docsEnum.advance(target);
          if (tempDocId == target) {
            docId = tempDocId;
            break;
          }
        } else if (docId == target) {
          break;
        }
        docsEnum = null; // goto the next ord.
      } while (docId != DocIdSetIterator.NO_MORE_DOCS);
      return docId;
    }

    @Override
    public int freq() {
      return 1;
    }

    @Override
    public long cost() {
      return cost;
    }
  }

  // This impl that tracks whether a docid has already been emitted. This check makes sure that docs aren't emitted
  // twice for different join values. This means that the first encountered join value determines the score of a document
  // even if other join values yield a higher score.
  class MVInnerScorer extends SVInnerScorer {

    final FixedBitSet alreadyEmittedDocs;

    MVInnerScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum, int maxDoc, long cost) {
      super(weight, acceptDocs, termsEnum, cost);
      alreadyEmittedDocs = new FixedBitSet(maxDoc);
    }

    @Override
    int nextDocOutOfOrder() throws IOException {
      if (docsEnum != null) {
        int docId;
        do {
          docId = docsEnum.nextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
        } while (alreadyEmittedDocs.get(docId));
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          docsEnum = null;
        } else {
          alreadyEmittedDocs.set(docId);
          return docId;
        }
      }

      for (;;) {
        do {
          if (upto == terms.size()) {
            return DocIdSetIterator.NO_MORE_DOCS;
          }

          scoreUpto = upto;
          if (termsEnum.seekExact(terms.get(ords[upto++], spare), true)) {
            docsEnum = reuse = termsEnum.docs(acceptDocs, reuse, DocsEnum.FLAG_NONE);
          }
        } while (docsEnum == null);

        int docId;
        do {
          docId = docsEnum.nextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
        } while (alreadyEmittedDocs.get(docId));
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          docsEnum = null;
        } else {
          alreadyEmittedDocs.set(docId);
          return docId;
        }
      }
    }
  }

  class SVInOrderScorer extends Scorer {

    final DocIdSetIterator matchingDocsIterator;
    final float[] scores;
    final long cost;

    int currentDoc = -1;

    SVInOrderScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum, int maxDoc, long cost) throws IOException {
      super(weight);
      FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
      this.scores = new float[maxDoc];
      fillDocsAndScores(matchingDocs, acceptDocs, termsEnum);
      this.matchingDocsIterator = matchingDocs.iterator();
      this.cost = cost;
    }

    protected void fillDocsAndScores(FixedBitSet matchingDocs, Bits acceptDocs, TermsEnum termsEnum) throws IOException {
      BytesRef spare = new BytesRef();
      DocsEnum docsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare), true)) {
          docsEnum = termsEnum.docs(acceptDocs, docsEnum, DocsEnum.FLAG_NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = docsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docsEnum.nextDoc()) {
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
      return scores[currentDoc];
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int docID() {
      return currentDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      return currentDoc = matchingDocsIterator.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return currentDoc = matchingDocsIterator.advance(target);
    }

    @Override
    public long cost() {
      return cost;
    }
  }

  // This scorer deals with the fact that a document can have more than one score from multiple related documents.
  class MVInOrderScorer extends SVInOrderScorer {

    MVInOrderScorer(Weight weight, Bits acceptDocs, TermsEnum termsEnum, int maxDoc, long cost) throws IOException {
      super(weight, acceptDocs, termsEnum, maxDoc, cost);
    }

    @Override
    protected void fillDocsAndScores(FixedBitSet matchingDocs, Bits acceptDocs, TermsEnum termsEnum) throws IOException {
      BytesRef spare = new BytesRef();
      DocsEnum docsEnum = null;
      for (int i = 0; i < terms.size(); i++) {
        if (termsEnum.seekExact(terms.get(ords[i], spare), true)) {
          docsEnum = termsEnum.docs(acceptDocs, docsEnum, DocsEnum.FLAG_NONE);
          float score = TermsIncludingScoreQuery.this.scores[ords[i]];
          for (int doc = docsEnum.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docsEnum.nextDoc()) {
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
