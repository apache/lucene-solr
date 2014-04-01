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

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

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
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.FixedBitSet;

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
        SVInnerScorer scorer = (SVInnerScorer) bulkScorer(context, false, null);
        if (scorer != null) {
          return scorer.explain(doc);
        }
        return new ComplexExplanation(false, 0.0f, "Not a match");
      }

      @Override
      public boolean scoresDocsOutOfOrder() {
        // We have optimized impls below if we are allowed
        // to score out-of-order:
        return true;
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
      public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(field);
        if (terms == null) {
          return null;
        }
        
        // what is the runtime...seems ok?
        final long cost = context.reader().maxDoc() * terms.size();

        segmentTermsEnum = terms.iterator(segmentTermsEnum);
        if (multipleValuesPerDocument) {
          return new MVInOrderScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
        } else {
          return new SVInOrderScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
        }
      }

      @Override
      public BulkScorer bulkScorer(AtomicReaderContext context, boolean scoreDocsInOrder, Bits acceptDocs) throws IOException {

        if (scoreDocsInOrder) {
          return super.bulkScorer(context, scoreDocsInOrder, acceptDocs);
        } else {
          Terms terms = context.reader().terms(field);
          if (terms == null) {
            return null;
          }
          // what is the runtime...seems ok?
          final long cost = context.reader().maxDoc() * terms.size();

          segmentTermsEnum = terms.iterator(segmentTermsEnum);
          // Optimized impls that take advantage of docs
          // being allowed to be out of order:
          if (multipleValuesPerDocument) {
            return new MVInnerScorer(this, acceptDocs, segmentTermsEnum, context.reader().maxDoc(), cost);
          } else {
            return new SVInnerScorer(this, acceptDocs, segmentTermsEnum, cost);
          }
        }
      }
    };
  }

  // This impl assumes that the 'join' values are used uniquely per doc per field. Used for one to many relations.
  class SVInnerScorer extends BulkScorer {

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
      this.acceptDocs = acceptDocs;
      this.termsEnum = termsEnum;
      this.cost = cost;
      this.doc = -1;
    }

    @Override
    public boolean score(Collector collector, int max) throws IOException {
      FakeScorer fakeScorer = new FakeScorer();
      collector.setScorer(fakeScorer);
      if (doc == -1) {
        doc = nextDocOutOfOrder();
      }
      while(doc < max) {
        fakeScorer.doc = doc;
        fakeScorer.score = scores[ords[scoreUpto]];
        collector.collect(doc);
        doc = nextDocOutOfOrder();
      }

      return doc != DocsEnum.NO_MORE_DOCS;
    }

    int nextDocOutOfOrder() throws IOException {
      while (true) {
        if (docsEnum != null) {
          int docId = docsEnumNextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            docsEnum = null;
          } else {
            return doc = docId;
          }
        }

        if (upto == terms.size()) {
          return doc = DocIdSetIterator.NO_MORE_DOCS;
        }

        scoreUpto = upto;
        if (termsEnum.seekExact(terms.get(ords[upto++], spare))) {
          docsEnum = reuse = termsEnum.docs(acceptDocs, reuse, DocsEnum.FLAG_NONE);
        }
      }
    }

    protected int docsEnumNextDoc() throws IOException {
      return docsEnum.nextDoc();
    }

    private Explanation explain(int target) throws IOException {
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

      return new ComplexExplanation(true, scores[ords[scoreUpto]], "Score based on join value " + termsEnum.term().utf8ToString());
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
    protected int docsEnumNextDoc() throws IOException {
      while (true) {
        int docId = docsEnum.nextDoc();
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          return docId;
        }
        if (!alreadyEmittedDocs.getAndSet(docId)) {
          return docId;//if it wasn't previously set, return it
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
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
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
        if (termsEnum.seekExact(terms.get(ords[i], spare))) {
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
