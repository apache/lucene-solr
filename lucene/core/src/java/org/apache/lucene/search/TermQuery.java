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
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A Query that matches documents containing a term. This may be combined with
 * other terms with a {@link BooleanQuery}.
 */
public class TermQuery extends Query {

  private final Term term;
  private final TermStates perReaderTermState;

  final class TermWeight extends Weight {
    private final Similarity similarity;
    private final Similarity.SimScorer simScorer;
    private final TermStates termStates;
    private final ScoreMode scoreMode;

    public TermWeight(IndexSearcher searcher, ScoreMode scoreMode,
        float boost, TermStates termStates) throws IOException {
      super(TermQuery.this);
      if (scoreMode.needsScores() && termStates == null) {
        throw new IllegalStateException("termStates are required when scores are needed");
      }
      this.scoreMode = scoreMode;
      this.termStates = termStates;
      this.similarity = searcher.getSimilarity();

      final CollectionStatistics collectionStats;
      final TermStatistics termStats;
      if (scoreMode.needsScores()) {
        collectionStats = searcher.collectionStatistics(term.field());
        termStats = searcher.termStatistics(term, termStates);
      } else {
        // we do not need the actual stats, use fake stats with docFreq=maxDoc=ttf=1
        collectionStats = new CollectionStatistics(term.field(), 1, 1, 1, 1);
        termStats = new TermStatistics(term.bytes(), 1, 1);
      }
     
      if (termStats == null) {
        this.simScorer = null; // term doesn't exist in any segment, we won't use similarity at all
      } else {
        this.simScorer = similarity.scorer(boost, collectionStats, termStats);
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      terms.add(getTerm());
    }

    @Override
    public String toString() {
      return "weight(" + TermQuery.this + ")";
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      assert termStates == null || termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) : "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);;
      final TermsEnum termsEnum = getTermsEnum(context);
      if (termsEnum == null) {
        return null;
      }
      IndexOptions indexOptions = context.reader()
          .getFieldInfos()
          .fieldInfo(getTerm().field())
          .getIndexOptions();
      float maxFreq = getMaxFreq(indexOptions, termsEnum.totalTermFreq(), termsEnum.docFreq());
      LeafSimScorer scorer = new LeafSimScorer(simScorer, context.reader(), scoreMode.needsScores(), maxFreq);
      return new TermScorer(this, termsEnum, scoreMode, scorer);
    }

    private long getMaxFreq(IndexOptions indexOptions, long ttf, long df) {
      // TODO: store the max term freq?
      if (indexOptions.compareTo(IndexOptions.DOCS) <= 0) {
        // omitTFAP field, tf values are implicitly 1.
        return 1;
      } else {
        assert ttf >= 0;
        return Math.min(Integer.MAX_VALUE, ttf - df + 1);
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    /**
     * Returns a {@link TermsEnum} positioned at this weights Term or null if
     * the term does not exist in the given context
     */
    private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
      assert termStates != null;
      assert termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) :
          "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
      final TermState state = termStates.get(context);
      if (state == null) { // term is not present in that reader
        assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
        return null;
      }
      final TermsEnum termsEnum = context.reader().terms(term.field()).iterator();
      termsEnum.seekExact(term.bytes(), state);
      return termsEnum;
    }

    private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
      // only called from assert
      // System.out.println("TQ.termNotInReader reader=" + reader + " term=" +
      // field + ":" + bytes.utf8ToString());
      return reader.docFreq(term) == 0;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      TermScorer scorer = (TermScorer) scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          LeafSimScorer docScorer = new LeafSimScorer(simScorer, context.reader(), true, Integer.MAX_VALUE);
          Explanation freqExplanation = Explanation.match(freq, "freq, occurrences of term within document");
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight(" + getQuery() + " in " + doc + ") ["
                  + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }
  }

  /** Constructs a query for the term <code>t</code>. */
  public TermQuery(Term t) {
    term = Objects.requireNonNull(t);
    perReaderTermState = null;
  }

  /**
   * Expert: constructs a TermQuery that will use the provided docFreq instead
   * of looking up the docFreq against the searcher.
   */
  public TermQuery(Term t, TermStates states) {
    assert states != null;
    term = Objects.requireNonNull(t);
    perReaderTermState = Objects.requireNonNull(states);
  }

  /** Returns the term of this query. */
  public Term getTerm() {
    return term;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final IndexReaderContext context = searcher.getTopReaderContext();
    final TermStates termState;
    if (perReaderTermState == null
        || perReaderTermState.wasBuiltFor(context) == false) {
      termState = TermStates.build(context, term, scoreMode.needsScores());
    } else {
      // PRTS was pre-build for this IS
      termState = this.perReaderTermState;
    }

    return new TermWeight(searcher, scoreMode, boost, termState);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!term.field().equals(field)) {
      buffer.append(term.field());
      buffer.append(":");
    }
    buffer.append(term.text());
    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           term.equals(((TermQuery) other).term);
  }

  @Override
  public int hashCode() {
    return classHash() ^ term.hashCode();
  }
}
