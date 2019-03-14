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
package org.apache.lucene.search.spans;


import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;

/** Matches spans containing a term.
 * This should not be used for terms that are indexed at position Integer.MAX_VALUE.
 */
public class SpanTermQuery extends SpanQuery {

  protected final Term term;
  protected final TermStates termStates;

  /** Construct a SpanTermQuery matching the named term's spans. */
  public SpanTermQuery(Term term) {
    this.term = Objects.requireNonNull(term);
    this.termStates = null;
  }

  /**
   * Expert: Construct a SpanTermQuery matching the named term's spans, using
   * the provided TermStates
   */
  public SpanTermQuery(Term term, TermStates termStates) {
    this.term = Objects.requireNonNull(term);
    this.termStates = termStates;
  }

  /** Return the term whose spans are matched. */
  public Term getTerm() { return term; }

  /** Returns the {@link TermStates} passed to the constructor, or null if it was not passed.
   *
   * @lucene.experimental */
  public TermStates getTermStates() {
    return termStates;
  }

  @Override
  public String getField() { return term.field(); }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final TermStates context;
    final IndexReaderContext topContext = searcher.getTopReaderContext();
    if (termStates == null || termStates.wasBuiltFor(topContext) == false) {
      context = TermStates.build(topContext, term, scoreMode.needsScores());
    }
    else {
      context = termStates;
    }
    return new SpanTermWeight(context, searcher, scoreMode.needsScores() ? Collections.singletonMap(term, context) : null, boost);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(term.field())) {
      visitor.consumeTerms(this, term);
    }
  }

  public class SpanTermWeight extends SpanWeight {

    final TermStates termStates;

    public SpanTermWeight(TermStates termStates, IndexSearcher searcher, Map<Term, TermStates> terms, float boost) throws IOException {
      super(SpanTermQuery.this, searcher, terms, boost);
      this.termStates = termStates;
      assert termStates != null : "TermStates must not be null";
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      terms.add(term);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      contexts.put(term, termStates);
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings) throws IOException {

      assert termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) : "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);

      final TermState state = termStates.get(context);
      if (state == null) { // term is not present in that reader
        assert context.reader().docFreq(term) == 0 : "no termstate found but term exists in reader term=" + term;
        return null;
      }

      final Terms terms = context.reader().terms(term.field());
      if (terms == null)
        return null;
      if (terms.hasPositions() == false)
        throw new IllegalStateException("field \"" + term.field() + "\" was indexed without position data; cannot run SpanTermQuery (term=" + term.text() + ")");

      final TermsEnum termsEnum = terms.iterator();
      termsEnum.seekExact(term.bytes(), state);

      final PostingsEnum postings = termsEnum.postings(null, requiredPostings.getRequiredPostings());
      float positionsCost = termPositionsCost(termsEnum) * PHRASE_TO_SPAN_TERM_POSITIONS_COST;
      return new TermSpans(getSimScorer(context), postings, term, positionsCost);
    }
  }

  /** A guess of
   * the relative cost of dealing with the term positions
   * when using a SpanNearQuery instead of a PhraseQuery.
   */
  private static final float PHRASE_TO_SPAN_TERM_POSITIONS_COST = 4.0f;

  private static final int TERM_POSNS_SEEK_OPS_PER_DOC = 128;

  private static final int TERM_OPS_PER_POS = 7;

  /** Returns an expected cost in simple operations
   *  of processing the occurrences of a term
   *  in a document that contains the term.
   *  @param termsEnum The term is the term at which this TermsEnum is positioned.
   *  <p>
   *  This is a copy of org.apache.lucene.search.PhraseQuery.termPositionsCost().
   *  <br>
   *  TODO: keep only a single copy of this method and the constants used in it
   *  when SpanTermQuery moves to the o.a.l.search package.
   */
  static float termPositionsCost(TermsEnum termsEnum) throws IOException {
    int docFreq = termsEnum.docFreq();
    assert docFreq > 0;
    long totalTermFreq = termsEnum.totalTermFreq();
    assert totalTermFreq > 0;
    float expOccurrencesInMatchingDoc = totalTermFreq / (float) docFreq;
    return TERM_POSNS_SEEK_OPS_PER_DOC + expOccurrencesInMatchingDoc * TERM_OPS_PER_POS;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (term.field().equals(field))
      buffer.append(term.text());
    else
      buffer.append(term.toString());
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    return classHash() ^ term.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           term.equals(((SpanTermQuery) other).term);
  }

}
