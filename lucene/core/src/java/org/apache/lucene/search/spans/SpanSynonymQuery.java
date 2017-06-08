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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;

import org.apache.lucene.search.similarities.Similarity.SimScorer;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.SynonymQuery.SynonymWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchNoDocsQuery;

/**
 * A SpanQuery that treats terms as synonyms.
 * <p>
 * For scoring purposes, this query tries to score the terms as if you
 * had indexed them as one term: it will match any of the terms while
 * using the same scoring as {@link SynonymQuery}, as far as possible.
 */
public final class SpanSynonymQuery extends SpanQuery {
  final SynonymQuery synonymQuery;
  final List<Term> terms;

  /**
   * Creates a new SpanSynonymQuery, matching any of the supplied terms.
   * <p>
   * The terms must all have the same field.
   */
  public SpanSynonymQuery(Term... terms) {
    this.synonymQuery = new SynonymQuery(terms);
    this.terms = synonymQuery.getTerms();
  }

  @Override
  public String getField() {
    return synonymQuery.getField();
  }

  public List<Term> getTerms() {
    return terms;
  }

  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("SpanSynonym(");
    builder.append(synonymQuery.toString(field));
    builder.append(")");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return 31 * classHash() - synonymQuery.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           synonymQuery.equals(((SpanSynonymQuery) other).synonymQuery);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // optimize zero and single term cases
    int numTerms = terms.size();
    if (numTerms == 0) {
      return new MatchNoDocsQuery();
    }
    if (numTerms == 1) {
      return new SpanTermQuery(terms.get(0));
    }
    return this;
  }

  /** The returned SpanWeight does not support {@link SpanWeight#explain}. */
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    if (needsScores) {
      SynonymWeight synonymWeight = (SynonymWeight)
                      synonymQuery.createWeight(searcher, needsScores, boost);
      return new SpanSynonymWeight(searcher, boost, synonymWeight);
    }
    else { // scores not needed, use SpanOrQuery without scoring.
      SpanTermQuery[] clauses = new SpanTermQuery[terms.size()];
      int i = 0;
      for (Term term : terms) {
        clauses[i++] = new SpanTermQuery(term);
      }
      return new SpanOrQuery(clauses).createWeight(searcher, needsScores, boost);
    }
  }

  class SpanSynonymWeight extends SpanWeight {
    final SynonymWeight synonymWeight;

    SpanSynonymWeight(
            IndexSearcher searcher,
            float boost,
            SynonymWeight synonymWeight)
    throws IOException {
      super(SpanSynonymQuery.this, searcher, null, boost); // null: no term context map
      this.synonymWeight = synonymWeight;
    }

    @Override
    public void extractTerms(Set<Term> termSet) {
      for (Term t : terms) {
        termSet.add(t);
      }
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> termContextbyTerm) {
      TermContext[] termContexts = synonymWeight.getTermContexts();
      int i = 0;
      for (Term term : terms) {
        TermContext termContext = termContexts[i++];
        termContextbyTerm.put(term, termContext);
      }
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SimScorer getSimScorer(LeafReaderContext context) throws IOException {
      return synonymWeight.getSimScorer(context);
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings)
    throws IOException {
      SimScorer simScorer = getSimScorer(context);
      final String field = getField();
      Terms fieldTerms = context.reader().terms(field);
      List<Spans> termSpans = new ArrayList<>(terms.size());
      if (fieldTerms != null) {
        TermsEnum termsEnum = fieldTerms.iterator();
        TermContext[] termContexts = synonymWeight.getTermContexts();
        int i = 0;
        for (Term term : terms) {
          TermContext termContext = termContexts[i++]; // in term order
          TermState termState = termContext.get(context.ord);
          if (termState != null) {
            termsEnum.seekExact(term.bytes(), termState);
            PostingsEnum postings = termsEnum.postings(null, PostingsEnum.POSITIONS);
            float positionsCost = SpanTermQuery.termPositionsCost(termsEnum)
                                * SpanTermQuery.PHRASE_TO_SPAN_TERM_POSITIONS_COST;
            termSpans.add(new TermSpans(simScorer, postings, term, positionsCost));
          }
        }
      }

      return  (termSpans.size() == 0) ? null
            : (termSpans.size() == 1) ? termSpans.get(0)
            : new SynonymSpans(SpanSynonymQuery.this, termSpans, simScorer);
    }
  }
}
