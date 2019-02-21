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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class TestQueryVisitor extends LuceneTestCase {

  private static final Query query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.MUST)
      .add(new BooleanQuery.Builder()
          .add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.SHOULD)
          .add(new BoostQuery(new TermQuery(new Term("field1", "term3")), 2), BooleanClause.Occur.SHOULD)
          .build(), BooleanClause.Occur.MUST)
      .add(new BoostQuery(new PhraseQuery.Builder()
          .add(new Term("field1", "term4"))
          .add(new Term("field1", "term5"))
          .build(), 3), BooleanClause.Occur.MUST)
      .add(new SpanNearQuery(new SpanQuery[]{
          new SpanTermQuery(new Term("field1", "term6")),
          new SpanTermQuery(new Term("field1", "term7"))
      }, 2, true), BooleanClause.Occur.MUST)
      .add(new TermQuery(new Term("field1", "term8")), BooleanClause.Occur.MUST_NOT)
      .add(new PrefixQuery(new Term("field1", "term9")), BooleanClause.Occur.SHOULD)
      .build();

  public void testExtractTermsEquivalent() {
    Set<Term> terms = new HashSet<>();
    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field1", "term1"), new Term("field1", "term2"),
        new Term("field1", "term3"), new Term("field1", "term4"),
        new Term("field1", "term5"), new Term("field1", "term6"),
        new Term("field1", "term7")
    ));
    query.visit(terms::add);
    assertThat(terms, equalTo(expected));
  }

  public void extractAllTerms() {
    Set<Term> terms = new HashSet<>();
    QueryVisitor visitor = new QueryVisitor() {
      @Override
      public void matchesTerm(Term term) {
        terms.add(term);
      }
      @Override
      public QueryVisitor getNonMatchingVisitor(Query parent) {
        return this;
      }
    };
    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field1", "term1"), new Term("field1", "term2"),
        new Term("field1", "term3"), new Term("field1", "term4"),
        new Term("field1", "term5"), new Term("field1", "term6"),
        new Term("field1", "term7"), new Term("field1", "term8")
    ));
    query.visit(visitor);
    assertThat(terms, equalTo(expected));
  }

  static class BoostedTermExtractor implements QueryVisitor {

    final float boost;
    final Map<Term, Float> termsToBoosts;

    BoostedTermExtractor(float boost, Map<Term, Float> termsToBoosts) {
      this.boost = boost;
      this.termsToBoosts = termsToBoosts;
    }

    @Override
    public void matchesTerm(Term term) {
      termsToBoosts.put(term, boost);
    }

    @Override
    public QueryVisitor getMatchingVisitor(Query parent) {
      if (parent instanceof BoostQuery) {
        return new BoostedTermExtractor(boost * ((BoostQuery)parent).getBoost(), termsToBoosts);
      }
      return this;
    }
  }

  public void testExtractTermsAndBoosts() {
    Map<Term, Float> termsToBoosts = new HashMap<>();
    query.visit(new BoostedTermExtractor(1, termsToBoosts));
    Map<Term, Float> expected = new HashMap<>();
    expected.put(new Term("field1", "term1"), 1f);
    expected.put(new Term("field1", "term2"), 1f);
    expected.put(new Term("field1", "term3"), 2f);
    expected.put(new Term("field1", "term4"), 3f);
    expected.put(new Term("field1", "term5"), 3f);
    expected.put(new Term("field1", "term6"), 1f);
    expected.put(new Term("field1", "term7"), 1f);
    assertThat(termsToBoosts, equalTo(expected));
  }

  static class MinimumMatchingTermSetExtractor implements QueryVisitor {

    List<MinimumMatchingTermSetExtractor> mustMatchLeaves = new ArrayList<>();
    List<MinimumMatchingTermSetExtractor> shouldMatchLeaves = new ArrayList<>();
    Term term;
    int weight;

    @Override
    public void matchesTerm(Term term) {
      this.term = term;
      this.weight = term.text().length();
    }

    @Override
    public void visitLeaf(Query query) {
      this.term = new Term("", "ANY");
      this.weight = 100;
    }

    @Override
    public QueryVisitor getMatchingVisitor(Query parent) {
      MinimumMatchingTermSetExtractor child = new MinimumMatchingTermSetExtractor();
      mustMatchLeaves.add(child);
      return child;
    }

    @Override
    public QueryVisitor getFilteringVisitor(Query parent) {
      return getMatchingVisitor(parent);
    }

    @Override
    public QueryVisitor getShouldMatchVisitor(Query parent) {
      MinimumMatchingTermSetExtractor child = new MinimumMatchingTermSetExtractor();
      shouldMatchLeaves.add(child);
      return child;
    }

    int getWeight() {
      if (mustMatchLeaves.size() > 0) {
        mustMatchLeaves.sort(Comparator.comparingInt(MinimumMatchingTermSetExtractor::getWeight));
        return mustMatchLeaves.get(0).getWeight();
      }
      if (shouldMatchLeaves.size() > 0) {
        shouldMatchLeaves.sort(Comparator.comparingInt(MinimumMatchingTermSetExtractor::getWeight).reversed());
        return shouldMatchLeaves.get(0).getWeight();
      }
      return weight;
    }

    void getMatchesTermSet(Set<Term> terms) {
      if (mustMatchLeaves.size() > 0) {
        mustMatchLeaves.sort(Comparator.comparingInt(MinimumMatchingTermSetExtractor::getWeight));
        mustMatchLeaves.get(0).getMatchesTermSet(terms);
        return;
      }
      if (shouldMatchLeaves.size() > 0) {
        for (MinimumMatchingTermSetExtractor child : shouldMatchLeaves) {
          child.getMatchesTermSet(terms);
        }
        return;
      }
      terms.add(term);
    }

    boolean nextMatchingSet() {
      if (mustMatchLeaves.size() > 0) {
        if (mustMatchLeaves.get(0).nextMatchingSet()) {
          return true;
        }
        mustMatchLeaves.remove(0);
        return shouldMatchLeaves.size() > 0;
      }
      if (shouldMatchLeaves.size() == 0) {
        return false;
      }
      boolean advanced = false;
      for (MinimumMatchingTermSetExtractor child : shouldMatchLeaves) {
        advanced |= child.nextMatchingSet();
      }
      return advanced;
    }
  }

  public void testExtractMatchingTermSet() {
    MinimumMatchingTermSetExtractor extractor = new MinimumMatchingTermSetExtractor();
    query.visit(extractor);
    Set<Term> minimumTermSet = new HashSet<>();
    extractor.getMatchesTermSet(minimumTermSet);

    Set<Term> expected1 = new HashSet<>(Collections.singletonList(new Term("field1", "term1")));
    assertThat(minimumTermSet, equalTo(expected1));
    assertTrue(extractor.nextMatchingSet());
    Set<Term> expected2 = new HashSet<>(Arrays.asList(new Term("field1", "term2"), new Term("field1", "term3")));
    minimumTermSet.clear();
    extractor.getMatchesTermSet(minimumTermSet);
    assertThat(minimumTermSet, equalTo(expected2));
  }

}
