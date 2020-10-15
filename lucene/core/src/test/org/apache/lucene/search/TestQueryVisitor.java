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
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class TestQueryVisitor extends LuceneTestCase {

  private static final Query query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("field1", "t1")), BooleanClause.Occur.MUST)
      .add(new BooleanQuery.Builder()
          .add(new TermQuery(new Term("field1", "tm2")), BooleanClause.Occur.SHOULD)
          .add(new BoostQuery(new TermQuery(new Term("field1", "tm3")), 2), BooleanClause.Occur.SHOULD)
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
      .add(new BoostQuery(new BooleanQuery.Builder()
          .add(new BoostQuery(new TermQuery(new Term("field2", "term10")), 3), BooleanClause.Occur.MUST)
          .build(), 2), BooleanClause.Occur.SHOULD)
      .build();

  public void testExtractTermsEquivalent() {
    Set<Term> terms = new HashSet<>();
    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field1", "t1"), new Term("field1", "tm2"),
        new Term("field1", "tm3"), new Term("field1", "term4"),
        new Term("field1", "term5"), new Term("field1", "term6"),
        new Term("field1", "term7"), new Term("field2", "term10")
    ));
    query.visit(QueryVisitor.termCollector(terms));
    assertThat(terms, equalTo(expected));
  }

  public void extractAllTerms() {
    Set<Term> terms = new HashSet<>();
    QueryVisitor visitor = new QueryVisitor() {
      @Override
      public void consumeTerms(Query query, Term... ts) {
        terms.addAll(Arrays.asList(ts));
      }
      @Override
      public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        return this;
      }
    };
    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field1", "t1"), new Term("field1", "tm2"),
        new Term("field1", "tm3"), new Term("field1", "term4"),
        new Term("field1", "term5"), new Term("field1", "term6"),
        new Term("field1", "term7"), new Term("field1", "term8"),
        new Term("field2", "term10")
    ));
    query.visit(visitor);
    assertThat(terms, equalTo(expected));
  }

  public void extractTermsFromField() {
    final Set<Term> actual = new HashSet<>();
    Set<Term> expected = new HashSet<>(Arrays.asList(new Term("field2", "term10")));
    query.visit(new QueryVisitor(){
      @Override
      public boolean acceptField(String field) {
        return "field2".equals(field);
      }
      @Override
      public void consumeTerms(Query query, Term... terms) {
        actual.addAll(Arrays.asList(terms));
      }
    });
    assertThat(actual, equalTo(expected));
  }

  static class BoostedTermExtractor extends QueryVisitor {

    final float boost;
    final Map<Term, Float> termsToBoosts;

    BoostedTermExtractor(float boost, Map<Term, Float> termsToBoosts) {
      this.boost = boost;
      this.termsToBoosts = termsToBoosts;
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
      for (Term term : terms) {
        termsToBoosts.put(term, boost);
      }
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (parent instanceof BoostQuery) {
        return new BoostedTermExtractor(boost * ((BoostQuery)parent).getBoost(), termsToBoosts);
      }
      return super.getSubVisitor(occur, parent);
    }
  }

  public void testExtractTermsAndBoosts() {
    Map<Term, Float> termsToBoosts = new HashMap<>();
    query.visit(new BoostedTermExtractor(1, termsToBoosts));
    Map<Term, Float> expected = new HashMap<>();
    expected.put(new Term("field1", "t1"), 1f);
    expected.put(new Term("field1", "tm2"), 1f);
    expected.put(new Term("field1", "tm3"), 2f);
    expected.put(new Term("field1", "term4"), 3f);
    expected.put(new Term("field1", "term5"), 3f);
    expected.put(new Term("field1", "term6"), 1f);
    expected.put(new Term("field1", "term7"), 1f);
    expected.put(new Term("field2", "term10"), 6f);
    assertThat(termsToBoosts, equalTo(expected));
  }

  public void testLeafQueryTypeCounts() {
    Map<Class<? extends Query>, Integer> queryCounts = new HashMap<>();
    query.visit(new QueryVisitor() {

      private void countQuery(Query q) {
        queryCounts.compute(q.getClass(), (query, i) -> {
          if (i == null) {
            return 1;
          }
          return i + 1;
        });
      }

      @Override
      public void consumeTerms(Query query, Term... terms) {
        countQuery(query);
      }

      @Override
      public void visitLeaf(Query query) {
        countQuery(query);
      }

    });
    assertEquals(4, queryCounts.get(TermQuery.class).intValue());
    assertEquals(1, queryCounts.get(PhraseQuery.class).intValue());
  }

  static abstract class QueryNode extends QueryVisitor {

    final List<QueryNode> children = new ArrayList<>();

    abstract int getWeight();
    abstract void collectTerms(Set<Term> terms);
    abstract boolean nextTermSet();

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER) {
        QueryNode n = new ConjunctionNode();
        children.add(n);
        return n;
      }
      if (occur == BooleanClause.Occur.MUST_NOT) {
        return QueryVisitor.EMPTY_VISITOR;
      }
      if (parent instanceof BooleanQuery) {
        BooleanQuery bq = (BooleanQuery) parent;
        if (bq.getClauses(BooleanClause.Occur.MUST).size() > 0 || bq.getClauses(BooleanClause.Occur.FILTER).size() > 0) {
          return QueryVisitor.EMPTY_VISITOR;
        }
      }
      DisjunctionNode n = new DisjunctionNode();
      children.add(n);
      return n;
    }
  }

  static class TermNode extends QueryNode {

    final Term term;

    TermNode(Term term) {
      this.term = term;
    }

    @Override
    int getWeight() {
      return term.text().length();
    }

    @Override
    void collectTerms(Set<Term> terms) {
      terms.add(term);
    }

    @Override
    boolean nextTermSet() {
      return false;
    }

    @Override
    public String toString() {
      return "TERM(" + term.toString() + ")";
    }
  }

  static class ConjunctionNode extends QueryNode {

    @Override
    int getWeight() {
      children.sort(Comparator.comparingInt(QueryNode::getWeight));
      return children.get(0).getWeight();
    }

    @Override
    void collectTerms(Set<Term> terms) {
      children.sort(Comparator.comparingInt(QueryNode::getWeight));
      children.get(0).collectTerms(terms);
    }

    @Override
    boolean nextTermSet() {
      children.sort(Comparator.comparingInt(QueryNode::getWeight));
      if (children.get(0).nextTermSet()) {
        return true;
      }
      if (children.size() == 1) {
        return false;
      }
      children.remove(0);
      return true;
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
      for (Term term : terms) {
        children.add(new TermNode(term));
      }
    }

    @Override
    public String toString() {
      return children.stream().map(QueryNode::toString).collect(Collectors.joining(",", "AND(", ")"));
    }
  }

  static class DisjunctionNode extends QueryNode {

    @Override
    int getWeight() {
      children.sort(Comparator.comparingInt(QueryNode::getWeight).reversed());
      return children.get(0).getWeight();
    }

    @Override
    void collectTerms(Set<Term> terms) {
      for (QueryNode child : children) {
        child.collectTerms(terms);
      }
    }

    @Override
    boolean nextTermSet() {
      boolean next = false;
      for (QueryNode child : children) {
        next |= child.nextTermSet();
      }
      return next;
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
      for (Term term : terms) {
        children.add(new TermNode(term));
      }
    }

    @Override
    public String toString() {
      return children.stream().map(QueryNode::toString).collect(Collectors.joining(",", "OR(", ")"));
    }
  }

  public void testExtractMatchingTermSet() {
    QueryNode extractor = new ConjunctionNode();
    query.visit(extractor);
    Set<Term> minimumTermSet = new HashSet<>();
    extractor.collectTerms(minimumTermSet);

    Set<Term> expected1 = new HashSet<>(Collections.singletonList(new Term("field1", "t1")));
    assertThat(minimumTermSet, equalTo(expected1));
    assertTrue(extractor.nextTermSet());
    Set<Term> expected2 = new HashSet<>(Arrays.asList(new Term("field1", "tm2"), new Term("field1", "tm3")));
    minimumTermSet.clear();
    extractor.collectTerms(minimumTermSet);
    assertThat(minimumTermSet, equalTo(expected2));

    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f", "1")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("f", "61")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("f", "211")), BooleanClause.Occur.FILTER)
            .add(new TermQuery(new Term("f", "5")), BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.SHOULD)
        .add(new PhraseQuery("f", "3333", "44444"), BooleanClause.Occur.SHOULD)
        .build();
    QueryNode ex2 = new ConjunctionNode();
    bq.visit(ex2);
    Set<Term> expected3 = new HashSet<>(Arrays.asList(new Term("f", "1"), new Term("f", "3333")));
    minimumTermSet.clear();
    ex2.collectTerms(minimumTermSet);
    assertThat(minimumTermSet, equalTo(expected3));
    ex2.getWeight(); // force sort order
    assertThat(ex2.toString(), equalTo("AND(AND(OR(AND(TERM(f:3333),TERM(f:44444)),AND(TERM(f:1),TERM(f:61),AND(TERM(f:211))))))"));
  }

}
