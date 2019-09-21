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

package org.apache.lucene.monitor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryAnalyzer extends LuceneTestCase {

  public static final QueryAnalyzer analyzer = new QueryAnalyzer();

  private Set<Term> collectTerms(QueryTree tree) {
    Set<Term> terms = new HashSet<>();
    tree.collectTerms((f, b) -> terms.add(new Term(f, b)));
    return terms;
  }

  private Set<Term> collectTerms(Query query) {
    return collectTerms(analyzer.buildTree(query, TermWeightor.DEFAULT));
  }

  public void testAdvancesCollectDifferentTerms() {

    Query q = MonitorTestBase.parse("field:(+hello +goodbye)");
    QueryTree querytree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected = Collections.singleton(new Term("field", "goodbye"));
    assertEquals(expected, collectTerms(querytree));

    assertTrue(querytree.advancePhase(0));
    expected = Collections.singleton(new Term("field", "hello"));
    assertEquals(expected, collectTerms(querytree));

    assertFalse(querytree.advancePhase(0));

    assertEquals(expected, collectTerms(querytree));

  }

  public void testDisjunctionsWithAnyClausesOnlyReturnANYTOKEN() {

    // disjunction containing a pure negative - we can't narrow this down
    Query q = MonitorTestBase.parse("hello goodbye (*:* -term)");

    Set<Term> terms = collectTerms(q);
    assertEquals(1, terms.size());
    assertEquals(TermFilteredPresearcher.ANYTOKEN_FIELD, terms.iterator().next().field());

  }

  public void testConjunctionsDoNotAdvanceOverANYTOKENs() {

    Query q = MonitorTestBase.parse("+hello +howdyedo +(goodbye (*:* -whatever))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected = Collections.singleton(new Term("field", "howdyedo"));
    assertEquals(expected, collectTerms(tree));

    assertTrue(tree.advancePhase(0));
    expected = Collections.singleton(new Term("field", "hello"));
    assertEquals(expected, collectTerms(tree));

    assertFalse(tree.advancePhase(0));
    assertEquals(expected, collectTerms(tree));

  }

  public void testConjunctionsCannotAdvanceOverMinWeightedTokens() {

    TermWeightor weightor = TermWeightor.combine(
        TermWeightor.termWeightor(0.1, new BytesRef("startterm")),
        TermWeightor.lengthWeightor(1, 1));

    QueryAnalyzer analyzer = new QueryAnalyzer();

    Query q = MonitorTestBase.parse("+startterm +hello +goodbye");
    QueryTree tree = analyzer.buildTree(q, weightor);

    Set<Term> expected = Collections.singleton(new Term("field", "goodbye"));
    assertEquals(expected, collectTerms(tree));

    assertTrue(tree.advancePhase(0.5));
    expected = Collections.singleton(new Term("field", "hello"));
    assertEquals(expected, collectTerms(tree));

    assertFalse(tree.advancePhase(0.5));

  }

  public void testNestedConjunctions() {

    Query q = MonitorTestBase.parse("+(+(+(+aaaa +cc) +(+d +bbb)))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected = Collections.singleton(new Term("field", "aaaa"));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new Term("field", "bbb"));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new Term("field", "cc"));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new Term("field", "d"));
    assertEquals(expected, collectTerms(tree));
    assertFalse(tree.advancePhase(0));

  }

  public void testNestedDisjunctions() {

    Query q = MonitorTestBase.parse("+(+((+aaaa +cc) (+dd +bbb +f)))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field", "aaaa"),
        new Term("field", "bbb"
    )));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = new HashSet<>(Arrays.asList(
        new Term("field", "cc"),
        new Term("field", "dd")
    ));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = new HashSet<>(Arrays.asList(
        new Term("field", "cc"),
        new Term("field", "f")
    ));
    assertEquals(expected, collectTerms(tree));
    assertFalse(tree.advancePhase(0));
  }

  public void testMinWeightAdvances() {
    QueryTree tree = QueryTree.disjunction(
        QueryTree.conjunction(
            QueryTree.term(new Term("field", "term1"), 1),
            QueryTree.term(new Term("field", "term2"), 0.1),
            QueryTree.anyTerm("*:*")
        ),
        QueryTree.conjunction(
            QueryTree.disjunction(
                QueryTree.term(new Term("field", "term4"), 0.2),
                QueryTree.term(new Term("field", "term5"), 1)
            ),
            QueryTree.term(new Term("field", "term3"), 0.5)
        )
    );

    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field", "term1"),
        new Term("field", "term3")
    ));
    assertEquals(expected, collectTerms(tree));
    assertTrue(tree.advancePhase(0.1f));

    expected = new HashSet<>(Arrays.asList(
        new Term("field", "term1"),
        new Term("field", "term4"),
        new Term("field", "term5")
    ));
    assertEquals(expected, collectTerms(tree));
    assertFalse(tree.advancePhase(0.1f));
  }

}
