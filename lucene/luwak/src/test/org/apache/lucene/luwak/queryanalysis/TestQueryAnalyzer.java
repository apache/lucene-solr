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

package org.apache.lucene.luwak.queryanalysis;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.luwak.testutils.ParserUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryAnalyzer extends LuceneTestCase {

  public static final QueryAnalyzer analyzer = new QueryAnalyzer();

  public void testAdvancesCollectDifferentTerms() throws Exception {

    Query q = ParserUtils.parse("field:(+hello +goodbye)");
    QueryTree querytree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(querytree));

    assertTrue(querytree.advancePhase(0));
    expected = Collections.singleton(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(querytree));

    assertFalse(querytree.advancePhase(0));

    assertEquals(expected, analyzer.collectTerms(querytree));

  }

  public void testDisjunctionsWithAnyClausesOnlyReturnANYTOKEN() throws Exception {

    // disjunction containing a pure negative - we can't narrow this down
    Query q = ParserUtils.parse("hello goodbye (*:* -term)");

    Set<QueryTerm> terms = analyzer.collectTerms(q, TermWeightor.DEFAULT);
    assertEquals(1, terms.size());
    assertEquals(QueryTerm.Type.ANY, terms.iterator().next().type);

  }

  public void testConjunctionsDoNotAdvanceOverANYTOKENs() throws Exception {

    Query q = ParserUtils.parse("+hello +howdyedo +(goodbye (*:* -whatever))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field", "howdyedo", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));

    assertTrue(tree.advancePhase(0));
    expected = Collections.singleton(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));

    assertFalse(tree.advancePhase(0));
    assertEquals(expected, analyzer.collectTerms(tree));

  }

  public void testConjunctionsCannotAdvanceOverZeroWeightedTokens() throws Exception {

    TermWeightor weightor = TermWeightor.combine(
        TermWeightor.termWeightor(0, new BytesRef("startterm")),
        TermWeightor.lengthWeightor(1, 1));

    QueryAnalyzer analyzer = new QueryAnalyzer();

    Query q = ParserUtils.parse("+startterm +hello +goodbye");
    QueryTree tree = analyzer.buildTree(q, weightor);

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field", "goodbye", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));

    assertTrue(tree.advancePhase(0));
    expected = Collections.singleton(new QueryTerm("field", "hello", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));

    assertFalse(tree.advancePhase(0));

  }

  public void testNestedConjunctions() throws Exception {

    Query q = ParserUtils.parse("+(+(+(+aaaa +cc) +(+d +bbb)))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new QueryTerm("field", "bbb", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new QueryTerm("field", "cc", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = Collections.singleton(new QueryTerm("field", "d", QueryTerm.Type.EXACT));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertFalse(tree.advancePhase(0));

  }

  public void testNestedDisjunctions() throws Exception {

    Query q = ParserUtils.parse("+(+((+aaaa +cc) (+dd +bbb +f)))");
    QueryTree tree = analyzer.buildTree(q, TermWeightor.DEFAULT);

    Set<QueryTerm> expected = new HashSet<>(Arrays.asList(
        new QueryTerm("field", "aaaa", QueryTerm.Type.EXACT),
        new QueryTerm("field", "bbb", QueryTerm.Type.EXACT)
    ));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = new HashSet<>(Arrays.asList(
        new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
        new QueryTerm("field", "dd", QueryTerm.Type.EXACT)
    ));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertTrue(tree.advancePhase(0));

    expected = new HashSet<>(Arrays.asList(
        new QueryTerm("field", "cc", QueryTerm.Type.EXACT),
        new QueryTerm("field", "f", QueryTerm.Type.EXACT)
    ));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertFalse(tree.advancePhase(0));
  }

  public void testMinWeightAdvances() {
    QueryTree tree = QueryTree.disjunction(
        QueryTree.conjunction(
            QueryTree.term(new QueryTerm("field", "term1", QueryTerm.Type.EXACT), 1),
            QueryTree.term(new QueryTerm("field", "term2", QueryTerm.Type.EXACT), 0.1),
            QueryTree.anyTerm("*:*")
        ),
        QueryTree.conjunction(
            QueryTree.disjunction(
                QueryTree.term(new QueryTerm("field", "term4", QueryTerm.Type.EXACT), 0.2),
                QueryTree.term(new QueryTerm("field", "term5", QueryTerm.Type.EXACT), 1)
            ),
            QueryTree.term(new QueryTerm("field", "term3", QueryTerm.Type.EXACT), 0.5)
        )
    );

    Set<QueryTerm> expected = new HashSet<>(Arrays.asList(
        new QueryTerm("field", "term1", QueryTerm.Type.EXACT),
        new QueryTerm("field", "term3", QueryTerm.Type.EXACT)
    ));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertTrue(tree.advancePhase(0.1f));

    expected = new HashSet<>(Arrays.asList(
        new QueryTerm("field", "term1", QueryTerm.Type.EXACT),
        new QueryTerm("field", "term4", QueryTerm.Type.EXACT),
        new QueryTerm("field", "term5", QueryTerm.Type.EXACT)
    ));
    assertEquals(expected, analyzer.collectTerms(tree));
    assertFalse(tree.advancePhase(0.1f));
  }

}
