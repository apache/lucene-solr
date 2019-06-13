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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;

public class TestBooleanTermExtractor extends LuceneTestCase {

  private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();
  private static final TermWeightor WEIGHTOR = TermWeightor.DEFAULT;

  private Set<Term> collectTerms(Query query) {
    Set<Term> terms = new HashSet<>();
    QueryTree tree = treeBuilder.buildTree(query, TermWeightor.DEFAULT);
    tree.collectTerms((f, b) -> terms.add(new Term(f, b)));
    return terms;
  }

  public void testAllDisjunctionQueriesAreIncluded() {

    Query bq = MonitorTestBase.parse("field1:term1 field1:term2");
    Set<Term> terms = collectTerms(bq);
    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("field1", "term1"),
        new Term("field1", "term2")));
    assertEquals(expected, terms);

  }

  public void testAllNestedDisjunctionClausesAreIncluded() {
    Query q = MonitorTestBase.parse("field1:term3 (field1:term1 field1:term2)");
    assertEquals(3, collectTerms(q).size());
  }

  public void testAllDisjunctionClausesOfAConjunctionAreExtracted() {
    Query q = MonitorTestBase.parse("+(field1:term1 field1:term2) field1:term3");
    assertEquals(2, collectTerms(q).size());
  }

  public void testConjunctionsOutweighDisjunctions() {
    Query bq = MonitorTestBase.parse("field1:term1 +field1:term2");
    Set<Term> expected = Collections.singleton(new Term("field1", "term2"));
    assertEquals(expected, collectTerms(bq));
  }

  public void testDisjunctionsWithPureNegativeClausesReturnANYTOKEN() {
    Query q = MonitorTestBase.parse("+field1:term1 +(field2:term22 (-field2:notterm))");
    Set<Term> expected = Collections.singleton(new Term("field1", "term1"));
    assertEquals(expected, collectTerms(q));
  }

  public void testDisjunctionsWithMatchAllNegativeClausesReturnANYTOKEN() {
    Query q = MonitorTestBase.parse("+field1:term1 +(field2:term22 (*:* -field2:notterm))");
    Set<Term> expected = Collections.singleton(new Term("field1", "term1"));
    assertEquals(expected, collectTerms(q));
  }

  public void testMatchAllDocsIsOnlyQuery() {
    // Set up - single MatchAllDocsQuery clause in a BooleanQuery
    Query q = MonitorTestBase.parse("+*:*");
    assertTrue(q instanceof BooleanQuery);
    BooleanClause clause = ((BooleanQuery)q).iterator().next();
    assertTrue(clause.getQuery() instanceof MatchAllDocsQuery);
    assertEquals(BooleanClause.Occur.MUST, clause.getOccur());

    Set<Term> terms = collectTerms(q);
    assertEquals(1, terms.size());
    Term t = terms.iterator().next();
    assertEquals(TermFilteredPresearcher.ANYTOKEN_FIELD, t.field());
  }

  public void testMatchAllDocsMustWithKeywordShould() {
    Query q = MonitorTestBase.parse("+*:* field1:term1");
    // Because field1:term1 is optional, only the MatchAllDocsQuery is collected.
    Set<Term> terms = collectTerms(q);
    assertEquals(1, terms.size());
    Term t = terms.iterator().next();
    assertEquals(TermFilteredPresearcher.ANYTOKEN_FIELD, t.field());
  }

  public void testMatchAllDocsMustWithKeywordNot() throws Exception {
    Query q = MonitorTestBase.parse("+*:* -field1:notterm");

    // Because field1:notterm is negated, only the mandatory MatchAllDocsQuery is collected.
    Set<Term> terms = collectTerms(q);
    assertEquals(1, terms.size());
    Term t = terms.iterator().next();
    assertEquals(TermFilteredPresearcher.ANYTOKEN_FIELD, t.field());
  }

  public void testMatchAllDocsMustWithKeywordShouldAndKeywordNot() throws Exception {
    Query q = MonitorTestBase.parse("+*:* field1:term1 -field2:notterm");

    // Because field1:notterm is negated and field1:term1 is optional, only the mandatory MatchAllDocsQuery is collected.
    Set<Term> terms = collectTerms(q);
    assertEquals(1, terms.size());
    Term t = terms.iterator().next();
    assertEquals(TermFilteredPresearcher.ANYTOKEN_FIELD, t.field());
  }

  public void testMatchAllDocsMustAndOtherMustWithKeywordShouldAndKeywordNot() throws Exception {
    Query q = MonitorTestBase.parse("+*:* +field9:term9 field1:term1 -field2:notterm");

    // The queryterm collected by weight is the non-anynode, so field9:term9 shows up before MatchAllDocsQuery.
    Set<Term> terms = collectTerms(q);
    Set<Term> expected = Collections.singleton(new Term("field9", "term9"));
    assertEquals(expected, terms);
  }

}
