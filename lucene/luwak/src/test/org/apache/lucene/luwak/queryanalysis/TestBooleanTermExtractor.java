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

import org.apache.lucene.luwak.MonitorTestBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;

public class TestBooleanTermExtractor extends LuceneTestCase {

  private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();
  private static final TermWeightor WEIGHTOR = TermWeightor.DEFAULT;

  public void testAllDisjunctionQueriesAreIncluded() throws Exception {

    Query bq = MonitorTestBase.parse("field1:term1 field1:term2");
    Set<QueryTerm> terms = treeBuilder.collectTerms(bq, WEIGHTOR);
    Set<QueryTerm> expected = new HashSet<>(Arrays.asList(
        new QueryTerm("field1", "term1", QueryTerm.Type.EXACT),
        new QueryTerm("field1", "term2", QueryTerm.Type.EXACT)));
    assertEquals(expected, terms);

  }

  public void testAllNestedDisjunctionClausesAreIncluded() throws Exception {
    Query q = MonitorTestBase.parse("field1:term3 (field1:term1 field1:term2)");
    assertEquals(3, treeBuilder.collectTerms(q, WEIGHTOR).size());
  }

  public void testAllDisjunctionClausesOfAConjunctionAreExtracted() throws Exception {
    Query q = MonitorTestBase.parse("+(field1:term1 field1:term2) field1:term3");
    assertEquals(2, treeBuilder.collectTerms(q, WEIGHTOR).size());
  }

  public void testConjunctionsOutweighDisjunctions() throws Exception {
    Query bq = MonitorTestBase.parse("field1:term1 +field1:term2");
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(bq, WEIGHTOR));
  }

  public void testDisjunctionsWithPureNegativeClausesReturnANYTOKEN() throws Exception {
    Query q = MonitorTestBase.parse("+field1:term1 +(field2:term22 (-field2:notterm))");
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testDisjunctionsWithMatchAllNegativeClausesReturnANYTOKEN() throws Exception {
    Query q = MonitorTestBase.parse("+field1:term1 +(field2:term22 (*:* -field2:notterm))");
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field1", "term1", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(q, WEIGHTOR));
  }

  public void testMatchAllDocsIsOnlyQuery() throws Exception {
    // Set up - single MatchAllDocsQuery clause in a BooleanQuery
    Query q = MonitorTestBase.parse("+*:*");
    assertTrue(q instanceof BooleanQuery);
    BooleanClause clause = ((BooleanQuery)q).iterator().next();
    assertTrue(clause.getQuery() instanceof MatchAllDocsQuery);
    assertEquals(BooleanClause.Occur.MUST, clause.getOccur());

    Set<QueryTerm> terms = treeBuilder.collectTerms(q, WEIGHTOR);
    assertEquals(1, terms.size());
    QueryTerm t = terms.iterator().next();
    assertEquals(QueryTerm.Type.ANY, t.type);
  }

  public void testMatchAllDocsMustWithKeywordShould() throws Exception {
    Query q = MonitorTestBase.parse("+*:* field1:term1");
    // Because field1:term1 is optional, only the MatchAllDocsQuery is collected.
    Set<QueryTerm> terms = treeBuilder.collectTerms(q, WEIGHTOR);
    assertEquals(1, terms.size());
    QueryTerm t = terms.iterator().next();
    assertEquals(QueryTerm.Type.ANY, t.type);
  }

  public void testMatchAllDocsMustWithKeywordNot() throws Exception {
    Query q = MonitorTestBase.parse("+*:* -field1:notterm");

    // Because field1:notterm is negated, only the mandatory MatchAllDocsQuery is collected.
    Set<QueryTerm> terms = treeBuilder.collectTerms(q, WEIGHTOR);
    assertEquals(1, terms.size());
    QueryTerm t = terms.iterator().next();
    assertEquals(QueryTerm.Type.ANY, t.type);
  }

  public void testMatchAllDocsMustWithKeywordShouldAndKeywordNot() throws Exception {
    Query q = MonitorTestBase.parse("+*:* field1:term1 -field2:notterm");

    // Because field1:notterm is negated and field1:term1 is optional, only the mandatory MatchAllDocsQuery is collected.
    Set<QueryTerm> terms = treeBuilder.collectTerms(q, WEIGHTOR);
    assertEquals(1, terms.size());
    QueryTerm t = terms.iterator().next();
    assertEquals(QueryTerm.Type.ANY, t.type);
  }

  public void testMatchAllDocsMustAndOtherMustWithKeywordShouldAndKeywordNot() throws Exception {
    Query q = MonitorTestBase.parse("+*:* +field9:term9 field1:term1 -field2:notterm");

    // The queryterm collected by weight is the non-anynode, so field9:term9 shows up before MatchAllDocsQuery.
    Set<QueryTerm> terms = treeBuilder.collectTerms(q, WEIGHTOR);
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field9", "term9", QueryTerm.Type.EXACT));
    assertEquals(expected, terms);
  }

}
