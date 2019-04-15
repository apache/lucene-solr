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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryTermComparators extends LuceneTestCase {

  public void testAnyTokensAreNotPreferred() {

    QueryTree node1 = QueryTree.term(new QueryTerm("f", "foo", QueryTerm.Type.EXACT), 1.0);
    QueryTree node2 = QueryTree.anyTerm("*:*");

    QueryTree conjunction = QueryTree.conjunction(node1, node2);
    Set<QueryTerm> terms = new HashSet<>();
    conjunction.collectTerms(terms);
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
    assertEquals(expected, terms);
  }

  public void testHigherWeightsArePreferred() {

    QueryTree node1 = QueryTree.term(new QueryTerm("f", "foo", QueryTerm.Type.EXACT), 1);
    QueryTree node2 = QueryTree.term(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT), 1.5);

    QueryTree conjunction = QueryTree.conjunction(node1, node2);
    Set<QueryTerm> terms = new HashSet<>();
    conjunction.collectTerms(terms);
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));
    assertEquals(expected, terms);
  }

  public void testShorterTermListsArePreferred() {

    Term term = new Term("f", "foobar");

    QueryTree node1 = QueryTree.term(new QueryTerm(term), 1);
    QueryTree node2 = QueryTree.disjunction(
        QueryTree.term(new QueryTerm(term), 1),
        QueryTree.term(new QueryTerm(term), 1));

    QueryTree conjunction = QueryTree.conjunction(node1, node2);
    Set<QueryTerm> terms = new HashSet<>();
    conjunction.collectTerms(terms);
    assertEquals(1, terms.size());

  }

  public void testFieldWeights() {
    TermWeightor weightor = TermWeightor.fieldWeightor(1.5, "g");
    assertEquals(1, weightor.applyAsDouble(new QueryTerm("f", "foo", QueryTerm.Type.EXACT)), 0);
    assertEquals(1.5f, weightor.applyAsDouble(new QueryTerm("g", "foo", QueryTerm.Type.EXACT)), 0);
  }

  public void testTermWeights() {
    TermWeightor weight = TermWeightor.termWeightor(0.01f, new BytesRef("START"));
    assertEquals(0.01f, weight.applyAsDouble(new QueryTerm("f", "START", QueryTerm.Type.EXACT)), 0);
  }

  public void testTermFrequencyNorms() {

    Map<String, Integer> termfreqs = new HashMap<>();
    termfreqs.put("france", 31635);
    termfreqs.put("s", 47088);
    TermWeightor weight = TermWeightor.termFreqWeightor(termfreqs, 100, 0.8);

    assertTrue(weight.applyAsDouble(new QueryTerm("f", "france", QueryTerm.Type.EXACT)) >
        weight.applyAsDouble(new QueryTerm("f", "s", QueryTerm.Type.EXACT)));

  }

  public void testFieldSpecificTermWeightNorms() {
    TermWeightor weight = TermWeightor.termAndFieldWeightor(0.1,
        new Term("field1", "f"),
        new Term("field1", "g"));
    assertEquals(0.1, weight.applyAsDouble(new QueryTerm("field1", "f", QueryTerm.Type.EXACT)), 0);
    assertEquals(1, weight.applyAsDouble(new QueryTerm("field2", "f", QueryTerm.Type.EXACT)), 0);
  }

  public void testTermTypeWeightNorms() {

    TermWeightor weight = TermWeightor.combine(
        TermWeightor.typeWeightor(0.2, QueryTerm.Type.CUSTOM),
        TermWeightor.typeWeightor(0.1, QueryTerm.Type.CUSTOM, "wildcard")
    );

    assertEquals(0.2 * 0.1, weight.applyAsDouble(new QueryTerm("field", "fooXX", QueryTerm.Type.CUSTOM, "wildcard")), 0);
    assertEquals(1, weight.applyAsDouble(new QueryTerm("field", "foo", QueryTerm.Type.EXACT)), 0);
    assertEquals(0.2, weight.applyAsDouble(new QueryTerm("field", "foo", QueryTerm.Type.CUSTOM)), 0);

  }

}
