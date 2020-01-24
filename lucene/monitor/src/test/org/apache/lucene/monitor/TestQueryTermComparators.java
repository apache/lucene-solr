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

    QueryTree node1 = QueryTree.term("f", new BytesRef("foo"), 1.0);
    QueryTree node2 = QueryTree.anyTerm("*:*");

    QueryTree conjunction = QueryTree.conjunction(node1, node2);
    Set<Term> terms = new HashSet<>();
    conjunction.collectTerms((f, b) -> terms.add(new Term(f, b)));
    Set<Term> expected = Collections.singleton(new Term("f", "foo"));
    assertEquals(expected, terms);
  }

  public void testHigherWeightsArePreferred() {

    QueryTree node1 = QueryTree.term(new Term("f", "foo"), 1);
    QueryTree node2 = QueryTree.term(new Term("f", "foobar"), 1.5);

    QueryTree conjunction = QueryTree.conjunction(node1, node2);
    Set<Term> terms = new HashSet<>();
    conjunction.collectTerms((f, b) -> terms.add(new Term(f, b)));
    Set<Term> expected = Collections.singleton(new Term("f", "foobar"));
    assertEquals(expected, terms);
  }

  public void testShorterTermListsArePreferred() {

    Term term = new Term("f", "foobar");

    QueryTree node1 = QueryTree.term(term, 1);
    QueryTree node2 = QueryTree.disjunction(
        QueryTree.term(term, 1),
        QueryTree.term(term, 1));

    QueryTree conjunction = QueryTree.conjunction(node1, node2);
    Set<Term> terms = new HashSet<>();
    conjunction.collectTerms((f, b) -> terms.add(new Term(f, b)));
    assertEquals(1, terms.size());

  }

  public void testFieldWeights() {
    TermWeightor weightor = TermWeightor.fieldWeightor(1.5, "g");
    assertEquals(1, weightor.applyAsDouble(new Term("f", "foo")), 0);
    assertEquals(1.5f, weightor.applyAsDouble(new Term("g", "foo")), 0);
  }

  public void testTermWeights() {
    TermWeightor weight = TermWeightor.termWeightor(0.01f, new BytesRef("START"));
    assertEquals(0.01f, weight.applyAsDouble(new Term("f", "START")), 0);
  }

  public void testTermFrequencyNorms() {

    Map<String, Integer> termfreqs = new HashMap<>();
    termfreqs.put("france", 31635);
    termfreqs.put("s", 47088);
    TermWeightor weight = TermWeightor.termFreqWeightor(termfreqs, 100, 0.8);

    assertTrue(weight.applyAsDouble(new Term("f", "france")) >
        weight.applyAsDouble(new Term("f", "s")));

  }

  public void testFieldSpecificTermWeightNorms() {
    TermWeightor weight = TermWeightor.termAndFieldWeightor(0.1,
        new Term("field1", "f"),
        new Term("field1", "g"));
    assertEquals(0.1, weight.applyAsDouble(new Term("field1", "f")), 0);
    assertEquals(1, weight.applyAsDouble(new Term("field2", "f")), 0);
  }

}
