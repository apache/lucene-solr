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

package org.apache.lucene.luwak.termextractor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.termextractor.querytree.AnyNode;
import org.apache.lucene.luwak.termextractor.querytree.ConjunctionNode;
import org.apache.lucene.luwak.termextractor.querytree.DisjunctionNode;
import org.apache.lucene.luwak.termextractor.querytree.QueryTree;
import org.apache.lucene.luwak.termextractor.querytree.TermNode;
import org.apache.lucene.luwak.termextractor.weights.FieldSpecificTermWeightNorm;
import org.apache.lucene.luwak.termextractor.weights.FieldWeightNorm;
import org.apache.lucene.luwak.termextractor.weights.TermFrequencyWeightNorm;
import org.apache.lucene.luwak.termextractor.weights.TermTypeNorm;
import org.apache.lucene.luwak.termextractor.weights.TermWeightNorm;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryTermComparators extends LuceneTestCase {

  public void testAnyTokensAreNotPreferred() {

    QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.EXACT), 1.0);
    QueryTree node2 = new AnyNode("*:*");

    QueryTree conjunction = ConjunctionNode.build(node1, node2);
    Set<QueryTerm> terms = new HashSet<>();
    conjunction.collectTerms(terms);
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "foo", QueryTerm.Type.EXACT));
    assertEquals(expected, terms);
  }

  public void testHigherWeightsArePreferred() {

    QueryTree node1 = new TermNode(new QueryTerm("f", "foo", QueryTerm.Type.EXACT), 1);
    QueryTree node2 = new TermNode(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT), 1.5);

    QueryTree conjunction = ConjunctionNode.build(node1, node2);
    Set<QueryTerm> terms = new HashSet<>();
    conjunction.collectTerms(terms);
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "foobar", QueryTerm.Type.EXACT));
    assertEquals(expected, terms);
  }

  public void testShorterTermListsArePreferred() {

    Term term = new Term("f", "foobar");

    QueryTree node1 = new TermNode(new QueryTerm(term), 1);
    QueryTree node2 = DisjunctionNode.build(new TermNode(new QueryTerm(term), 1),
        new TermNode(new QueryTerm(term), 1));

    QueryTree conjunction = ConjunctionNode.build(node1, node2);
    Set<QueryTerm> terms = new HashSet<>();
    conjunction.collectTerms(terms);
    assertEquals(1, terms.size());

  }

  public void testFieldWeights() {
    TermWeightor weightor = new TermWeightor(new FieldWeightNorm(1.5f, "g"));
    assertEquals(1, weightor.weigh(new QueryTerm("f", "foo", QueryTerm.Type.EXACT)), 0);
    assertEquals(1.5f, weightor.weigh(new QueryTerm("g", "foo", QueryTerm.Type.EXACT)), 0);
  }

  public void testTermWeights() {
    TermWeightor weight = new TermWeightor(new TermWeightNorm(0.01f, Collections.singleton("START")));
    assertEquals(0.01f, weight.weigh(new QueryTerm("f", "START", QueryTerm.Type.EXACT)), 0);
  }

  public void testTermFrequencyNorms() {

    Map<String, Integer> termfreqs = new HashMap<>();
    termfreqs.put("france", 31635);
    termfreqs.put("s", 47088);
    TermWeightor weight = new TermWeightor(new TermFrequencyWeightNorm(termfreqs, 100, 0.8f));

    assertTrue(weight.weigh(new QueryTerm("f", "france", QueryTerm.Type.EXACT)) >
        weight.weigh(new QueryTerm("f", "s", QueryTerm.Type.EXACT)));

  }

  public void testFieldSpecificTermWeightNorms() {
    TermWeightor weight = new TermWeightor(new FieldSpecificTermWeightNorm(0.1f, "field1", "f", "g"));
    assertEquals(0.1f, weight.weigh(new QueryTerm("field1", "f", QueryTerm.Type.EXACT)), 0);
    assertEquals(1, weight.weigh(new QueryTerm("field2", "f", QueryTerm.Type.EXACT)), 0);
  }

  public void testTermTypeWeightNorms() {

    TermWeightor weight = new TermWeightor(new TermTypeNorm(QueryTerm.Type.CUSTOM, 0.2f),
        new TermTypeNorm(QueryTerm.Type.CUSTOM, "wildcard", 0.1f));

    assertEquals(0.1f, weight.weigh(new QueryTerm("field", "fooXX", QueryTerm.Type.CUSTOM, "wildcard")), 0);
    assertEquals(1, weight.weigh(new QueryTerm("field", "foo", QueryTerm.Type.EXACT)), 0);
    assertEquals(0.2f, weight.weigh(new QueryTerm("field", "foo", QueryTerm.Type.CUSTOM)), 0);

  }

}
