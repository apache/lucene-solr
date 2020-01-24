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
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;

public class TestExtractors extends LuceneTestCase {

  private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();

  private Set<Term> collectTerms(Query query) {
    Set<Term> terms = new HashSet<>();
    QueryTree tree = treeBuilder.buildTree(query, TermWeightor.DEFAULT);
    tree.collectTerms((f, b) -> terms.add(new Term(f, b)));
    return terms;
  }

  public void testConstantScoreQueryExtractor() {

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

    Query csqWithQuery = new ConstantScoreQuery(bq.build());
    Set<Term> expected = Collections.singleton(new Term("f", "q1"));
    assertEquals(expected, collectTerms(csqWithQuery));

  }

  public void testPhraseQueryExtractor() {

    PhraseQuery.Builder pq = new PhraseQuery.Builder();
    pq.add(new Term("f", "hello"));
    pq.add(new Term("f", "encyclopedia"));

    Set<Term> expected = Collections.singleton(new Term("f", "encyclopedia"));
    assertEquals(expected, collectTerms(pq.build()));

  }

  public void testBoostQueryExtractor() {

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

    Query boostQuery = new BoostQuery(bq.build(), 0.5f);
    Set<Term> expected = Collections.singleton(new Term("f", "q1"));
    assertEquals(expected, collectTerms(boostQuery));
  }

  public void testDisjunctionMaxExtractor() {

    Query query = new DisjunctionMaxQuery(
        Arrays.asList(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))), 0.1f
    );
    Set<Term> expected = new HashSet<>(Arrays.asList(
        new Term("f", "t1"),
        new Term("f", "t2")
    ));
    assertEquals(expected, collectTerms(query));
  }

  public void testBooleanExtractsFilter() {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("f", "must")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term("f", "filter")), BooleanClause.Occur.FILTER)
        .build();
    Set<Term> expected = Collections.singleton(new Term("f", "filter")); // it's longer, so it wins
    assertEquals(expected, collectTerms(q));
  }


}
