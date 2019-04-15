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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.presearcher.WildcardNGramPresearcherComponent;
import org.apache.lucene.luwak.termextractor.weights.TermWeightor;
import org.apache.lucene.luwak.termextractor.weights.TokenLengthNorm;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;

public class TestExtractors extends LuceneTestCase {

  private static final QueryAnalyzer treeBuilder = new QueryAnalyzer();

  private static final TermWeightor WEIGHTOR = new TermWeightor(new TokenLengthNorm());

  public void testRegexpExtractor() {

    QueryAnalyzer builder = QueryAnalyzer.fromComponents(
        new WildcardNGramPresearcherComponent("XX", 30, "WILDCARD", null));

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("field", "califragilisticXX", QueryTerm.Type.CUSTOM, "WILDCARD"));
    assertEquals(expected, builder.collectTerms(new RegexpQuery(new Term("field", "super.*califragilistic")), WEIGHTOR));

    expected = Collections.singleton(new QueryTerm("field", "hellXX", QueryTerm.Type.CUSTOM, "WILDCARD"));
    assertEquals(expected, builder.collectTerms(new RegexpQuery(new Term("field", "hell.")), WEIGHTOR));

    expected = Collections.singleton(new QueryTerm("field", "heXX", QueryTerm.Type.CUSTOM, "WILDCARD"));
    assertEquals(expected, builder.collectTerms(new RegexpQuery(new Term("field", "hel?o")), WEIGHTOR));

  }

  public void testConstantScoreQueryExtractor() {

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

    Query csqWithQuery = new ConstantScoreQuery(bq.build());
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(csqWithQuery, WEIGHTOR));

  }

  public void testPhraseQueryExtractor() {

    PhraseQuery.Builder pq = new PhraseQuery.Builder();
    pq.add(new Term("f", "hello"));
    pq.add(new Term("f", "encyclopedia"));

    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "encyclopedia", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(pq.build(), WEIGHTOR));

  }

  public void testBoostQueryExtractor() {

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("f", "q1")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("f", "q2")), BooleanClause.Occur.SHOULD);

    Query boostQuery = new BoostQuery(bq.build(), 0.5f);
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "q1", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(boostQuery, WEIGHTOR));
  }

  public void testDisjunctionMaxExtractor() {

    Query query = new DisjunctionMaxQuery(
        Arrays.asList(new TermQuery(new Term("f", "t1")), new TermQuery(new Term("f", "t2"))), 0.1f
    );
    Set<QueryTerm> expected = new HashSet<>(Arrays.asList(
        new QueryTerm("f", "t1", QueryTerm.Type.EXACT),
        new QueryTerm("f", "t2", QueryTerm.Type.EXACT)
    ));
    assertEquals(expected, treeBuilder.collectTerms(query, WEIGHTOR));
  }

  public void testBooleanExtractsFilter() {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("f", "must")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term("f", "filter")), BooleanClause.Occur.FILTER)
        .build();
    Set<QueryTerm> expected = Collections.singleton(new QueryTerm("f", "filter", QueryTerm.Type.EXACT)); // it's longer, so it wins
    assertEquals(expected, treeBuilder.collectTerms(q, WEIGHTOR));
  }


}
