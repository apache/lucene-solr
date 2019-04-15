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
import java.util.Set;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;

public class TestBooleanClauseWeightings extends LuceneTestCase {

  private static QueryAnalyzer treeBuilder = new QueryAnalyzer();

  public void testExactClausesPreferred() {
    Query bq = new BooleanQuery.Builder()
        .add(LongPoint.newRangeQuery("field2", 1, 2), BooleanClause.Occur.MUST)
        .add(new BooleanQuery.Builder()
            .add(new TermQuery(new Term("field1", "term1")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("field1", "term2")), BooleanClause.Occur.SHOULD)
            .build(), BooleanClause.Occur.MUST)
        .build();
    assertEquals(2, treeBuilder.collectTerms(bq, TermWeightor.DEFAULT).size());
  }

  public void testLongerTermsPreferred() {
    Query q = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("field1", "a")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term("field1", "supercalifragilisticexpialidocious")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term("field1", "b")), BooleanClause.Occur.MUST)
        .build();
    Set<QueryTerm> expected
        = Collections.singleton(new QueryTerm("field1", "supercalifragilisticexpialidocious", QueryTerm.Type.EXACT));
    assertEquals(expected, treeBuilder.collectTerms(q, TermWeightor.DEFAULT));
  }

}
