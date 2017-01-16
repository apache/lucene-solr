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

package org.apache.lucene.queries.function;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestFunctionScoreQuery extends FunctionTestSetup {

  static IndexReader reader;
  static IndexSearcher searcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(true);
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
  }

  // FunctionQuery equivalent
  public void testSimpleSourceScore() throws Exception {

    FunctionScoreQuery q = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "first")),
        DoubleValuesSource.fromIntField(INT_FIELD));

    QueryUtils.check(random(), q, searcher, rarely());

    int expectedDocs[] = new int[]{ 4, 7, 9 };
    TopDocs docs = searcher.search(q, 4);
    assertEquals(expectedDocs.length, docs.totalHits);
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(docs.scoreDocs[i].doc, expectedDocs[i]);
    }

  }

  // CustomScoreQuery and BoostedQuery equivalent
  public void testScoreModifyingSource() throws Exception {

    DoubleValuesSource iii = DoubleValuesSource.fromIntField("iii");
    DoubleValuesSource score = DoubleValuesSource.scoringFunction(iii, (v, s) -> v * s);

    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(TEXT_FIELD, "first")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(TEXT_FIELD, "text")), BooleanClause.Occur.SHOULD)
        .build();
    TopDocs plain = searcher.search(bq, 1);

    FunctionScoreQuery fq = new FunctionScoreQuery(bq, score);

    QueryUtils.check(random(), fq, searcher, rarely());

    int[] expectedDocs = new int[]{ 4, 7, 9, 8, 12 };
    TopDocs docs = searcher.search(fq, 5);
    assertEquals(plain.totalHits, docs.totalHits);
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(expectedDocs[i], docs.scoreDocs[i].doc);

    }

  }

  // check boosts with non-distributive score source
  public void testBoostsAreAppliedLast() throws Exception {

    DoubleValuesSource scores
        = DoubleValuesSource.function(DoubleValuesSource.SCORES, v -> Math.log(v + 4));

    Query q1 = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "text")), scores);
    TopDocs plain = searcher.search(q1, 5);

    Query boosted = new BoostQuery(q1, 2);
    TopDocs afterboost = searcher.search(boosted, 5);
    assertEquals(plain.totalHits, afterboost.totalHits);
    for (int i = 0; i < 5; i++) {
      assertEquals(plain.scoreDocs[i].doc, afterboost.scoreDocs[i].doc);
      assertEquals(plain.scoreDocs[i].score, afterboost.scoreDocs[i].score / 2, 0.0001);
    }

  }

}
