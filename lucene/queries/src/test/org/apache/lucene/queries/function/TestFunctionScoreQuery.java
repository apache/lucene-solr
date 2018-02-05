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

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
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

  public void testEqualities() {

    Query q1 = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(1));
    Query q2 = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(1));
    Query q3 = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(2));
    Query q4 = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(2));

    QueryUtils.check(q1);
    QueryUtils.checkUnequal(q1, q3);
    QueryUtils.checkUnequal(q1, q2);
    QueryUtils.checkUnequal(q2, q3);
    QueryUtils.checkEqual(q3, q4);

    Query bq1 = FunctionScoreQuery.boostByValue(new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(2));
    QueryUtils.check(bq1);
    Query bq2 = FunctionScoreQuery.boostByValue(new TermQuery(new Term(TEXT_FIELD, "a")), DoubleValuesSource.constant(4));
    QueryUtils.checkUnequal(bq1, bq2);
    Query bq3 = FunctionScoreQuery.boostByValue(new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(4));
    QueryUtils.checkUnequal(bq1, bq3);
    QueryUtils.checkUnequal(bq2, bq3);
    Query bq4 = FunctionScoreQuery.boostByValue(new TermQuery(new Term(TEXT_FIELD, "b")), DoubleValuesSource.constant(4));
    QueryUtils.checkEqual(bq3, bq4);

    Query qq1 = FunctionScoreQuery.boostByQuery(new TermQuery(new Term(TEXT_FIELD, "a")), new TermQuery(new Term(TEXT_FIELD, "z")), 0.1f);
    QueryUtils.check(qq1);
    Query qq2 = FunctionScoreQuery.boostByQuery(new TermQuery(new Term(TEXT_FIELD, "a")), new TermQuery(new Term(TEXT_FIELD, "z")), 0.2f);
    QueryUtils.checkUnequal(qq1, qq2);
    Query qq3 = FunctionScoreQuery.boostByQuery(new TermQuery(new Term(TEXT_FIELD, "b")), new TermQuery(new Term(TEXT_FIELD, "z")), 0.1f);
    QueryUtils.checkUnequal(qq1, qq3);
    QueryUtils.checkUnequal(qq2, qq3);
    Query qq4 = FunctionScoreQuery.boostByQuery(new TermQuery(new Term(TEXT_FIELD, "a")), new TermQuery(new Term(TEXT_FIELD, "zz")), 0.1f);
    QueryUtils.checkUnequal(qq1, qq4);
    QueryUtils.checkUnequal(qq2, qq4);
    QueryUtils.checkUnequal(qq3, qq4);
    Query qq5 = FunctionScoreQuery.boostByQuery(new TermQuery(new Term(TEXT_FIELD, "a")), new TermQuery(new Term(TEXT_FIELD, "z")), 0.1f);
    QueryUtils.checkEqual(qq1, qq5);

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

    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(TEXT_FIELD, "first")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term(TEXT_FIELD, "text")), BooleanClause.Occur.SHOULD)
        .build();
    TopDocs plain = searcher.search(bq, 1);

    FunctionScoreQuery fq = FunctionScoreQuery.boostByValue(bq, DoubleValuesSource.fromIntField("iii"));

    QueryUtils.check(random(), fq, searcher, rarely());

    int[] expectedDocs = new int[]{ 4, 7, 9, 8, 12 };
    TopDocs docs = searcher.search(fq, 5);
    assertEquals(plain.totalHits, docs.totalHits);
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(expectedDocs[i], docs.scoreDocs[i].doc);

    }

  }

  // BoostingQuery equivalent
  public void testCombiningMultipleQueryScores() throws Exception {

    TermQuery q = new TermQuery(new Term(TEXT_FIELD, "text"));
    TopDocs plain = searcher.search(q, 1);

    FunctionScoreQuery fq
        = FunctionScoreQuery.boostByQuery(q, new TermQuery(new Term(TEXT_FIELD, "rechecking")), 100f);

    QueryUtils.check(random(), fq, searcher, rarely());

    int[] expectedDocs = new int[]{ 6, 1, 0, 2, 8 };
    TopDocs docs = searcher.search(fq, 20);
    assertEquals(plain.totalHits, docs.totalHits);
    for (int i = 0; i < expectedDocs.length; i++) {
      assertEquals(expectedDocs[i], docs.scoreDocs[i].doc);

    }
  }

  // check boosts with non-distributive score source
  public void testBoostsAreAppliedLast() throws Exception {

    SimpleBindings bindings = new SimpleBindings();
    bindings.add("score", DoubleValuesSource.SCORES);
    Expression expr = JavascriptCompiler.compile("ln(score + 4)");

    Query q1 = new FunctionScoreQuery(new TermQuery(new Term(TEXT_FIELD, "text")), expr.getDoubleValuesSource(bindings));
    TopDocs plain = searcher.search(q1, 5);

    Query boosted = new BoostQuery(q1, 2);
    TopDocs afterboost = searcher.search(boosted, 5);
    assertEquals(plain.totalHits, afterboost.totalHits);
    for (int i = 0; i < 5; i++) {
      assertEquals(plain.scoreDocs[i].doc, afterboost.scoreDocs[i].doc);
      assertEquals(plain.scoreDocs[i].score, afterboost.scoreDocs[i].score / 2, 0.0001);
    }

  }

  public void testTruncateNegativeScores() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", -2));
    w.addDocument(doc);
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q = new FunctionScoreQuery(new MatchAllDocsQuery(), DoubleValuesSource.fromLongField("foo"));
    QueryUtils.check(random(), q, searcher);
    Explanation expl = searcher.explain(q, 0);
    assertEquals(0, expl.getValue().doubleValue(), 0f);
    assertTrue(expl.toString(), expl.getDetails()[0].getDescription().contains("truncated score"));
    reader.close();
    dir.close();
  }

  public void testNaN() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", Double.doubleToLongBits(Double.NaN)));
    w.addDocument(doc);
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);
    Query q = new FunctionScoreQuery(new MatchAllDocsQuery(), DoubleValuesSource.fromDoubleField("foo"));
    QueryUtils.check(random(), q, searcher);
    Explanation expl = searcher.explain(q, 0);
    assertEquals(0, expl.getValue().doubleValue(), 0f);
    assertTrue(expl.toString(), expl.getDetails()[0].getDescription().contains("NaN is an illegal score"));
    reader.close();
    dir.close();
  }
}
