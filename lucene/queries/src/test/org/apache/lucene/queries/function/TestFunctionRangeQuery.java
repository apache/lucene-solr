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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFunctionRangeQuery extends FunctionTestSetup {

  IndexReader indexReader;
  IndexSearcher indexSearcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createIndex(true);//doMultiSegment
  }

  @Before
  protected void before() throws IOException {
    indexReader = DirectoryReader.open(dir);
    indexSearcher = newSearcher(indexReader);
  }

  @After
  public void after() throws IOException {
    indexReader.close();
  }

  @Test
  public void testRangeInt() throws IOException {
    doTestRange(INT_VALUESOURCE);
  }

  @Test
  public void testRangeFloat() throws IOException {
    doTestRange(FLOAT_VALUESOURCE);
  }

  private void doTestRange(ValueSource valueSource) throws IOException {
    Query rangeQuery = new FunctionRangeQuery(valueSource, 2, 4, true, false);
    ScoreDoc[] scoreDocs = indexSearcher.search(rangeQuery, N_DOCS).scoreDocs;
    expectScores(scoreDocs, 3, 2);

    rangeQuery = new FunctionRangeQuery(valueSource, 2, 4, false, true);
    scoreDocs = indexSearcher.search(rangeQuery, N_DOCS).scoreDocs;
    expectScores(scoreDocs, 4, 3);
  }

  @Test
  public void testDeleted() throws IOException {
    // We delete doc with #3. Note we don't commit it to disk; we search using a near eal-time reader.
    final ValueSource valueSource = INT_VALUESOURCE;
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(null));
    try {
      writer.deleteDocuments(new FunctionRangeQuery(valueSource, 3, 3, true, true));//delete the one with #3
      assert writer.hasDeletions();
      try (IndexReader indexReader2 = DirectoryReader.open(writer)) {
        IndexSearcher indexSearcher2 = new IndexSearcher(indexReader2);
        TopDocs topDocs = indexSearcher2.search(new FunctionRangeQuery(valueSource, 3, 4, true, true), N_DOCS);
        expectScores(topDocs.scoreDocs, 4);//missing #3 because it's deleted
      }
    } finally {
      writer.rollback();
      writer.close();
    }
  }

  @Test
  public void testExplain() throws IOException {
    Query rangeQuery = new FunctionRangeQuery(INT_VALUESOURCE, 2, 2, true, true);
    ScoreDoc[] scoreDocs = indexSearcher.search(rangeQuery, N_DOCS).scoreDocs;
    Explanation explain = indexSearcher.explain(rangeQuery, scoreDocs[0].doc);
    // Just validate it looks reasonable
    assertEquals(
            "2.0 = frange(int(" + INT_FIELD + ")):[2 TO 2]\n" +
            "  2.0 = int(" + INT_FIELD + ")=2\n",
        explain.toString());
  }

  private void expectScores(ScoreDoc[] scoreDocs, int... docScores) {
    assertEquals(docScores.length, scoreDocs.length);
    for (int i = 0; i < docScores.length; i++) {
      assertEquals(docScores[i], scoreDocs[i].score, 0.0);
    }
  }
}
