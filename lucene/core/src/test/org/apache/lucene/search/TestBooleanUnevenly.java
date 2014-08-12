package org.apache.lucene.search;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * BooleanQuery.scorer should be tested, when hit documents
 * are very unevenly distributed.
 */
public class TestBooleanUnevenly extends LuceneTestCase {
  private static IndexSearcher searcher;
  private static IndexReader reader;

  public static final String field = "field";
  private static Directory directory;

  private static int count1;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), directory, new MockAnalyzer(random()));
    Document doc;
    count1 = 0;
    for (int i=0;i<2;i++) {
      for (int j=0;j<2048;j++) {
        doc = new Document();
        doc.add(newTextField(field, "1", Field.Store.NO));
        count1 ++;
        w.addDocument(doc);
      }
      for (int j=0;j<2048;j++) {
        doc = new Document();
        doc.add(newTextField(field, "2", Field.Store.NO));
        w.addDocument(doc);
      }
      doc = new Document();
      doc.add(newTextField(field, "1", Field.Store.NO));
      count1 ++;
      w.addDocument(doc);
      for (int j=0;j<2048;j++) {
        doc = new Document();
        doc.add(newTextField(field, "2", Field.Store.NO));
        w.addDocument(doc);
      }
    }
    reader = w.getReader();
    searcher = newSearcher(reader);
    w.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    searcher = null;
    reader = null;
    directory = null;
  }

  @Test
  public void testQueries01() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(field, "1")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "1")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(field, "2")), BooleanClause.Occur.SHOULD);

    TopScoreDocCollector collector = TopScoreDocCollector.create(1000, false);
    searcher.search(query, null, collector);
    TopDocs tops1 = collector.topDocs();
    ScoreDoc[] hits1 = tops1.scoreDocs;
    int hitsNum1 = tops1.totalHits;

    collector = TopScoreDocCollector.create(1000, true);
    searcher.search(query, null, collector);
    TopDocs tops2 = collector.topDocs();
    ScoreDoc[] hits2 = tops2.scoreDocs;
    int hitsNum2 = tops2.totalHits;

    assertEquals(count1, hitsNum1);
    assertEquals(count1, hitsNum2);
    CheckHits.checkEqual(query, hits1, hits2);
  }

  @Test
  public void testQueries02() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term(field, "1")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term(field, "1")), BooleanClause.Occur.SHOULD);

    TopScoreDocCollector collector = TopScoreDocCollector.create(1000, false);
    searcher.search(query, null, collector);
    TopDocs tops1 = collector.topDocs();
    ScoreDoc[] hits1 = tops1.scoreDocs;
    int hitsNum1 = tops1.totalHits;

    collector = TopScoreDocCollector.create(1000, true);
    searcher.search(query, null, collector);
    TopDocs tops2 = collector.topDocs();
    ScoreDoc[] hits2 = tops2.scoreDocs;
    int hitsNum2 = tops2.totalHits;

    assertEquals(count1, hitsNum1);
    assertEquals(count1, hitsNum2);
    CheckHits.checkEqual(query, hits1, hits2);
  }
}
