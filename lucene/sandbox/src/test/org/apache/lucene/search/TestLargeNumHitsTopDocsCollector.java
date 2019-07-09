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

package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestLargeNumHitsTopDocsCollector extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private final Query testQuery = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("field", "5")), BooleanClause.Occur.SHOULD)
      .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
      .build();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 1_000; i++) {
      Document doc = new Document();
      doc.add(newStringField("field", "5", Field.Store.NO));
      writer.addDocument(doc);
      writer.addDocument(new Document());
    }
    reader = writer.getReader();
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    dir = null;
    super.tearDown();
  }
  public void testRequestMoreHitsThanCollected() throws Exception {
    runNumHits(150);
  }

  public void testSingleNumHit() throws Exception {
    runNumHits(1);
  }

  public void testRequestLessHitsThanCollected() throws Exception {
    runNumHits(25);
  }

  public void testIllegalArguments() throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    LargeNumHitsTopDocsCollector largeCollector = new LargeNumHitsTopDocsCollector(15);
    TopScoreDocCollector regularCollector = TopScoreDocCollector.create(15, null, Integer.MAX_VALUE);

    searcher.search(testQuery, largeCollector);
    searcher.search(testQuery, regularCollector);

    assertEquals(largeCollector.totalHits, regularCollector.totalHits);

    IllegalArgumentException expected  = expectThrows(IllegalArgumentException.class, () -> {
      largeCollector.topDocs(350_000);
    });

    assertTrue(expected.getMessage().contains("Incorrect number of hits requested"));
  }

  public void testNoPQBuild() throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    LargeNumHitsTopDocsCollector largeCollector = new LargeNumHitsTopDocsCollector(250_000);
    TopScoreDocCollector regularCollector = TopScoreDocCollector.create(250_000, null, Integer.MAX_VALUE);

    searcher.search(testQuery, largeCollector);
    searcher.search(testQuery, regularCollector);

    assertEquals(largeCollector.totalHits, regularCollector.totalHits);

    assertEquals(largeCollector.pq, null);
    assertEquals(largeCollector.pqTop, null);
  }

  public void testPQBuild() throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    LargeNumHitsTopDocsCollector largeCollector = new LargeNumHitsTopDocsCollector(50);
    TopScoreDocCollector regularCollector = TopScoreDocCollector.create(50, null, Integer.MAX_VALUE);

    searcher.search(testQuery, largeCollector);
    searcher.search(testQuery, regularCollector);

    assertEquals(largeCollector.totalHits, regularCollector.totalHits);

    assertNotEquals(largeCollector.pq, null);
    assertNotEquals(largeCollector.pqTop, null);
  }

  public void testNoPQHitsOrder() throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    LargeNumHitsTopDocsCollector largeCollector = new LargeNumHitsTopDocsCollector(250_000);
    TopScoreDocCollector regularCollector = TopScoreDocCollector.create(250_000, null, Integer.MAX_VALUE);

    searcher.search(testQuery, largeCollector);
    searcher.search(testQuery, regularCollector);

    assertEquals(largeCollector.totalHits, regularCollector.totalHits);

    assertEquals(largeCollector.pq, null);
    assertEquals(largeCollector.pqTop, null);

    TopDocs topDocs = largeCollector.topDocs();

    if (topDocs.scoreDocs.length > 0) {
      float preScore = topDocs.scoreDocs[0].score;
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        assert scoreDoc.score <= preScore;
        preScore = scoreDoc.score;
      }
    }
  }

  private void runNumHits(int numHits) throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    LargeNumHitsTopDocsCollector largeCollector = new LargeNumHitsTopDocsCollector(numHits);
    TopScoreDocCollector regularCollector = TopScoreDocCollector.create(numHits, null, Integer.MAX_VALUE);

    searcher.search(testQuery, largeCollector);
    searcher.search(testQuery, regularCollector);

    assertEquals(largeCollector.totalHits, regularCollector.totalHits);

    TopDocs firstTopDocs = largeCollector.topDocs();
    TopDocs secondTopDocs = regularCollector.topDocs();

    assertEquals(firstTopDocs.scoreDocs.length, secondTopDocs.scoreDocs.length);

    CheckHits.checkEqual(testQuery, firstTopDocs.scoreDocs, secondTopDocs.scoreDocs);
  }
}
