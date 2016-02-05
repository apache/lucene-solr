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
package org.apache.lucene.search.similarities;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestClassicSimilarity extends LuceneTestCase {
  private Directory directory;
  private IndexReader indexReader;
  private IndexSearcher indexSearcher;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    try (IndexWriter indexWriter = new IndexWriter(directory, newIndexWriterConfig())) {
      Document document = new Document();
      document.add(new StringField("test", "hit", Store.NO));
      indexWriter.addDocument(document);
      indexWriter.commit();
    }
    indexReader = DirectoryReader.open(directory);
    indexSearcher = newSearcher(indexReader);
    indexSearcher.setSimilarity(new ClassicSimilarity());
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(indexReader, directory);
    super.tearDown();
  }
  
  // Javadocs give this as an example so we test to make sure it's correct:
  public void testPrecisionLoss() throws Exception {
    ClassicSimilarity sim = new ClassicSimilarity();
    float v = sim.decodeNormValue(sim.encodeNormValue(.89f));
    assertEquals(0.875f, v, 0.0001f);
  }


  public void testHit() throws IOException {
    Query query = new TermQuery(new Term("test", "hit"));
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1, topDocs.scoreDocs.length);
    assertTrue(topDocs.scoreDocs[0].score != 0);
  }

  public void testMiss() throws IOException {
    Query query = new TermQuery(new Term("test", "miss"));
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(0, topDocs.totalHits);
  }

  public void testEmpty() throws IOException {
    Query query = new TermQuery(new Term("empty", "miss"));
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(0, topDocs.totalHits);
  }

  public void testBQHit() throws IOException {
    Query query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("test", "hit")), Occur.SHOULD)
      .build();
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1, topDocs.scoreDocs.length);
    assertTrue(topDocs.scoreDocs[0].score != 0);
  }

  public void testBQHitOrMiss() throws IOException {
    Query query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("test", "hit")), Occur.SHOULD)
      .add(new TermQuery(new Term("test", "miss")), Occur.SHOULD)
      .build();
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1, topDocs.scoreDocs.length);
    assertTrue(topDocs.scoreDocs[0].score != 0);
  }

  public void testBQHitOrEmpty() throws IOException {
    Query query = new BooleanQuery.Builder()
      .add(new TermQuery(new Term("test", "hit")), Occur.SHOULD)
      .add(new TermQuery(new Term("empty", "miss")), Occur.SHOULD)
      .build();
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1, topDocs.scoreDocs.length);
    assertTrue(topDocs.scoreDocs[0].score != 0);
  }

  public void testDMQHit() throws IOException {
    Query query = new DisjunctionMaxQuery(
      Arrays.asList(
        new TermQuery(new Term("test", "hit"))),
      0);
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1, topDocs.scoreDocs.length);
    assertTrue(topDocs.scoreDocs[0].score != 0);
  }

  public void testDMQHitOrMiss() throws IOException {
    Query query = new DisjunctionMaxQuery(
      Arrays.asList(
        new TermQuery(new Term("test", "hit")),
        new TermQuery(new Term("test", "miss"))),
      0);
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1, topDocs.scoreDocs.length);
    assertTrue(topDocs.scoreDocs[0].score != 0);
  }

  public void testDMQHitOrEmpty() throws IOException {
    Query query = new DisjunctionMaxQuery(
      Arrays.asList(
        new TermQuery(new Term("test", "hit")),
        new TermQuery(new Term("empty", "miss"))),
      0);
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits);
    assertEquals(1, topDocs.scoreDocs.length);
    assertTrue(topDocs.scoreDocs[0].score != 0);
  }
  
  public void testSaneNormValues() {
    ClassicSimilarity sim = new ClassicSimilarity();
    for (int i = 0; i < 256; i++) {
      float boost = sim.decodeNormValue((byte) i);
      assertFalse("negative boost: " + boost + ", byte=" + i, boost < 0.0f);
      assertFalse("inf bost: " + boost + ", byte=" + i, Float.isInfinite(boost));
      assertFalse("nan boost for byte=" + i, Float.isNaN(boost));
      if (i > 0) {
        assertTrue("boost is not increasing: " + boost + ",byte=" + i, boost > sim.decodeNormValue((byte)(i-1)));
      }
    }
  }
}
