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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.TFIDFSimilarity.IDFStats;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

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
  
  public void testSaneNormValues() throws IOException {
    ClassicSimilarity sim = new ClassicSimilarity();
    for (int i = 0; i < 256; i++) {
      float boost = TFIDFSimilarity.OLD_NORM_TABLE[i];
      assertFalse("negative boost: " + boost + ", byte=" + i, boost < 0.0f);
      assertFalse("inf bost: " + boost + ", byte=" + i, Float.isInfinite(boost));
      assertFalse("nan boost for byte=" + i, Float.isNaN(boost));
      if (i > 0) {
        assertTrue("boost is not increasing: " + boost + ",byte=" + i, boost > TFIDFSimilarity.OLD_NORM_TABLE[i-1]);
      }
    }

    TFIDFSimilarity.IDFStats stats = (IDFStats) sim.computeWeight(1f, new IndexSearcher(new MultiReader()).collectionStatistics("foo"));
    for (int i = 0; i < 256; i++) {
      float boost = stats.normTable[i];
      assertFalse("negative boost: " + boost + ", byte=" + i, boost < 0.0f);
      assertFalse("inf bost: " + boost + ", byte=" + i, Float.isInfinite(boost));
      assertFalse("nan boost for byte=" + i, Float.isNaN(boost));
      if (i > 0) {
        assertTrue("boost is not decreasing: " + boost + ",byte=" + i, boost < stats.normTable[i-1]);
      }
    }
  }

  public void testNormEncodingBackwardCompatibility() throws IOException {
    Similarity similarity = new ClassicSimilarity();
    for (int indexCreatedVersionMajor : new int[] { Version.LUCENE_6_0_0.major, Version.LATEST.major}) {
      for (int length : new int[] {1, 4, 16 }) { // these length values are encoded accurately on both cases
        Directory dir = newDirectory();
        // set the version on the directory
        new SegmentInfos(indexCreatedVersionMajor).commit(dir);
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(similarity));
        Document doc = new Document();
        String value = IntStream.range(0, length).mapToObj(i -> "b").collect(Collectors.joining(" "));
        doc.add(new TextField("foo", value, Store.NO));
        w.addDocument(doc);
        IndexReader reader = DirectoryReader.open(w);
        IndexSearcher searcher = newSearcher(reader);
        searcher.setSimilarity(similarity);
        Explanation expl = searcher.explain(new TermQuery(new Term("foo", "b")), 0);
        Explanation fieldNorm = findExplanation(expl, "fieldNorm");
        assertNotNull(fieldNorm);
        assertEquals(fieldNorm.toString(), 1/Math.sqrt(length), fieldNorm.getValue(), 0f);
        w.close();
        reader.close();
        dir.close();
      }
    }
  }

  private static Explanation findExplanation(Explanation expl, String text) {
    if (expl.getDescription().startsWith(text)) {
      return expl;
    } else {
      for (Explanation sub : expl.getDetails()) {
        Explanation match = findExplanation(sub, text);
        if (match != null) {
          return match;
        }
      }
    }
    return null;
  }

  public void testSameNormsAsBM25() {
    ClassicSimilarity sim1 = new ClassicSimilarity();
    BM25Similarity sim2 = new BM25Similarity();
    sim2.setDiscountOverlaps(true);
    for (int iter = 0; iter < 100; ++iter) {
      final int length = TestUtil.nextInt(random(), 1, 1000);
      final int position = random().nextInt(length);
      final int numOverlaps = random().nextInt(length);
      FieldInvertState state = new FieldInvertState(Version.LATEST.major, "foo", position, length, numOverlaps, 100);
      assertEquals(
          sim2.computeNorm(state),
          sim1.computeNorm(state),
          0f);
    }
  }
}
