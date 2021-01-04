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
package org.apache.lucene.sandbox.search;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class TestIndexSortDocValuesNumbersQuery extends LuceneTestCase {

  public void testEquals() {
    assertEquals(new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L, 42L), 17L, 42L),
        new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L, 42L), 17L, 42L));
    assertEquals(new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L, 42L, 32416190071L), 17L, 42L, 32416190071L),
        new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L, 42L, 32416190071L), 17L, 42L, 32416190071L));
    assertNotEquals(new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L, 42L),
        17L, 42L), new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L), 17L));
    assertNotEquals(new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L, 42L),
        17L, 42L), new IndexSortDocValuesNumbersQuery("field2", LongPoint.newSetQuery("field", 17L, 42L), 17L, 42L));
    assertNotEquals(new IndexSortDocValuesNumbersQuery("field", LongPoint.newSetQuery("field", 17L),
        17L, 42L), new IndexSortDocValuesNumbersQuery("field2", LongPoint.newSetQuery("field", 17L, 42L), 17L, 42L));
  }

  public void testSameHitsAsPointInSetQuery() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean reverse = random().nextBoolean();
      SortField sortField = new SortedNumericSortField("dv", SortField.Type.LONG, reverse);
      sortField.setMissingValue(TestUtil.nextLong(random(), -1000, 1000));
      iwc.setIndexSort(new Sort(sortField));

      RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = TestUtil.nextInt(random(), 0, 1);
        for (int j = 0; j < numValues; ++j) {
          final long value = TestUtil.nextLong(random(), -1000, 1000);
          doc.add(new SortedNumericDocValuesField("dv", value));
          doc.add(new LongPoint("idx", value));
        }
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(LongPoint.newSetQuery("idx", randomLongs(0, 10, -100, 100)));
      }
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader);
      iw.close();

      for (int i = 0; i < 100; ++i) {
        long[] longs = randomLongs(1, 100, -100, 100);
        final Query q1 = LongPoint.newSetQuery("idx", longs);
        final Query q2 = createQuery("dv", longs);
        assertSameHits(searcher, q1, q2, false);
      }

      reader.close();
      dir.close();
    }
  }

  private long[] randomLongs(int minLen, int maxLen, int minNum, int maxNum) {
    int termLength = TestUtil.nextInt(random(), minLen, maxLen);
    long[] terms = new long[termLength];
    for (int i = 0; i < termLength; i++) {
      terms[i] = TestUtil.nextLong(random(), minNum, maxNum);
    }
    return terms;
  }

  private void assertSameHits(IndexSearcher searcher, Query q1, Query q2, boolean scores) throws IOException {
    final int maxDoc = searcher.getIndexReader().leaves().get(0).reader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);

    assertEquals(td1.totalHits.value, td2.totalHits.value);
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }

  private Query createQuery(String field, long... values) {
    Query fallbackQuery = LongPoint.newSetQuery(field, values);
    return new IndexSortDocValuesNumbersQuery(field, fallbackQuery, values);
  }

  /**
   * Test that the index sort optimization is not activated when some documents
   * have multiple values.
   */
  public void testMultiDocValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("field", 0));
    doc.add(new SortedNumericDocValuesField("field", 10));
    writer.addDocument(doc);

    testIndexSortOptimizationDeactivated(writer, 0);

    writer.close();
    dir.close();
  }

  public void testNoIndexSort() throws Exception {
    Directory dir = newDirectory();

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(createDocument("field", 0));

    testIndexSortOptimizationDeactivated(writer);

    writer.close();
    dir.close();
  }

  /**
   * Test that the index sort optimization is not activated when the sort is
   * on the wrong field.
   */
  public void testIndexSortOnWrongField() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("other-field", SortField.Type.LONG));
    iwc.setIndexSort(indexSort);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
    writer.addDocument(createDocument("field", 0));

    testIndexSortOptimizationDeactivated(writer);

    writer.close();
    dir.close();
  }

  private void testIndexSortOptimizationDeactivated(RandomIndexWriter writer, long... dvExist) throws IOException {
    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // search existed dv to guarantee scorer will not be null if no fallback
    Query query = createQuery("field", dvExist);
    Weight weight = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);

    // Check scorer is null, indicating that it fallback to LongPoint.newSetQuery.
    // It will always be null when fallback because we did not index any point value
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(context);
      assertNull(scorer);
    }

    reader.close();
  }

  private Document createDocument(String field, long value) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(field, value));
    return doc;
  }

  public void testNoDocuments() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    Query query = createQuery("foo", 2);
    Weight w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
    assertNull(w.scorer(searcher.getIndexReader().leaves().get(0)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testToString() {
    Query q1 = createQuery("foo", 1, 2, 3);
    assertEquals("foo:[1, 2, 3]", q1.toString());
    assertEquals("[1, 2, 3]", q1.toString("foo"));
    assertEquals("foo:[1, 2, 3]", q1.toString("bar"));
  }

  public void testRewriteFallbackQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();

    // Create an (unrealistic) fallback query that is sure to be rewritten.
    Query fallbackQuery = new BooleanQuery.Builder().build();
    Query query = new IndexSortDocValuesNumbersQuery("field", fallbackQuery, 1);

    Query rewrittenQuery = query.rewrite(reader);
    assertNotEquals(query, rewrittenQuery);
    org.hamcrest.MatcherAssert.assertThat(rewrittenQuery, instanceOf(IndexSortDocValuesNumbersQuery.class));
    assertEquals(new MatchNoDocsQuery(), ((IndexSortDocValuesNumbersQuery)rewrittenQuery).getFallbackQuery());

    writer.close();
    reader.close();
    dir.close();
  }
}
