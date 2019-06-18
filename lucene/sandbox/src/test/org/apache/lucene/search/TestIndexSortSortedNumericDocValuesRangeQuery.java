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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import static org.hamcrest.CoreMatchers.instanceOf;

public class TestIndexSortSortedNumericDocValuesRangeQuery extends LuceneTestCase {

  public void testSameHitsAsPointRangeQuery() throws IOException {
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();

      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      boolean reverse = random().nextBoolean();
      SortField sortField = new SortedNumericSortField("dv", SortField.Type.LONG, reverse);
      sortField.setMissingValue(random().nextLong());
      iwc.setIndexSort(new Sort(sortField));

      RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = TestUtil.nextInt(random(), 0, 1);
        for (int j = 0; j < numValues; ++j) {
          final long value = TestUtil.nextLong(random(), -100, 10000);
          doc.add(new SortedNumericDocValuesField("dv", value));
          doc.add(new LongPoint("idx", value));
        }
        iw.addDocument(doc);
      }
      if (random().nextBoolean()) {
        iw.deleteDocuments(LongPoint.newRangeQuery("idx", 0L, 10L));
      }
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader, false);
      iw.close();

      for (int i = 0; i < 100; ++i) {
        final long min = random().nextBoolean() ? Long.MIN_VALUE : TestUtil.nextLong(random(), -100, 10000);
        final long max = random().nextBoolean() ? Long.MAX_VALUE : TestUtil.nextLong(random(), -100, 10000);
        final Query q1 = LongPoint.newRangeQuery("idx", min, max);
        final Query q2 = createQuery("dv", min, max);
        assertSameHits(searcher, q1, q2, false);
      }

      reader.close();
      dir.close();
    }
  }

  private void assertSameHits(IndexSearcher searcher, Query q1, Query q2, boolean scores) throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
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

  public void testEquals() {
    Query q1 = createQuery("foo", 3, 5);
    QueryUtils.checkEqual(q1, createQuery("foo", 3, 5));
    QueryUtils.checkUnequal(q1, createQuery("foo", 3, 6));
    QueryUtils.checkUnequal(q1, createQuery("foo", 4, 5));
    QueryUtils.checkUnequal(q1, createQuery("bar", 3, 5));
  }

  public void testToString() {
    Query q1 = createQuery("foo", 3, 5);
    assertEquals("foo:[3 TO 5]", q1.toString());
    assertEquals("[3 TO 5]", q1.toString("foo"));
    assertEquals("foo:[3 TO 5]", q1.toString("bar"));

    Query q2 = SortedSetDocValuesField.newSlowRangeQuery("foo", new BytesRef("bar"), new BytesRef("baz"), true, true);
    assertEquals("foo:[[62 61 72] TO [62 61 7a]]", q2.toString());
    q2 = SortedSetDocValuesField.newSlowRangeQuery("foo", new BytesRef("bar"), new BytesRef("baz"), false, true);
    assertEquals("foo:{[62 61 72] TO [62 61 7a]]", q2.toString());
    q2 = SortedSetDocValuesField.newSlowRangeQuery("foo", new BytesRef("bar"), new BytesRef("baz"), false, false);
    assertEquals("foo:{[62 61 72] TO [62 61 7a]}", q2.toString());
    q2 = SortedSetDocValuesField.newSlowRangeQuery("foo", new BytesRef("bar"), null, true, true);
    assertEquals("foo:[[62 61 72] TO *}", q2.toString());
    q2 = SortedSetDocValuesField.newSlowRangeQuery("foo", null, new BytesRef("baz"), true, true);
    assertEquals("foo:{* TO [62 61 7a]]", q2.toString());
    assertEquals("{* TO [62 61 7a]]", q2.toString("foo"));
    assertEquals("foo:{* TO [62 61 7a]]", q2.toString("bar"));
  }

  public void testIndexSortDocValuesWithEvenLength() throws Exception {
    testIndexSortDocValuesWithEvenLength(false);
    testIndexSortDocValuesWithEvenLength(true);
  }

  public void testIndexSortDocValuesWithEvenLength(boolean reverse) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 30));
    writer.addDocument(createDocument("field", 35));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertEquals(1, searcher.count(createQuery("field", -80, -80)));
    assertEquals(1, searcher.count(createQuery("field", -5, -5)));
    assertEquals(2, searcher.count(createQuery("field", 0, 0)));
    assertEquals(1, searcher.count(createQuery("field", 30, 30)));
    assertEquals(1, searcher.count(createQuery("field", 35, 35)));

    assertEquals(0, searcher.count(createQuery("field", -90, -90)));
    assertEquals(0, searcher.count(createQuery("field", 5, 5)));
    assertEquals(0, searcher.count(createQuery("field", 40, 40)));

    // Test the lower end of the document value range.
    assertEquals(2, searcher.count(createQuery("field", -90, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -4)));
    assertEquals(1, searcher.count(createQuery("field", -70, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -5)));

    // Test the upper end of the document value range.
    assertEquals(1, searcher.count(createQuery("field", 25, 34)));
    assertEquals(2, searcher.count(createQuery("field", 25, 35)));
    assertEquals(2, searcher.count(createQuery("field", 25, 36)));
    assertEquals(2, searcher.count(createQuery("field", 30, 35)));

    // Test multiple occurrences of the same value.
    assertEquals(2, searcher.count(createQuery("field", -4, 4)));
    assertEquals(2, searcher.count(createQuery("field", -4, 0)));
    assertEquals(2, searcher.count(createQuery("field", 0, 4)));
    assertEquals(3, searcher.count(createQuery("field", 0, 30)));

    // Test ranges that span all documents.
    assertEquals(6, searcher.count(createQuery("field", -80, 35)));
    assertEquals(6, searcher.count(createQuery("field", -90, 40)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortDocValuesWithOddLength() throws Exception {
    testIndexSortDocValuesWithOddLength(false);
    testIndexSortDocValuesWithOddLength(true);
  }

  public void testIndexSortDocValuesWithOddLength(boolean reverse) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 5));
    writer.addDocument(createDocument("field", 30));
    writer.addDocument(createDocument("field", 35));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    // Test ranges consisting of one value.
    assertEquals(1, searcher.count(createQuery("field", -80, -80)));
    assertEquals(1, searcher.count(createQuery("field", -5, -5)));
    assertEquals(2, searcher.count(createQuery("field", 0, 0)));
    assertEquals(1, searcher.count(createQuery("field", 5, 5)));
    assertEquals(1, searcher.count(createQuery("field", 30, 30)));
    assertEquals(1, searcher.count(createQuery("field", 35, 35)));

    assertEquals(0, searcher.count(createQuery("field", -90, -90)));
    assertEquals(0, searcher.count(createQuery("field", 6, 6)));
    assertEquals(0, searcher.count(createQuery("field", 40, 40)));

    // Test the lower end of the document value range.
    assertEquals(2, searcher.count(createQuery("field", -90, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -4)));
    assertEquals(1, searcher.count(createQuery("field", -70, -4)));
    assertEquals(2, searcher.count(createQuery("field", -80, -5)));

    // Test the upper end of the document value range.
    assertEquals(1, searcher.count(createQuery("field", 25, 34)));
    assertEquals(2, searcher.count(createQuery("field", 25, 35)));
    assertEquals(2, searcher.count(createQuery("field", 25, 36)));
    assertEquals(2, searcher.count(createQuery("field", 30, 35)));

    // Test multiple occurrences of the same value.
    assertEquals(2, searcher.count(createQuery("field", -4, 4)));
    assertEquals(2, searcher.count(createQuery("field", -4, 0)));
    assertEquals(2, searcher.count(createQuery("field", 0, 4)));
    assertEquals(4, searcher.count(createQuery("field", 0, 30)));

    // Test ranges that span all documents.
    assertEquals(7, searcher.count(createQuery("field", -80, 35)));
    assertEquals(7, searcher.count(createQuery("field", -90, 40)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortDocValuesWithSingleValue() throws Exception {
    testIndexSortDocValuesWithSingleValue(false);
    testIndexSortDocValuesWithSingleValue(true);
  }

  private void testIndexSortDocValuesWithSingleValue(boolean reverse) throws IOException{
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    Sort indexSort = new Sort(new SortedNumericSortField("field", SortField.Type.LONG, reverse));
    iwc.setIndexSort(indexSort);
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", 42));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    assertEquals(1, searcher.count(createQuery("field", 42, 43)));
    assertEquals(1, searcher.count(createQuery("field", 42, 42)));
    assertEquals(0, searcher.count(createQuery("field", 41, 41)));
    assertEquals(0, searcher.count(createQuery("field", 43, 43)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testIndexSortMissingValues() throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    SortField sortField = new SortedNumericSortField("field", SortField.Type.LONG);
    sortField.setMissingValue(random().nextLong());
    iwc.setIndexSort(new Sort(sortField));
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);

    writer.addDocument(createDocument("field", -80));
    writer.addDocument(createDocument("field", -5));
    writer.addDocument(createDocument("field", 0));
    writer.addDocument(createDocument("field", 35));

    writer.addDocument(createDocument("other-field", 0));
    writer.addDocument(createDocument("other-field", 10));
    writer.addDocument(createDocument("other-field", 20));

    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    assertEquals(2, searcher.count(createQuery("field", -70, 0)));
    assertEquals(2, searcher.count(createQuery("field", -2, 35)));

    assertEquals(4, searcher.count(createQuery("field", -80, 35)));
    assertEquals(4, searcher.count(createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testNoDocuments() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    Query query = createQuery("foo", 2, 4);
    Weight w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
    assertNull(w.scorer(searcher.getIndexReader().leaves().get(0)));

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRewriteExhaustiveRange() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();

    Query query = createQuery("field", Long.MIN_VALUE, Long.MAX_VALUE);
    Query rewrittenQuery = query.rewrite(reader);
    assertEquals(new DocValuesFieldExistsQuery("field"), rewrittenQuery);

    writer.close();
    reader.close();
    dir.close();
  }

  public void testRewriteFallbackQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    writer.addDocument(new Document());
    IndexReader reader = writer.getReader();

    // Create an (unrealistic) fallback query that is sure to be rewritten.
    Query fallbackQuery = new BooleanQuery.Builder().build();
    Query query = new IndexSortSortedNumericDocValuesRangeQuery("field", 1, 42, fallbackQuery);

    Query rewrittenQuery = query.rewrite(reader);
    assertNotEquals(query, rewrittenQuery);
    assertThat(rewrittenQuery, instanceOf(IndexSortSortedNumericDocValuesRangeQuery.class));

    IndexSortSortedNumericDocValuesRangeQuery rangeQuery = (IndexSortSortedNumericDocValuesRangeQuery) rewrittenQuery;
    assertEquals(new MatchNoDocsQuery(), rangeQuery.getFallbackQuery());

    writer.close();
    reader.close();
    dir.close();
  }

  /**
   * Test that the index sort optimization not activated if there is no index sort.
   */
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

    testIndexSortOptimizationDeactivated(writer);

    writer.close();
    dir.close();
  }

  public void testIndexSortOptimizationDeactivated(RandomIndexWriter writer) throws IOException {
    DirectoryReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);

    Query query = createQuery("field", 0, 0);
    Weight weight = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);

    // Check that the two-phase iterator is not null, indicating that we've fallen
    // back to SortedNumericDocValuesField.newSlowRangeQuery.
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(context);
      assertNotNull(scorer.twoPhaseIterator());
    }

    reader.close();
  }

  private Document createDocument(String field, long value) {
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField(field, value));
    return doc;
  }

  private Query createQuery(String field, long lowerValue, long upperValue) {
    Query fallbackQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, lowerValue, upperValue);
    return new IndexSortSortedNumericDocValuesRangeQuery(field, lowerValue, upperValue, fallbackQuery);
  }
}
