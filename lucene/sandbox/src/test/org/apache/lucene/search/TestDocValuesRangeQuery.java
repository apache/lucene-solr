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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestDocValuesRangeQuery extends LuceneTestCase {

  public void testDuelNumericRangeQuery() throws IOException {
    final int iters = atLeast(10);
      for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
      final int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; ++i) {
        Document doc = new Document();
        final int numValues = random().nextInt(2);
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
      iw.commit();
      final IndexReader reader = iw.getReader();
      final IndexSearcher searcher = newSearcher(reader, false);
      iw.close();

      for (int i = 0; i < 100; ++i) {
        final Long min = TestUtil.nextLong(random(), -100, 1000);
        final Long max = TestUtil.nextLong(random(), -100, 1000);
        final Query q1 = LongPoint.newRangeQuery("idx", min, max);
        final Query q2 = DocValuesRangeQuery.newLongRange("dv", min, max, true, true);
        assertSameMatches(searcher, q1, q2, false);
      }

      reader.close();
      dir.close();
    }
  }

  private static BytesRef toSortableBytes(Long l) {
    if (l == null) {
      return null;
    } else {
      byte[] bytes = new byte[Long.BYTES];
      NumericUtils.longToSortableBytes(l, bytes, 0);
      return new BytesRef(bytes);
    }
  }

  public void testDuelNumericSorted() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      final int numValues = random().nextInt(3);
      for (int j = 0; j < numValues; ++j) {
        final long value = TestUtil.nextLong(random(), -100, 10000);
        doc.add(new SortedNumericDocValuesField("dv1", value));
        doc.add(new SortedSetDocValuesField("dv2", toSortableBytes(value)));
      }
      iw.addDocument(doc);
    }
    if (random().nextBoolean()) {
      iw.deleteDocuments(DocValuesRangeQuery.newLongRange("dv1", 0L, 10L, true, true));
    }
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    for (int i = 0; i < 100; ++i) {
      final Long min = random().nextBoolean() ? null : TestUtil.nextLong(random(), -100, 1000);
      final Long max = random().nextBoolean() ? null : TestUtil.nextLong(random(), -100, 1000);
      final boolean minInclusive = random().nextBoolean();
      final boolean maxInclusive = random().nextBoolean();
      final Query q1 = DocValuesRangeQuery.newLongRange("dv1", min, max, minInclusive, maxInclusive);
      final Query q2 = DocValuesRangeQuery.newBytesRefRange("dv2", toSortableBytes(min), toSortableBytes(max), minInclusive, maxInclusive);
      assertSameMatches(searcher, q1, q2, true);
    }

    reader.close();
    dir.close();
  }

  public void testScore() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      final int numValues = random().nextInt(3);
      for (int j = 0; j < numValues; ++j) {
        final long value = TestUtil.nextLong(random(), -100, 10000);
        doc.add(new SortedNumericDocValuesField("dv1", value));
        doc.add(new SortedSetDocValuesField("dv2", toSortableBytes(value)));
      }
      iw.addDocument(doc);
    }
    if (random().nextBoolean()) {
      iw.deleteDocuments(DocValuesRangeQuery.newLongRange("dv1", 0L, 10L, true, true));
    }
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    for (int i = 0; i < 100; ++i) {
      final Long min = random().nextBoolean() ? null : TestUtil.nextLong(random(), -100, 1000);
      final Long max = random().nextBoolean() ? null : TestUtil.nextLong(random(), -100, 1000);
      final boolean minInclusive = random().nextBoolean();
      final boolean maxInclusive = random().nextBoolean();

      final float boost = random().nextFloat() * 10;

      final Query q1 = new BoostQuery(DocValuesRangeQuery.newLongRange("dv1", min, max, minInclusive, maxInclusive), boost);
      final Query csq1 = new BoostQuery(new ConstantScoreQuery(DocValuesRangeQuery.newLongRange("dv1", min, max, minInclusive, maxInclusive)), boost);
      assertSameMatches(searcher, q1, csq1, true);

      final Query q2 = new BoostQuery(DocValuesRangeQuery.newBytesRefRange("dv2", toSortableBytes(min), toSortableBytes(max), minInclusive, maxInclusive), boost);
      final Query csq2 = new BoostQuery(new ConstantScoreQuery(DocValuesRangeQuery.newBytesRefRange("dv2", toSortableBytes(min), toSortableBytes(max), minInclusive, maxInclusive)), boost);
      assertSameMatches(searcher, q2, csq2, true);
    }

    reader.close();
    dir.close();
  }

  public void testApproximation() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      final int numValues = random().nextInt(3);
      for (int j = 0; j < numValues; ++j) {
        final long value = TestUtil.nextLong(random(), -100, 10000);
        doc.add(new SortedNumericDocValuesField("dv1", value));
        doc.add(new SortedSetDocValuesField("dv2", toSortableBytes(value)));
        doc.add(new LongPoint("idx", value));
        doc.add(new StringField("f", random().nextBoolean() ? "a" : "b", Store.NO));
      }
      iw.addDocument(doc);
    }
    if (random().nextBoolean()) {
      iw.deleteDocuments(LongPoint.newRangeQuery("idx", 0L, 10L));
    }
    iw.commit();
    final IndexReader reader = iw.getReader();
    final IndexSearcher searcher = newSearcher(reader, false);
    iw.close();

    for (int i = 0; i < 100; ++i) {
      final Long min = TestUtil.nextLong(random(), -100, 1000);
      final Long max = TestUtil.nextLong(random(), -100, 1000);

      BooleanQuery.Builder ref = new BooleanQuery.Builder();
      ref.add(LongPoint.newRangeQuery("idx", min, max), Occur.FILTER);
      ref.add(new TermQuery(new Term("f", "a")), Occur.MUST);

      BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
      bq1.add(DocValuesRangeQuery.newLongRange("dv1", min, max, true, true), Occur.FILTER);
      bq1.add(new TermQuery(new Term("f", "a")), Occur.MUST);

      assertSameMatches(searcher, ref.build(), bq1.build(), true);

      BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
      bq2.add(DocValuesRangeQuery.newBytesRefRange("dv2", toSortableBytes(min), toSortableBytes(max), true, true), Occur.FILTER);
      bq2.add(new TermQuery(new Term("f", "a")), Occur.MUST);

      assertSameMatches(searcher, ref.build(), bq2.build(), true);
    }

    reader.close();
    dir.close();
  }

  private void assertSameMatches(IndexSearcher searcher, Query q1, Query q2, boolean scores) throws IOException {
    final int maxDoc = searcher.getIndexReader().maxDoc();
    final TopDocs td1 = searcher.search(q1, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    final TopDocs td2 = searcher.search(q2, maxDoc, scores ? Sort.RELEVANCE : Sort.INDEXORDER);
    assertEquals(td1.totalHits, td2.totalHits);
    for (int i = 0; i < td1.scoreDocs.length; ++i) {
      assertEquals(td1.scoreDocs[i].doc, td2.scoreDocs[i].doc);
      if (scores) {
        assertEquals(td1.scoreDocs[i].score, td2.scoreDocs[i].score, 10e-7);
      }
    }
  }

  public void testToString() {
    assertEquals("f:[2 TO 5]", DocValuesRangeQuery.newLongRange("f", 2L, 5L, true, true).toString());
    assertEquals("f:{2 TO 5]", DocValuesRangeQuery.newLongRange("f", 2L, 5L, false, true).toString());
    assertEquals("f:{2 TO 5}", DocValuesRangeQuery.newLongRange("f", 2L, 5L, false, false).toString());
    assertEquals("f:{* TO 5}", DocValuesRangeQuery.newLongRange("f", null, 5L, false, false).toString());
    assertEquals("f:[2 TO *}", DocValuesRangeQuery.newLongRange("f", 2L, null, true, false).toString());

    BytesRef min = new BytesRef("a");
    BytesRef max = new BytesRef("b");
    assertEquals("f:[[61] TO [62]]", DocValuesRangeQuery.newBytesRefRange("f", min, max, true, true).toString());
    assertEquals("f:{[61] TO [62]]", DocValuesRangeQuery.newBytesRefRange("f", min, max, false, true).toString());
    assertEquals("f:{[61] TO [62]}", DocValuesRangeQuery.newBytesRefRange("f", min, max, false, false).toString());
    assertEquals("f:{* TO [62]}", DocValuesRangeQuery.newBytesRefRange("f", null, max, false, false).toString());
    assertEquals("f:[[61] TO *}", DocValuesRangeQuery.newBytesRefRange("f", min, null, true, false).toString());
  }

  public void testDocValuesRangeSupportsApproximation() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("dv1", 5L));
    doc.add(new SortedDocValuesField("dv2", toSortableBytes(42L)));
    iw.addDocument(doc);
    iw.commit();
    final IndexReader reader = iw.getReader();
    final LeafReaderContext ctx = reader.leaves().get(0);
    final IndexSearcher searcher = newSearcher(reader);
    iw.close();

    Query q1 = DocValuesRangeQuery.newLongRange("dv1", 0L, 100L, random().nextBoolean(), random().nextBoolean());
    Weight w = searcher.createNormalizedWeight(q1, true);
    Scorer s = w.scorer(ctx);
    assertNotNull(s.twoPhaseIterator());

    Query q2 = DocValuesRangeQuery.newBytesRefRange("dv2", toSortableBytes(0L), toSortableBytes(100L), random().nextBoolean(), random().nextBoolean());
    w = searcher.createNormalizedWeight(q2, true);
    s = w.scorer(ctx);
    assertNotNull(s.twoPhaseIterator());

    reader.close();
    dir.close();
  }

}
