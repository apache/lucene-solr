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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.search.SortField.FIELD_DOC;
import static org.apache.lucene.search.SortField.FIELD_SCORE;

public class TestFieldSortOptimizationSkipping extends LuceneTestCase {

  public void testLongSortOptimization() throws IOException {

    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new NumericDocValuesField("my_field", i));
      doc.add(new LongPoint("my_field", i));
      writer.addDocument(doc);
      if (i == 7000) writer.flush(); // two segments
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    final SortField sortField = new SortField("my_field", SortField.Type.LONG);
    final Sort sort = new Sort(sortField);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // simple sort
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(i, ((Long) fieldDoc.fields[0]).intValue());
      }
      assertTrue(collector.isEarlyTerminated());
      assertTrue(topDocs.totalHits.value < numDocs);
    }

    { // paging sort with after
      long afterValue = 2;
      FieldDoc after = new FieldDoc(2, Float.NaN, new Long[] {afterValue});
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, after, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(afterValue + 1 + i, fieldDoc.fields[0]);
      }
      assertTrue(collector.isEarlyTerminated());
      assertTrue(topDocs.totalHits.value < numDocs);
    }

    { // test that if there is the secondary sort on _score, scores are filled correctly
      final TopFieldCollector collector = TopFieldCollector.create(new Sort(sortField, FIELD_SCORE), numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(i, ((Long) fieldDoc.fields[0]).intValue());
        float score = (float) fieldDoc.fields[1];
        assertEquals(1.0, score, 0.001);
      }
      assertTrue(collector.isEarlyTerminated());
      assertTrue(topDocs.totalHits.value < numDocs);
    }

    { // test that if numeric field is a secondary sort, no optimization is run
      final TopFieldCollector collector = TopFieldCollector.create(new Sort(FIELD_SCORE, sortField), numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(topDocs.totalHits.value, numDocs); // assert that all documents were collected => optimization was not run
    }

    reader.close();
    dir.close();
  }
  
  /**
   * test that even if a field is not indexed with points, optimized sort still works as expected,
   * although no optimization will be run
   */
  public void testLongSortOptimizationOnFieldNotIndexedWithPoints() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(100);
    // my_field is not indexed with points
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new NumericDocValuesField("my_field", i));
      writer.addDocument(doc);
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    final SortField sortField = new SortField("my_field", SortField.Type.LONG);
    final Sort sort = new Sort(sortField);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
    searcher.search(new MatchAllDocsQuery(), collector);
    TopDocs topDocs = collector.topDocs();
    assertEquals(topDocs.scoreDocs.length, numHits);  // sort still works and returns expected number of docs
    for (int i = 0; i < numHits; i++) {
      FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
      assertEquals(i, ((Long) fieldDoc.fields[0]).intValue()); // returns expected values
    }
    assertEquals(topDocs.totalHits.value, numDocs); // assert that all documents were collected => optimization was not run

    reader.close();
    dir.close();
  }


  public void testSortOptimizationWithMissingValues() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      if ((i % 500) != 0) { // miss values on every 500th document
        doc.add(new NumericDocValuesField("my_field", i));
        doc.add(new LongPoint("my_field", i));
      }
      writer.addDocument(doc);
      if (i == 7000) writer.flush(); // two segments
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // test that optimization is not run when missing value setting of SortField is competitive
      final SortField sortField = new SortField("my_field", SortField.Type.LONG);
      sortField.setMissingValue(0L); // set a competitive missing value
      final Sort sort = new Sort(sortField);
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(topDocs.totalHits.value, numDocs); // assert that all documents were collected => optimization was not run
    }
    { // test that optimization is run when missing value setting of SortField is NOT competitive
      final SortField sortField = new SortField("my_field", SortField.Type.LONG);
      sortField.setMissingValue(100L); // set a NON competitive missing value
      final Sort sort = new Sort(sortField);
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertTrue(topDocs.totalHits.value < numDocs); // assert that some docs were skipped => optimization was run
    }

    reader.close();
    dir.close();
  }

  public void testSortOptimizationEqualValues() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(10000);
    for (int i = 1; i <= numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new NumericDocValuesField("my_field1", 100)); // all docs have the same value of my_field1
      doc.add(new IntPoint("my_field1", 100));
      doc.add(new NumericDocValuesField("my_field2", numDocs - i)); // diff values for the field my_field2
      writer.addDocument(doc);
      if (i == 7000) writer.flush(); // two segments
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // test that sorting on a single field with equal values uses the optimization
      final SortField sortField = new SortField("my_field1", SortField.Type.INT);
      final Sort sort = new Sort(sortField);
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(100, fieldDoc.fields[0]);
      }
      assertTrue(topDocs.totalHits.value < numDocs); // assert that some docs were skipped => optimization was run
    }

    { // test that sorting on a single field with equal values and after parameter uses the optimization
      final int afterValue = 100;
      final SortField sortField = new SortField("my_field1", SortField.Type.INT);
      final Sort sort = new Sort(sortField);
      FieldDoc after = new FieldDoc(10, Float.NaN, new Integer[] {afterValue});
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, after, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(100, fieldDoc.fields[0]);
      }
      assertTrue(topDocs.totalHits.value < numDocs); // assert that some docs were skipped => optimization was run
    }

    { // test that sorting on main field with equal values + another field for tie breaks doesn't use optimization
      final SortField sortField1 = new SortField("my_field1", SortField.Type.INT);
      final SortField sortField2 = new SortField("my_field2", SortField.Type.INT);
      final Sort sort = new Sort(sortField1, sortField2);
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(100, fieldDoc.fields[0]); // sort on 1st field as expected
        assertEquals(i, fieldDoc.fields[1]); // sort on 2nd field as expected
      }
      assertEquals(topDocs.scoreDocs.length, numHits);
      assertEquals(topDocs.totalHits.value, numDocs); // assert that all documents were collected => optimization was not run
    }

    reader.close();
    dir.close();
  }


  public void testFloatSortOptimization() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(10000);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      float f = 1f * i;
      doc.add(new FloatDocValuesField("my_field", f));
      doc.add(new FloatPoint("my_field", i));
      writer.addDocument(doc);
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    final SortField sortField = new SortField("my_field", SortField.Type.FLOAT);
    final Sort sort = new Sort(sortField);
    final int numHits = 3;
    final int totalHitsThreshold = 3;

    { // simple sort
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(topDocs.scoreDocs.length, numHits);
      for (int i = 0; i < numHits; i++) {
        FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
        assertEquals(1f * i, fieldDoc.fields[0]);
      }
      assertTrue(collector.isEarlyTerminated());
      assertTrue(topDocs.totalHits.value < numDocs);
    }

    reader.close();
    dir.close();
  }

  public void testDocSortOptimizationWithAfter() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(150);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      writer.addDocument(doc);
      if ((i > 0) && (i % 50 == 0)) {
        writer.flush();
      }
    }

    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();
    IndexSearcher searcher = newSearcher(reader);
    final int numHits = 10;
    final int totalHitsThreshold = 10;
    final int[] searchAfters = {3, 10, numDocs - 10};
    for (int searchAfter : searchAfters) {
      // sort by _doc with search after should trigger optimization
      {
        final Sort sort = new Sort(FIELD_DOC);
        FieldDoc after = new FieldDoc(searchAfter, Float.NaN, new Integer[]{searchAfter});
        final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, after, totalHitsThreshold);
        searcher.search(new MatchAllDocsQuery(), collector);
        TopDocs topDocs = collector.topDocs();
        int expNumHits = (searchAfter >= (numDocs - numHits)) ? (numDocs - searchAfter - 1) : numHits;
        assertEquals(expNumHits, topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          int expectedDocID = searchAfter + 1 + i;
          assertEquals(expectedDocID, topDocs.scoreDocs[i].doc);
        }
        assertTrue(collector.isEarlyTerminated());
        // check that very few docs were collected
        assertTrue(topDocs.totalHits.value < numDocs);
      }

      // sort by _doc + _score with search after should trigger optimization
      {
        final Sort sort = new Sort(FIELD_DOC, FIELD_SCORE);
        FieldDoc after = new FieldDoc(searchAfter, Float.NaN, new Object[]{searchAfter, 1.0f});
        final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, after, totalHitsThreshold);
        searcher.search(new MatchAllDocsQuery(), collector);
        TopDocs topDocs = collector.topDocs();
        int expNumHits = (searchAfter >= (numDocs - numHits)) ? (numDocs - searchAfter - 1) : numHits;
        assertEquals(expNumHits, topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          int expectedDocID = searchAfter + 1 + i;
          assertEquals(expectedDocID, topDocs.scoreDocs[i].doc);
        }
        assertTrue(collector.isEarlyTerminated());
        // assert that very few docs were collected
        assertTrue(topDocs.totalHits.value < numDocs);
      }

      // sort by _doc desc should not trigger optimization
      {
        final Sort sort = new Sort(new SortField(null, SortField.Type.DOC, true));
        FieldDoc after = new FieldDoc(searchAfter, Float.NaN, new Integer[]{searchAfter});
        final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, after, totalHitsThreshold);
        searcher.search(new MatchAllDocsQuery(), collector);
        TopDocs topDocs = collector.topDocs();
        int expNumHits = (searchAfter < numHits) ? searchAfter : numHits;
        assertEquals(expNumHits, topDocs.scoreDocs.length);
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
          int expectedDocID = searchAfter - 1 - i;
          assertEquals(expectedDocID, topDocs.scoreDocs[i].doc);
        }
        // assert that all documents were collected
        assertEquals(numDocs, topDocs.totalHits.value);
      }
    }

    reader.close();
    dir.close();
  }


  public void testDocSortOptimization() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = atLeast(100);
    int seg = 1;
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new LongPoint("lf", i));
      doc.add(new StoredField("slf", i));
      doc.add(new StringField("tf", "seg" + seg, Field.Store.YES));
      writer.addDocument(doc);
      if ((i > 0) && (i % 50 == 0)) {
        writer.flush();
        seg++;
      }
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();

    final int numHits = 3;
    final int totalHitsThreshold = 3;
    final Sort sort = new Sort(FIELD_DOC);

    // sort by _doc should skip all non-competitive documents
    {
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      IndexSearcher searcher = newSearcher(reader);
      searcher.search(new MatchAllDocsQuery(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(numHits, topDocs.scoreDocs.length);
      for (int i = 0; i < numHits; i++) {
        assertEquals(i, topDocs.scoreDocs[i].doc);
      }
      assertTrue(collector.isEarlyTerminated());
      assertTrue(topDocs.totalHits.value < 10); // assert that very few docs were collected
    }

    // sort by _doc with a bool query should skip all non-competitive documents
    {
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      int lowerRange = 40;
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(LongPoint.newRangeQuery("lf", lowerRange, Long.MAX_VALUE), BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("tf", "seg1")), BooleanClause.Occur.MUST);
      IndexSearcher searcher = newSearcher(reader);
      searcher.search(bq.build(), collector);

      TopDocs topDocs = collector.topDocs();
      assertEquals(numHits, topDocs.scoreDocs.length);
      for (int i = 0; i < numHits; i++) {
        Document d = searcher.doc(topDocs.scoreDocs[i].doc);
        assertEquals(Integer.toString(i + lowerRange), d.get("slf"));
        assertEquals("seg1", d.get("tf"));
      }
      assertTrue(collector.isEarlyTerminated());
      assertTrue(topDocs.totalHits.value < 10); // assert that very few docs were collected
    }

    reader.close();
    dir.close();
  }

  /**
   * Test that sorting on _doc works correctly.
   * This test goes through DefaultBulkSorter::scoreRange, where scorerIterator is BitSetIterator.
   * As a conjunction of this BitSetIterator with DocComparator's iterator, we get BitSetConjunctionDISI.
   * BitSetConjuctionDISI advances based on the DocComparator's iterator, and doesn't consider
   * that its BitSetIterator may have advanced passed a certain doc. 
   */
  public void testDocSort() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
    final int numDocs = 4;
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new StringField("id", "id" + i, Field.Store.NO));
      if (i < 2) {
        doc.add(new LongPoint("lf", 1));
      }
      writer.addDocument(doc);
    }
    final IndexReader reader = DirectoryReader.open(writer);
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    searcher.setQueryCache(null);
    final int numHits = 10;
    final int totalHitsThreshold = 10;
    final Sort sort = new Sort(FIELD_DOC);

    {
      final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(LongPoint.newExactQuery("lf", 1), BooleanClause.Occur.MUST);
      bq.add(new TermQuery(new Term("id", "id3")), BooleanClause.Occur.MUST_NOT);
      searcher.search(bq.build(), collector);
      TopDocs topDocs = collector.topDocs();
      assertEquals(2, topDocs.scoreDocs.length);
    }

    reader.close();
    dir.close();
  }

  public void testNumericSortOptimizationIndexSort() throws IOException {
    boolean reverseSort = randomBoolean();
    final SortField sortField = new SortField("field1", SortField.Type.LONG, reverseSort);
    Sort indexSort = new Sort(sortField);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setIndexSort(indexSort);

    try (
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, iwc)
    ) {
      final int numDocs = atLeast(50);
      int[] sortedValues = initializeNumericValues(numDocs, reverseSort, 0);
      int[] randomIdxs = randomIdxs(numDocs);
      for (int i = 0; i < numDocs; i++) {
        final Document doc = new Document();
        if (sortedValues[randomIdxs[i]] > 0) {
          doc.add(new NumericDocValuesField("field1", sortedValues[randomIdxs[i]]));
        }
        writer.addDocument(doc);
        if (i == 30) {
          writer.flush();
        }
      }
      try (IndexReader reader = DirectoryReader.open(writer)) {
        IndexSearcher searcher = newSearcher(reader);
        final int numHits = randomIntBetween(1, numDocs - 10);
        final int totalHitsThreshold = randomIntBetween(1, numDocs - 10);
        // test that optimization is run when search sort is equal to the index sort
        TopFieldCollector collector = TopFieldCollector.create(indexSort, numHits, null, totalHitsThreshold);
        searcher.search(new MatchAllDocsQuery(), collector);
        TopDocs topDocs = collector.topDocs();
        assertTrue(collector.isEarlyTerminated());
        assertEquals(topDocs.scoreDocs.length, numHits);
        assertTrue(topDocs.totalHits.value < numDocs);
        for (int i = 0; i < numHits; i++) {
          assertEquals(sortedValues[i], ((Long) ((FieldDoc) topDocs.scoreDocs[i]).fields[0]).intValue());
        }

        // test that search_after works correctly
        int afterIdx = randomIntBetween(0, numHits - 1);
        FieldDoc after = (FieldDoc) topDocs.scoreDocs[afterIdx];
        final int numHits2 = randomIntBetween(1, 5);
        final int totalHitsThreshold2 = randomIntBetween(1, 5);
        collector = TopFieldCollector.create(indexSort, numHits2, after, totalHitsThreshold2);
        searcher.search(new MatchAllDocsQuery(), collector);
        topDocs = collector.topDocs();
        assertEquals(topDocs.scoreDocs.length, numHits2);
        for (int i = 0; i < numHits2; i++) {
          assertEquals(sortedValues[afterIdx + 1 + i], ((Long) ((FieldDoc) topDocs.scoreDocs[i]).fields[0]).intValue());
        }
      }
    }
  }

  public void testNumericSortOptimizationMultipleFieldsIndexSort() throws IOException {
    boolean reverseSort = randomBoolean();
    final SortField sortField = new SortField("field1", SortField.Type.LONG, reverseSort);
    final SortField sortField2 = new SortField("field2", SortField.Type.INT, reverseSort);
    Sort indexSort = new Sort(sortField, sortField2);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setIndexSort(indexSort);

    try (
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, iwc)
    ) {
      final int numDocs = atLeast(50);
      for (int i = 0; i < numDocs; i++) {
        final Document doc = new Document();
        if (i % 10 != 0) { // to have some missing values
          doc.add(new NumericDocValuesField("field1", randomIntBetween(0, numDocs)));
          doc.add(new NumericDocValuesField("field2", randomIntBetween(0, numDocs)));
        }
        writer.addDocument(doc);
        if (i == 30) {
          writer.flush();
        }
      }
      try (IndexReader reader = DirectoryReader.open(writer)) {
        IndexSearcher searcher = newSearcher(reader);
        final int numHits = randomIntBetween(1, numDocs - 10);
        final int totalHitsThreshold = randomIntBetween(1, numDocs - 10);
        {
          // test that optimization is run when search sort is equal to the index sort
          TopFieldCollector collector = TopFieldCollector.create(indexSort, numHits, null, totalHitsThreshold);
          searcher.search(new MatchAllDocsQuery(), collector);
          TopDocs topDocs = collector.topDocs();
          assertTrue(collector.isEarlyTerminated());
          assertEquals(topDocs.scoreDocs.length, numHits);
          assertTrue(topDocs.totalHits.value < numDocs);
        }

        {
          // test that optimization is run when search sort is a starting part of the index sort
          Sort searchSort = new Sort(sortField);
          TopFieldCollector collector = TopFieldCollector.create(searchSort, numHits, null, totalHitsThreshold);
          searcher.search(new MatchAllDocsQuery(), collector);
          TopDocs topDocs = collector.topDocs();
          assertTrue(collector.isEarlyTerminated());
          assertEquals(topDocs.scoreDocs.length, numHits);
          assertTrue(topDocs.totalHits.value < numDocs);
        }

        {
          // test that optimization is NOT run when search sort is NOT a starting part of the index sort
          Sort searchSort = new Sort(sortField2);
          TopFieldCollector collector = TopFieldCollector.create(searchSort, numHits, null, totalHitsThreshold);
          searcher.search(new MatchAllDocsQuery(), collector);
          TopDocs topDocs = collector.topDocs();
          assertEquals(topDocs.scoreDocs.length, numHits);
          assertTrue(topDocs.totalHits.value == numDocs); // assert that all docs were collected
        }
      }
    }
  }

  public void testTermOrdValIndexSort() throws IOException {
    boolean reverseSort = randomBoolean();
    final SortField sortField = new SortField("field1", SortField.Type.STRING, reverseSort);
    sortField.setMissingValue(SortField.STRING_LAST);
    Sort indexSort = new Sort(sortField);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setIndexSort(indexSort);

    try (
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, iwc)
    ) {
      final int numDocs = atLeast(50);
      BytesRef[] sortedValues = initializeStringValues(numDocs, reverseSort);
      int[] randomIdxs = randomIdxs(numDocs);
      for (int i = 0; i < numDocs; i++) {
        final Document doc = new Document();
        if (sortedValues[randomIdxs[i]] != null) {
          doc.add(new SortedDocValuesField("field1", sortedValues[randomIdxs[i]]));
        }
        writer.addDocument(doc);
        if (i == 30) {
          writer.flush();
        }
      }
      try (IndexReader reader = DirectoryReader.open(writer)) {
        IndexSearcher searcher = newSearcher(reader);
        final int numHits = randomIntBetween(1, numDocs - 10);
        final int totalHitsThreshold = randomIntBetween(1, numDocs - 10);
        // test that optimization is run when search sort is equal to the index sort
        TopFieldCollector collector = TopFieldCollector.create(indexSort, numHits, null, totalHitsThreshold);
        searcher.search(new MatchAllDocsQuery(), collector);
        TopDocs topDocs = collector.topDocs();
        assertTrue(collector.isEarlyTerminated());
        assertEquals(topDocs.scoreDocs.length, numHits);
        assertTrue(topDocs.totalHits.value < numDocs);
        for (int i = 0; i < numHits; i++) {
          assertEquals(sortedValues[i], ((FieldDoc) topDocs.scoreDocs[i]).fields[0]);
        }

        // test that search_after works correctly
        int afterIdx = randomIntBetween(0, numHits - 1);
        FieldDoc after = (FieldDoc) topDocs.scoreDocs[afterIdx];
        final int numHits2 = randomIntBetween(1, 5);
        final int totalHitsThreshold2 = randomIntBetween(1, 5);
        collector = TopFieldCollector.create(indexSort, numHits2, after, totalHitsThreshold2);
        searcher.search(new MatchAllDocsQuery(), collector);
        topDocs = collector.topDocs();
        assertEquals(topDocs.scoreDocs.length, numHits2);
        for (int i = 0; i < numHits2; i++) {
          assertEquals(sortedValues[afterIdx + 1 + i], ((FieldDoc) topDocs.scoreDocs[i]).fields[0]);
        }
      }
    }
  }

  // initialize sorted values with some 0 (missing) and repeated values
  private static int[] initializeNumericValues(int numDocs, boolean reverseSort, int missingValue) {
    int[] values = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      if (randomBoolean() && i > 0) {
        values[i] = values[i - 1]; // repeat some values
      } else {
        if (i % 10 == 0) {
          values[i] = missingValue;
        } else {
          values[i] = randomIntBetween(1, numDocs);
        }
      }
    }
    if (reverseSort) {
      for (int i = 0; i < numDocs; i++) {
        values[i] = values[i] * -1;
      }
      Arrays.sort(values);
      for (int i = 0; i < numDocs; i++) {
        values[i] = values[i] * -1;
      }
    } else {
      Arrays.sort(values);
    }
    return values;
  }

  private static BytesRef[] initializeStringValues(int numDocs, boolean reverseSort) {
    BytesRef[] values = new BytesRef[numDocs];
    for (int i = 0; i < numDocs; i++) {
      if (randomBoolean() && i > 0) {
        values[i] = values[i - 1]; // repeat some values
      } else {
        if (i % 10 == 0) {
          values[i] = null; // missing value
        } else {
          values[i] = new BytesRef("id" + (100 + randomIntBetween(1, numDocs)));
        }
      }
    }
    if (reverseSort) {
      Arrays.sort(values, Comparator.nullsFirst(Comparator.reverseOrder()));
    } else {
      Arrays.sort(values, Comparator.nullsLast(Comparator.naturalOrder()));
    }
    return values;
  }

  // randomize array indexes
  private static int[] randomIdxs(int numDocs) {
    int[] randomIdxs = new int[numDocs];
    for (int i = 0; i < numDocs; i++) {
      randomIdxs[i] = i;
    }
    for (int i = 0; i < numDocs; i++) {
      int change = randomIntBetween(i, numDocs - 1);
      int tmp = randomIdxs[i];
      randomIdxs[i] = randomIdxs[change];
      randomIdxs[change] = tmp;
    }
    return randomIdxs;
  }
}
