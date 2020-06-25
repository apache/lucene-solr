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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

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
    IndexSearcher searcher = new IndexSearcher(reader);
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

    writer.close();
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
    IndexSearcher searcher = new IndexSearcher(reader);
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

    writer.close();
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
    IndexSearcher searcher = new IndexSearcher(reader);
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

    writer.close();
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
    IndexSearcher searcher = new IndexSearcher(reader);
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

    writer.close();
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
    IndexSearcher searcher = new IndexSearcher(reader);
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

    writer.close();
    reader.close();
    dir.close();
  }


}
