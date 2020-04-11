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
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestSortOptimization extends LuceneTestCase {

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
      assertTrue(topDocs.totalHits.value >= topDocs.scoreDocs.length);
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
      assertTrue(topDocs.totalHits.value >= topDocs.scoreDocs.length);
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
      assertTrue(topDocs.totalHits.value >= topDocs.scoreDocs.length);
    }

    writer.close();
    reader.close();
    dir.close();
  }

}
