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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

public class TestSortOptimization extends LuceneTestCase {

  private Directory dir;
  private RandomIndexWriter writer;
  private IndexReader reader;
  private IndexSearcher searcher;
  private int numDocs;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    writer = new RandomIndexWriter(random(), dir);
    numDocs = atLeast(15);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = new Document();
      doc.add(new NumericDocValuesField("my_field", i));
      doc.add(new LongPoint("my_field", i));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    searcher.setQueryCachingPolicy(NEVER_CACHE);
    writer.close();
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(reader, writer, dir);
    super.tearDown();
  }

  public void testSortOptimization() throws IOException {
    final Sort sort = new Sort(new LongDocValuesPointSortField("my_field"));
    final int numHits = 3;
    final int totalHitsThreshold = 3;
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

  // In this case optimization for reverse sort doesn't make sense, because
  // as docs are processed in order of id, and docs with a smaller id have smaller values,
  // we will have to iterater over all documents to get competitive values
  public void testSortReverseOptimization() throws IOException {
    final Sort sort = new Sort(new LongDocValuesPointSortField("my_field", true));
    final int numHits = 3;
    final int totalHitsThreshold = 3;
    final TopFieldCollector collector = TopFieldCollector.create(sort, numHits, null, totalHitsThreshold);
    searcher.search(new MatchAllDocsQuery(), collector);
    TopDocs topDocs = collector.topDocs();
    assertEquals(topDocs.scoreDocs.length, numHits);
    for (int i = 0; i < numHits; i++) {
      FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[i];
      int fieldValue = ((Long) fieldDoc.fields[0]).intValue();
      assertEquals(numDocs - i - 1, fieldValue);
    }
    assertTrue(collector.isEarlyTerminated());
    assertTrue(topDocs.totalHits.value >= topDocs.scoreDocs.length);
  }


  private static final QueryCachingPolicy NEVER_CACHE = new QueryCachingPolicy() {
    @Override
    public void onUse(Query query) {}
    @Override
    public boolean shouldCache(Query query) {
      return false;
    }
  };
}
