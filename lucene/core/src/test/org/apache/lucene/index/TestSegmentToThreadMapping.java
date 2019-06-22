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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.Version;

public class TestSegmentToThreadMapping extends LuceneTestCase {

  public LeafReader dummyIndexReader(final int maxDoc) {
    return new LeafReader() {
      @Override
      public int maxDoc() {
        return maxDoc;
      }

      @Override
      public int numDocs() {
        return maxDoc;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return FieldInfos.EMPTY;
      }

      @Override
      public Bits getLiveDocs() {
        return null;
      }

      @Override
      public Terms terms(String field) throws IOException {
        return null;
      }

      @Override
      public Fields getTermVectors(int doc) {
        return null;
      }

      @Override
      public NumericDocValues getNumericDocValues(String field) {
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) {
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) {
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) {
        return null;
      }

      @Override
      public NumericDocValues getNormValues(String field) {
        return null;
      }

      @Override
      public PointValues getPointValues(String field) {
        return null;
      }

      @Override
      protected void doClose() {
      }

      @Override
      public void document(int doc, StoredFieldVisitor visitor) {
      }

      @Override
      public void checkIntegrity() throws IOException {
      }

      @Override
      public LeafMetaData getMetaData() {
        return new LeafMetaData(Version.LATEST.major, Version.LATEST, null);
      }

      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
  }

  public void testSingleSlice() {
    LeafReader largeSegmentReader = dummyIndexReader(50_000);
    LeafReader firstMediumSegmentReader = dummyIndexReader(30_000);
    LeafReader secondMediumSegmentReader = dummyIndexReader(30__000);
    LeafReader thirdMediumSegmentReader = dummyIndexReader(30_000);
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>();

    leafReaderContexts.add(new LeafReaderContext(largeSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(firstMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(secondMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(thirdMediumSegmentReader));

    IndexSearcher.LeafSlice[] resultSlices = IndexSearcher.slices(leafReaderContexts, 250_000, 5);

    assertTrue(resultSlices.length == 1);

    final LeafReaderContext[] leaves = resultSlices[0].leaves;

    assertTrue(leaves.length == 4);
  }

  public void testSmallSegments() {
    LeafReader firstMediumSegmentReader = dummyIndexReader(10_000);
    LeafReader secondMediumSegmentReader = dummyIndexReader(10_000);
    LeafReader thirdMediumSegmentReader = dummyIndexReader(10_000);
    LeafReader fourthMediumSegmentReader = dummyIndexReader(10_000);
    LeafReader fifthMediumSegmentReader = dummyIndexReader(10_000);
    LeafReader sixthMediumSegmentReader = dummyIndexReader(10_000);
    LeafReader seventhLargeSegmentReader = dummyIndexReader(130_000);
    LeafReader eigthLargeSegmentReader = dummyIndexReader(130_000);
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>();

    leafReaderContexts.add(new LeafReaderContext(firstMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(secondMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(thirdMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(fourthMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(fifthMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(sixthMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(seventhLargeSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(eigthLargeSegmentReader));

    IndexSearcher.LeafSlice[] resultSlices = IndexSearcher.slices(leafReaderContexts, 250_000, 5);

    assertTrue(resultSlices.length == 3);

    final LeafReaderContext[] firstSliceleaves = resultSlices[0].leaves;
    final LeafReaderContext[] secondSliceleaves = resultSlices[1].leaves;
    final LeafReaderContext[] thirdSliceleaves = resultSlices[2].leaves;

    assertTrue(firstSliceleaves.length == 2);
    assertTrue(secondSliceleaves.length == 5);
    assertTrue(thirdSliceleaves.length == 1);
  }

  public void testLargeSlices() {
    LeafReader largeSegmentReader = dummyIndexReader(290_900);
    LeafReader firstMediumSegmentReader = dummyIndexReader(170_000);
    LeafReader secondMediumSegmentReader = dummyIndexReader(170_000);
    LeafReader thirdMediumSegmentReader = dummyIndexReader(170_000);
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>();

    leafReaderContexts.add(new LeafReaderContext(largeSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(firstMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(secondMediumSegmentReader));
    leafReaderContexts.add(new LeafReaderContext(thirdMediumSegmentReader));

    IndexSearcher.LeafSlice[] resultSlices = IndexSearcher.slices(leafReaderContexts, 250_000, 5);

    assertTrue(resultSlices.length == 3);

    final LeafReaderContext[] firstSliceleaves = resultSlices[0].leaves;
    final LeafReaderContext[] secondSliceleaves = resultSlices[1].leaves;
    final LeafReaderContext[] thirdSliceleaves = resultSlices[2].leaves;

    assertTrue(firstSliceleaves.length == 1);
    assertTrue(secondSliceleaves.length == 2);
    assertTrue(thirdSliceleaves.length == 1);
  }

  public void testIntraSliceDocIDOrder() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    w.addDocument(new Document());
    w.addDocument(new Document());
    w.commit();
    w.addDocument(new Document());
    w.addDocument(new Document());
    w.commit();
    IndexReader r = w.getReader();
    w.close();

    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("TestSegmentToThreadMapping"));
    IndexSearcher s = new IndexSearcher(r, service);
    Query query = new MatchAllDocsQuery();

    s.search(query, Integer.MAX_VALUE);

    IndexSearcher.LeafSlice[] slices = s.getSlices();
    assertNotNull(slices);

    for (IndexSearcher.LeafSlice leafSlice : slices) {
      LeafReaderContext[] leafReaderContexts = leafSlice.leaves;
      int previousDocBase = leafReaderContexts[0].docBase;

      for (LeafReaderContext leafReaderContext : leafReaderContexts) {
        assertTrue(previousDocBase <= leafReaderContext.docBase);
        previousDocBase = leafReaderContext.docBase;
      }
    }

    service.shutdown();
    IOUtils.close(r, dir);
  }

  public void testRandom() {
    List<LeafReaderContext> leafReaderContexts = new ArrayList<>();
    int max = 500_000;
    int min = 10_000;
    int numSegments = 1 + random().nextInt(50);

    for (int i = 0; i < numSegments; i++) {
      leafReaderContexts.add(new LeafReaderContext(dummyIndexReader(random().nextInt((max - min) + 1) + min)));
    }

    IndexSearcher.LeafSlice[] resultSlices = IndexSearcher.slices(leafReaderContexts, 250_000, 5);

    assertTrue(resultSlices.length > 0);
  }
}
