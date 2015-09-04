package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestMultiCollector extends LuceneTestCase {

  private static class TerminateAfterCollector extends FilterCollector {
    
    private int count = 0;
    private final int terminateAfter;
    
    public TerminateAfterCollector(Collector in, int terminateAfter) {
      super(in);
      this.terminateAfter = terminateAfter;
    }
    
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      if (count >= terminateAfter) {
        throw new CollectionTerminatedException();
      }
      final LeafCollector in = super.getLeafCollector(context);
      return new FilterLeafCollector(in) {
        @Override
        public void collect(int doc) throws IOException {
          if (count >= terminateAfter) {
            throw new CollectionTerminatedException();
          }
          super.collect(doc);
          count++;
        }
      };
    }
    
  }

  public void testCollectionTerminatedExceptionHandling() throws IOException {
    final int iters = atLeast(3);
    for (int iter = 0; iter < iters; ++iter) {
      Directory dir = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      final int numDocs = TestUtil.nextInt(random(), 100, 1000);
      final Document doc = new Document();
      for (int i = 0; i < numDocs; ++i) {
        w.addDocument(doc);
      }
      final IndexReader reader = w.getReader();
      w.close();
      final IndexSearcher searcher = newSearcher(reader);
      Map<TotalHitCountCollector, Integer> expectedCounts = new HashMap<>();
      List<Collector> collectors = new ArrayList<>();
      final int numCollectors = TestUtil.nextInt(random(), 1, 5);
      for (int i = 0; i < numCollectors; ++i) {
        final int terminateAfter = random().nextInt(numDocs + 10);
        final int expectedCount = terminateAfter > numDocs ? numDocs : terminateAfter;
        TotalHitCountCollector collector = new TotalHitCountCollector();
        expectedCounts.put(collector, expectedCount);
        collectors.add(new TerminateAfterCollector(collector, terminateAfter));
      }
      searcher.search(new MatchAllDocsQuery(), MultiCollector.wrap(collectors));
      for (Map.Entry<TotalHitCountCollector, Integer> expectedCount : expectedCounts.entrySet()) {
        assertEquals(expectedCount.getValue().intValue(), expectedCount.getKey().getTotalHits());
      }
      reader.close();
      dir.close();
    }
  }

}
