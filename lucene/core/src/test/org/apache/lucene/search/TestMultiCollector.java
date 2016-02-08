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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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

  private static class SetScorerCollector extends FilterCollector {

    private final AtomicBoolean setScorerCalled;

    public SetScorerCollector(Collector in, AtomicBoolean setScorerCalled) {
      super(in);
      this.setScorerCalled = setScorerCalled;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new FilterLeafCollector(super.getLeafCollector(context)) {
        @Override
        public void setScorer(Scorer scorer) throws IOException {
          super.setScorer(scorer);
          setScorerCalled.set(true);
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

  public void testSetScorerAfterCollectionTerminated() throws IOException {
    Collector collector1 = new TotalHitCountCollector();
    Collector collector2 = new TotalHitCountCollector();

    AtomicBoolean setScorerCalled1 = new AtomicBoolean();
    collector1 = new SetScorerCollector(collector1, setScorerCalled1);
    
    AtomicBoolean setScorerCalled2 = new AtomicBoolean();
    collector2 = new SetScorerCollector(collector2, setScorerCalled2);

    collector1 = new TerminateAfterCollector(collector1, 1);
    collector2 = new TerminateAfterCollector(collector2, 2);

    Scorer scorer = new FakeScorer();

    List<Collector> collectors = Arrays.asList(collector1, collector2);
    Collections.shuffle(collectors, random());
    Collector collector = MultiCollector.wrap(collectors);

    LeafCollector leafCollector = collector.getLeafCollector(null);
    leafCollector.setScorer(scorer);
    assertTrue(setScorerCalled1.get());
    assertTrue(setScorerCalled2.get());

    leafCollector.collect(0);
    leafCollector.collect(1);

    setScorerCalled1.set(false);
    setScorerCalled2.set(false);
    leafCollector.setScorer(scorer);
    assertFalse(setScorerCalled1.get());
    assertTrue(setScorerCalled2.get());

    try {
      leafCollector.collect(1);
      fail();
    } catch (CollectionTerminatedException e) {
      // expected
    }

    setScorerCalled1.set(false);
    setScorerCalled2.set(false);
    leafCollector.setScorer(scorer);
    assertFalse(setScorerCalled1.get());
    assertFalse(setScorerCalled2.get());
  }

}
