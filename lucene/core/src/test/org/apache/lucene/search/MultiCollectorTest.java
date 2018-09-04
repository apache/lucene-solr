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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class MultiCollectorTest extends LuceneTestCase {

  private static class DummyCollector extends SimpleCollector {

    boolean collectCalled = false;
    boolean setNextReaderCalled = false;
    boolean setScorerCalled = false;

    @Override
    public void collect(int doc) throws IOException {
      collectCalled = true;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      setNextReaderCalled = true;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      setScorerCalled = true;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }

  @Test
  public void testNullCollectors() throws Exception {
    // Tests that the collector rejects all null collectors.
    expectThrows(IllegalArgumentException.class, () -> {
      MultiCollector.wrap(null, null);
    });

    // Tests that the collector handles some null collectors well. If it
    // doesn't, an NPE would be thrown.
    Collector c = MultiCollector.wrap(new DummyCollector(), null, new DummyCollector());
    assertTrue(c instanceof MultiCollector);
    final LeafCollector ac = c.getLeafCollector(null);
    ac.collect(1);
    c.getLeafCollector(null);
    c.getLeafCollector(null).setScorer(new FakeScorer());
  }

  @Test
  public void testSingleCollector() throws Exception {
    // Tests that if a single Collector is input, it is returned (and not MultiCollector).
    DummyCollector dc = new DummyCollector();
    assertSame(dc, MultiCollector.wrap(dc));
    assertSame(dc, MultiCollector.wrap(dc, null));
  }
  
  @Test
  public void testCollector() throws Exception {
    // Tests that the collector delegates calls to input collectors properly.

    // Tests that the collector handles some null collectors well. If it
    // doesn't, an NPE would be thrown.
    DummyCollector[] dcs = new DummyCollector[] { new DummyCollector(), new DummyCollector() };
    Collector c = MultiCollector.wrap(dcs);
    LeafCollector ac = c.getLeafCollector(null);
    ac.collect(1);
    ac = c.getLeafCollector(null);
    ac.setScorer(new FakeScorer());

    for (DummyCollector dc : dcs) {
      assertTrue(dc.collectCalled);
      assertTrue(dc.setNextReaderCalled);
      assertTrue(dc.setScorerCalled);
    }

  }

  private static Collector collector(ScoreMode scoreMode, Class<?> expectedScorer) {
    return new Collector() {

      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return new LeafCollector() {

          @Override
          public void setScorer(Scorable scorer) throws IOException {
            while (expectedScorer.equals(scorer.getClass()) == false && scorer instanceof FilterScorable) {
              scorer = ((FilterScorable) scorer).in;
            }
            assertEquals(expectedScorer, scorer.getClass());
          }

          @Override
          public void collect(int doc) throws IOException {}
          
        };
      }

      @Override
      public ScoreMode scoreMode() {
        return scoreMode;
      }
      
    };
  }

  public void testCacheScoresIfNecessary() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    iw.commit();
    DirectoryReader reader = iw.getReader();
    iw.close();
    
    final LeafReaderContext ctx = reader.leaves().get(0);

    expectThrows(AssertionError.class, () -> {
      collector(ScoreMode.COMPLETE_NO_SCORES, ScoreCachingWrappingScorer.class).getLeafCollector(ctx).setScorer(new FakeScorer());
    });

    // no collector needs scores => no caching
    Collector c1 = collector(ScoreMode.COMPLETE_NO_SCORES, FakeScorer.class);
    Collector c2 = collector(ScoreMode.COMPLETE_NO_SCORES, FakeScorer.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new FakeScorer());

    // only one collector needs scores => no caching
    c1 = collector(ScoreMode.COMPLETE, FakeScorer.class);
    c2 = collector(ScoreMode.COMPLETE_NO_SCORES, FakeScorer.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new FakeScorer());

    // several collectors need scores => caching
    c1 = collector(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    c2 = collector(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new FakeScorer());

    reader.close();
    dir.close();
  }
}
