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
    c.getLeafCollector(null).setScorer(new ScoreAndDoc());
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
    ac.setScorer(new ScoreAndDoc());

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
      collector(ScoreMode.COMPLETE_NO_SCORES, ScoreCachingWrappingScorer.class).getLeafCollector(ctx).setScorer(new ScoreAndDoc());
    });

    // no collector needs scores => no caching
    Collector c1 = collector(ScoreMode.COMPLETE_NO_SCORES, ScoreAndDoc.class);
    Collector c2 = collector(ScoreMode.COMPLETE_NO_SCORES, ScoreAndDoc.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new ScoreAndDoc());

    // only one collector needs scores => no caching
    c1 = collector(ScoreMode.COMPLETE, ScoreAndDoc.class);
    c2 = collector(ScoreMode.COMPLETE_NO_SCORES, ScoreAndDoc.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new ScoreAndDoc());

    // several collectors need scores => caching
    c1 = collector(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    c2 = collector(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new ScoreAndDoc());

    reader.close();
    dir.close();
  }
  
  public void testScorerWrappingForTopScores() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    DirectoryReader reader = iw.getReader();
    iw.close();
    final LeafReaderContext ctx = reader.leaves().get(0);
    Collector c1 = collector(ScoreMode.TOP_SCORES, MultiCollector.MinCompetitiveScoreAwareScorable.class);
    Collector c2 = collector(ScoreMode.TOP_SCORES, MultiCollector.MinCompetitiveScoreAwareScorable.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new ScoreAndDoc());
    
    c1 = collector(ScoreMode.TOP_SCORES, ScoreCachingWrappingScorer.class);
    c2 = collector(ScoreMode.COMPLETE, ScoreCachingWrappingScorer.class);
    MultiCollector.wrap(c1, c2).getLeafCollector(ctx).setScorer(new ScoreAndDoc());
    
    reader.close();
    dir.close();
  }
  
  public void testMinCompetitiveScore() throws IOException {
    float[] currentMinScores = new float[3];
    float[] minCompetitiveScore = new float[1];
    Scorable scorer = new Scorable() {
      
      @Override
      public float score() throws IOException {
        return 0;
      }
      
      @Override
      public int docID() {
        return 0;
      }
      
      @Override
      public void setMinCompetitiveScore(float minScore) throws IOException {
        minCompetitiveScore[0] = minScore;
      }
    };
    Scorable s0 = new MultiCollector.MinCompetitiveScoreAwareScorable(scorer, 0, currentMinScores);
    Scorable s1 = new MultiCollector.MinCompetitiveScoreAwareScorable(scorer, 1, currentMinScores);
    Scorable s2 = new MultiCollector.MinCompetitiveScoreAwareScorable(scorer, 2, currentMinScores);
    assertEquals(0f, minCompetitiveScore[0], 0);
    s0.setMinCompetitiveScore(0.5f);
    assertEquals(0f, minCompetitiveScore[0], 0);
    s1.setMinCompetitiveScore(0.8f);
    assertEquals(0f, minCompetitiveScore[0], 0);
    s2.setMinCompetitiveScore(0.3f);
    assertEquals(0.3f, minCompetitiveScore[0], 0);
    s2.setMinCompetitiveScore(0.1f);
    assertEquals(0.3f, minCompetitiveScore[0], 0);
    s1.setMinCompetitiveScore(Float.MAX_VALUE);
    assertEquals(0.3f, minCompetitiveScore[0], 0);
    s2.setMinCompetitiveScore(Float.MAX_VALUE);
    assertEquals(0.5f, minCompetitiveScore[0], 0);
    s0.setMinCompetitiveScore(Float.MAX_VALUE);
    assertEquals(Float.MAX_VALUE, minCompetitiveScore[0], 0);
  }
  
  public void testCollectionTermination() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    DirectoryReader reader = iw.getReader();
    iw.close();
    final LeafReaderContext ctx = reader.leaves().get(0);
    DummyCollector c1 = new TerminatingDummyCollector(1, ScoreMode.COMPLETE);
    DummyCollector c2 = new TerminatingDummyCollector(2, ScoreMode.COMPLETE);

    Collector mc = MultiCollector.wrap(c1, c2);
    LeafCollector lc = mc.getLeafCollector(ctx);
    lc.setScorer(new ScoreAndDoc());
    lc.collect(0); // OK
    assertTrue("c1's collect should be called", c1.collectCalled);
    assertTrue("c2's collect should be called", c2.collectCalled);
    c1.collectCalled = false;
    c2.collectCalled = false;
    lc.collect(1); // OK, but c1 should terminate
    assertFalse("c1 should be removed already", c1.collectCalled);
    assertTrue("c2's collect should be called", c2.collectCalled);
    c2.collectCalled = false;
    
    expectThrows(CollectionTerminatedException.class, () -> {
      lc.collect(2);
    });
    assertFalse("c1 should be removed already", c1.collectCalled);
    assertFalse("c2 should be removed already", c2.collectCalled);
    
    reader.close();
    dir.close();
  }
  
  public void testSetScorerOnCollectionTerminationSkipNonCompetitive() throws IOException {
    doTestSetScorerOnCollectionTermination(true);
  }
  
  public void testSetScorerOnCollectionTerminationSkipNoSkips() throws IOException {
    doTestSetScorerOnCollectionTermination(false);
  }
  
  private void doTestSetScorerOnCollectionTermination(boolean allowSkipNonCompetitive) throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    DirectoryReader reader = iw.getReader();
    iw.close();
    final LeafReaderContext ctx = reader.leaves().get(0);
    
    DummyCollector c1 = new TerminatingDummyCollector(1, allowSkipNonCompetitive? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE);
    DummyCollector c2 = new TerminatingDummyCollector(2, allowSkipNonCompetitive? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE);
    
    Collector mc = MultiCollector.wrap(c1, c2);
    LeafCollector lc = mc.getLeafCollector(ctx);
    assertFalse(c1.setScorerCalled);
    assertFalse(c2.setScorerCalled);
    lc.setScorer(new ScoreAndDoc());
    assertTrue(c1.setScorerCalled);
    assertTrue(c2.setScorerCalled);
    c1.setScorerCalled = false;
    c2.setScorerCalled = false;
    lc.collect(0); // OK
    
    lc.setScorer(new ScoreAndDoc());
    assertTrue(c1.setScorerCalled);
    assertTrue(c2.setScorerCalled);
    c1.setScorerCalled = false;
    c2.setScorerCalled = false;
    
    lc.collect(1); // OK, but c1 should terminate
    lc.setScorer(new ScoreAndDoc());
    assertFalse(c1.setScorerCalled);
    assertTrue(c2.setScorerCalled);
    c2.setScorerCalled = false;
    
    expectThrows(CollectionTerminatedException.class, () -> {
      lc.collect(2);
    });
    lc.setScorer(new ScoreAndDoc());
    assertFalse(c1.setScorerCalled);
    assertFalse(c2.setScorerCalled);
    
    reader.close();
    dir.close();
  }
  
  private static class TerminatingDummyCollector extends DummyCollector {
    
    private final int terminateOnDoc;
    private final ScoreMode scoreMode;
    
    public TerminatingDummyCollector(int terminateOnDoc, ScoreMode scoreMode) {
      super();
      this.terminateOnDoc = terminateOnDoc;
      this.scoreMode = scoreMode;
    }
    
    @Override
    public void collect(int doc) throws IOException {
      if (doc == terminateOnDoc) {
        throw new CollectionTerminatedException();
      }
      super.collect(doc);
    }
    
    @Override
    public ScoreMode scoreMode() {
      return scoreMode;
    }
    
  }

}
