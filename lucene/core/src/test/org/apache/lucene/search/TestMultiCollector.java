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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import org.junit.Test;

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
        public void setScorer(Scorable scorer) throws IOException {
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

    Scorable scorer = new ScoreAndDoc();

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

    expectThrows(CollectionTerminatedException.class, () -> {
      leafCollector.collect(1);
    });

    setScorerCalled1.set(false);
    setScorerCalled2.set(false);
    leafCollector.setScorer(scorer);
    assertFalse(setScorerCalled1.get());
    assertFalse(setScorerCalled2.get());
  }

  public void testDisablesSetMinScore() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    IndexReader reader = DirectoryReader.open(w);
    w.close();

    Scorable scorer = new Scorable() {
      @Override
      public int docID() {
        throw new UnsupportedOperationException();
      }

      @Override
      public float score() {
        return 0;
      }

      @Override
      public void setMinCompetitiveScore(float minScore) {
        throw new AssertionError();
      }
    };

    Collector collector = new SimpleCollector() {
      private Scorable scorer;
      float minScore = 0;

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.TOP_SCORES;
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        minScore = Math.nextUp(minScore);
        scorer.setMinCompetitiveScore(minScore);
      }
    };
    Collector multiCollector = MultiCollector.wrap(collector, new TotalHitCountCollector());
    LeafCollector leafCollector = multiCollector.getLeafCollector(reader.leaves().get(0));
    leafCollector.setScorer(scorer);
    leafCollector.collect(0); // no exception

    reader.close();
    dir.close();
  }

  public void testDisablesSetMinScoreWithEarlyTermination() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    IndexReader reader = DirectoryReader.open(w);
    w.close();

    Scorable scorer =
        new Scorable() {
          @Override
          public int docID() {
            throw new UnsupportedOperationException();
          }

          @Override
          public float score() {
            return 0;
          }

          @Override
          public void setMinCompetitiveScore(float minScore) {
            throw new AssertionError();
          }
        };

    Collector collector =
        new SimpleCollector() {
          private Scorable scorer;
          float minScore = 0;

          @Override
          public ScoreMode scoreMode() {
            return ScoreMode.TOP_SCORES;
          }

          @Override
          public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
          }

          @Override
          public void collect(int doc) throws IOException {
            minScore = Math.nextUp(minScore);
            scorer.setMinCompetitiveScore(minScore);
          }
        };
    for (int numCol = 1; numCol < 4; numCol++) {
      List<Collector> cols = new ArrayList<>();
      cols.add(collector);
      for (int col = 0; col < numCol; col++) {
        cols.add(new TerminateAfterCollector(new TotalHitCountCollector(), 0));
      }
      Collections.shuffle(cols, random());
      Collector multiCollector = MultiCollector.wrap(cols);
      LeafCollector leafCollector = multiCollector.getLeafCollector(reader.leaves().get(0));
      leafCollector.setScorer(scorer);
      leafCollector.collect(0); // no exception
    }

    reader.close();
    dir.close();
  }

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
