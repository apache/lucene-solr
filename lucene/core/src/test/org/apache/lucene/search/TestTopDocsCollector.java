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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

public class TestTopDocsCollector extends LuceneTestCase {

  private static final class MyTopsDocCollector extends TopDocsCollector<ScoreDoc> {

    private int idx = 0;
    
    public MyTopsDocCollector(int size) {
      super(new HitQueue(size, false));
    }
    
    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      if (results == null) {
        return EMPTY_TOPDOCS;
      }
      
      return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }
    
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int base = context.docBase;
      return new LeafCollector() {
        
        @Override
        public void collect(int doc) {
          ++totalHits;
          pq.insertWithOverflow(new ScoreDoc(doc + base, scores[idx++]));
        }

        @Override
        public void setScorer(Scorable scorer) {
          // Don't do anything. Assign scores in random
        }
      };
    }
    
    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

  }

  // Scores array to be used by MyTopDocsCollector. If it is changed, MAX_SCORE
  // must also change.
  private static final float[] scores = new float[] {
    0.7767749f, 1.7839992f, 8.9925785f, 7.9608946f, 0.07948637f, 2.6356435f, 
    7.4950366f, 7.1490803f, 8.108544f, 4.961808f, 2.2423935f, 7.285586f, 4.6699767f,
    2.9655676f, 6.953706f, 5.383931f, 6.9916306f, 8.365894f, 7.888485f, 8.723962f,
    3.1796896f, 0.39971232f, 1.3077754f, 6.8489285f, 9.17561f, 5.060466f, 7.9793315f,
    8.601509f, 4.1858315f, 0.28146625f
  };
  
  private static final float MAX_SCORE = 9.17561f;
  
  private Directory dir;
  private IndexReader reader;

  private TopDocsCollector<ScoreDoc> doSearch(int numResults) throws IOException {
    Query q = new MatchAllDocsQuery();
    return doSearch(numResults, q);
  }

  private TopDocsCollector<ScoreDoc> doSearch(int numResults, Query q) throws IOException {
    IndexSearcher searcher = newSearcher(reader);
    TopDocsCollector<ScoreDoc> tdc = new MyTopsDocCollector(numResults);
    searcher.search(q, tdc);
    return tdc;
  }

  private TopDocsCollector<ScoreDoc> doSearchWithThreshold(int numResults, int thresHold, Query q, IndexReader indexReader) throws IOException {
    IndexSearcher searcher = new IndexSearcher(indexReader);
    TopDocsCollector<ScoreDoc> tdc = TopScoreDocCollector.create(numResults, thresHold);
    searcher.search(q, tdc);
    return tdc;
  }

  private TopDocs doConcurrentSearchWithThreshold(int numResults, int threshold, Query q, IndexReader indexReader) throws IOException {
    ExecutorService service = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new NamedThreadFactory("TestTopDocsCollector"));
    try {
      IndexSearcher searcher = new IndexSearcher(indexReader, service);

      CollectorManager collectorManager = TopScoreDocCollector.createSharedManager(numResults,
          null, threshold);

      return (TopDocs) searcher.search(q, collectorManager);
    } finally {
      service.shutdown();
    }
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    // populate an index with 30 documents, this should be enough for the test.
    // The documents have no content - the test uses MatchAllDocsQuery().
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 30; i++) {
      writer.addDocument(new Document());
    }
    reader = writer.getReader();
    writer.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    dir = null;
    super.tearDown();
  }
  
  public void testInvalidArguments() throws Exception {
    int numResults = 5;
    TopDocsCollector<ScoreDoc> tdc = doSearch(numResults);
    
    // start < 0
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
      tdc.topDocs(-1);
    });

    assertEquals("Expected value of starting position is between 0 and 5, got -1", exception.getMessage());

    // start == pq.size()
    assertEquals(0, tdc.topDocs(numResults).scoreDocs.length);
    
    // howMany < 0
    exception = expectThrows(IllegalArgumentException.class, () -> {
      tdc.topDocs(0, -1);
    });

    assertEquals("Number of hits requested must be greater than 0 but value was -1", exception.getMessage());
  }
  
  public void testZeroResults() throws Exception {
    TopDocsCollector<ScoreDoc> tdc = new MyTopsDocCollector(5);
    assertEquals(0, tdc.topDocs(0, 1).scoreDocs.length);
  }
  
  public void testFirstResultsPage() throws Exception {
    TopDocsCollector<ScoreDoc> tdc = doSearch(15);
    assertEquals(10, tdc.topDocs(0, 10).scoreDocs.length);
  }
  
  public void testSecondResultsPages() throws Exception {
    TopDocsCollector<ScoreDoc> tdc = doSearch(15);
    // ask for more results than are available
    assertEquals(5, tdc.topDocs(10, 10).scoreDocs.length);
    
    // ask for 5 results (exactly what there should be
    tdc = doSearch(15);
    assertEquals(5, tdc.topDocs(10, 5).scoreDocs.length);
    
    // ask for less results than there are
    tdc = doSearch(15);
    assertEquals(4, tdc.topDocs(10, 4).scoreDocs.length);
  }
  
  public void testGetAllResults() throws Exception {
    TopDocsCollector<ScoreDoc> tdc = doSearch(15);
    assertEquals(15, tdc.topDocs().scoreDocs.length);
  }
  
  public void testGetResultsFromStart() throws Exception {
    TopDocsCollector<ScoreDoc> tdc = doSearch(15);
    // should bring all results
    assertEquals(15, tdc.topDocs(0).scoreDocs.length);
    
    tdc = doSearch(15);
    // get the last 5 only.
    assertEquals(5, tdc.topDocs(10).scoreDocs.length);
  }

  public void testIllegalArguments() throws Exception {
    final TopDocsCollector<ScoreDoc> tdc = doSearch(15);

    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tdc.topDocs(-1);
    });

    assertEquals("Expected value of starting position is between 0 and 15, got -1", expected.getMessage());

    expected = expectThrows(IllegalArgumentException.class, () -> {
      tdc.topDocs(9, -1);
    });

    assertEquals("Number of hits requested must be greater than 0 but value was -1", expected.getMessage());
  }
  
  // This does not test the PQ's correctness, but whether topDocs()
  // implementations return the results in decreasing score order.
  public void testResultsOrder() throws Exception {
    TopDocsCollector<ScoreDoc> tdc = doSearch(15);
    ScoreDoc[] sd = tdc.topDocs().scoreDocs;
    
    assertEquals(MAX_SCORE, sd[0].score, 0f);
    for (int i = 1; i < sd.length; i++) {
      assertTrue(sd[i - 1].score >= sd[i].score);
    }
  }

  private static class ScoreAndDoc extends Scorable {
    int doc = -1;
    float score;
    Float minCompetitiveScore = null;

    @Override
    public void setMinCompetitiveScore(float minCompetitiveScore) {
      this.minCompetitiveScore = minCompetitiveScore;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public float score() throws IOException {
      return score;
    }
  }

  public void testSetMinCompetitiveScore() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(2, reader.leaves().size());
    w.close();

    TopScoreDocCollector collector = TopScoreDocCollector.create(2, null, 1);
    ScoreAndDoc scorer = new ScoreAndDoc();

    LeafCollector leafCollector = collector.getLeafCollector(reader.leaves().get(0));
    leafCollector.setScorer(scorer);
    assertNull(scorer.minCompetitiveScore);

    scorer.doc = 0;
    scorer.score = 1;
    leafCollector.collect(0);
    assertNull(scorer.minCompetitiveScore);

    scorer.doc = 1;
    scorer.score = 2;
    leafCollector.collect(1);
    assertEquals(Math.nextUp(1f), scorer.minCompetitiveScore, 0f);

    scorer.doc = 2;
    scorer.score = 0.5f;
    // Make sure we do not call setMinCompetitiveScore for non-competitive hits
    scorer.minCompetitiveScore = Float.NaN;
    leafCollector.collect(2);
    assertTrue(Float.isNaN(scorer.minCompetitiveScore));

    scorer.doc = 3;
    scorer.score = 4;
    leafCollector.collect(3);
    assertEquals(Math.nextUp(2f), scorer.minCompetitiveScore, 0f);

    // Make sure the min score is set on scorers on new segments
    scorer = new ScoreAndDoc();
    leafCollector = collector.getLeafCollector(reader.leaves().get(1));
    leafCollector.setScorer(scorer);
    assertEquals(Math.nextUp(2f), scorer.minCompetitiveScore, 0f);

    scorer.doc = 0;
    scorer.score = 1;
    leafCollector.collect(0);
    assertEquals(Math.nextUp(2f), scorer.minCompetitiveScore, 0f);

    scorer.doc = 1;
    scorer.score = 3;
    leafCollector.collect(1);
    assertEquals(Math.nextUp(3f), scorer.minCompetitiveScore, 0f);

    reader.close();
    dir.close();
  }

  public void testSharedCountCollectorManager() throws Exception {
    Query q = new MatchAllDocsQuery();
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(2, reader.leaves().size());
    w.close();

    TopDocsCollector collector = doSearchWithThreshold( 5, 10, q, reader);
    TopDocs tdc = doConcurrentSearchWithThreshold(5, 10, q, reader);
    TopDocs tdc2 = collector.topDocs();

    CheckHits.checkEqual(q, tdc.scoreDocs, tdc2.scoreDocs);

    reader.close();
    dir.close();
  }

  public void testTotalHits() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(2, reader.leaves().size());
    w.close();

    for (int totalHitsThreshold = 0; totalHitsThreshold < 20; ++ totalHitsThreshold) {
      TopScoreDocCollector collector = TopScoreDocCollector.create(2, null, totalHitsThreshold);
      ScoreAndDoc scorer = new ScoreAndDoc();

      LeafCollector leafCollector = collector.getLeafCollector(reader.leaves().get(0));
      leafCollector.setScorer(scorer);

      scorer.doc = 0;
      scorer.score = 3;
      leafCollector.collect(0);

      scorer.doc = 1;
      scorer.score = 3;
      leafCollector.collect(1);

      leafCollector = collector.getLeafCollector(reader.leaves().get(1));
      leafCollector.setScorer(scorer);

      scorer.doc = 1;
      scorer.score = 3;
      leafCollector.collect(1);

      scorer.doc = 5;
      scorer.score = 4;
      leafCollector.collect(1);

      TopDocs topDocs = collector.topDocs();
      assertEquals(4, topDocs.totalHits.value);
      assertEquals(totalHitsThreshold < 4, scorer.minCompetitiveScore != null);
      assertEquals(new TotalHits(4, totalHitsThreshold < 4 ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO), topDocs.totalHits);
    }

    reader.close();
    dir.close();
  }

  public void testConcurrentMinScore() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));
    Document doc = new Document();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc, doc, doc, doc, doc));
    w.flush();
    w.addDocuments(Arrays.asList(doc, doc));
    w.flush();
    IndexReader reader = DirectoryReader.open(w);
    assertEquals(3, reader.leaves().size());
    w.close();

    CollectorManager<TopScoreDocCollector, TopDocs> manager =
        TopScoreDocCollector.createSharedManager(2, null, 0);
    TopScoreDocCollector collector = manager.newCollector();
    TopScoreDocCollector collector2 = manager.newCollector();
    assertTrue(collector.minScoreAcc == collector2.minScoreAcc);
    MaxScoreAccumulator minValueChecker = collector.minScoreAcc;
    // force the check of the global minimum score on every round
    minValueChecker.modInterval = 0;

    ScoreAndDoc scorer = new ScoreAndDoc();
    ScoreAndDoc scorer2 = new ScoreAndDoc();

    LeafCollector leafCollector = collector.getLeafCollector(reader.leaves().get(0));
    leafCollector.setScorer(scorer);
    LeafCollector leafCollector2 = collector2.getLeafCollector(reader.leaves().get(1));
    leafCollector2.setScorer(scorer2);

    scorer.doc = 0;
    scorer.score = 3;
    leafCollector.collect(0);
    assertNull(minValueChecker.get());
    assertNull(scorer.minCompetitiveScore);

    scorer2.doc = 0;
    scorer2.score = 6;
    leafCollector2.collect(0);
    assertNull(minValueChecker.get());
    assertNull(scorer2.minCompetitiveScore);

    scorer.doc = 1;
    scorer.score = 2;
    leafCollector.collect(1);
    assertEquals(2f, minValueChecker.get().score, 0f);
    assertEquals(Math.nextUp(2f), scorer.minCompetitiveScore, 0f);
    assertNull(scorer2.minCompetitiveScore);

    scorer2.doc = 1;
    scorer2.score = 9;
    leafCollector2.collect(1);
    assertEquals(6f, minValueChecker.get().score, 0f);
    assertEquals(Math.nextUp(2f), scorer.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(6f), scorer2.minCompetitiveScore, 0f);

    scorer2.doc = 2;
    scorer2.score = 7;
    leafCollector2.collect(2);
    assertEquals(minValueChecker.get().score, 7f, 0f);
    assertEquals(Math.nextUp(2f), scorer.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(7f), scorer2.minCompetitiveScore, 0f);

    scorer2.doc = 3;
    scorer2.score = 1;
    leafCollector2.collect(3);
    assertEquals(minValueChecker.get().score, 7f, 0f);
    assertEquals(Math.nextUp(2f), scorer.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(7f), scorer2.minCompetitiveScore, 0f);

    scorer.doc = 2;
    scorer.score = 10;
    leafCollector.collect(2);
    assertEquals(minValueChecker.get().score, 7f, 0f);
    assertEquals(7f, scorer.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(7f), scorer2.minCompetitiveScore, 0f);

    scorer.doc = 3;
    scorer.score = 11;
    leafCollector.collect(3);
    assertEquals(minValueChecker.get().score, 10, 0f);
    assertEquals(Math.nextUp(10f), scorer.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(7f), scorer2.minCompetitiveScore, 0f);

    TopScoreDocCollector collector3 = manager.newCollector();
    LeafCollector leafCollector3 = collector3.getLeafCollector(reader.leaves().get(2));
    ScoreAndDoc scorer3 = new ScoreAndDoc();
    leafCollector3.setScorer(scorer3);
    assertEquals(Math.nextUp(10f), scorer3.minCompetitiveScore, 0f);

    scorer3.doc = 0;
    scorer3.score = 1f;
    leafCollector3.collect(0);
    assertEquals(10f, minValueChecker.get().score, 0f);
    assertEquals(Math.nextUp(10f), scorer3.minCompetitiveScore, 0f);

    scorer.doc = 4;
    scorer.score = 11;
    leafCollector.collect(4);
    assertEquals(11f, minValueChecker.get().score, 0f);
    assertEquals(Math.nextUp(11f), scorer.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(7f), scorer2.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(10f), scorer3.minCompetitiveScore, 0f);

    scorer3.doc = 1;
    scorer3.score = 2f;
    leafCollector3.collect(1);
    assertEquals(minValueChecker.get().score, 11f, 0f);
    assertEquals(Math.nextUp(11f), scorer.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(7f), scorer2.minCompetitiveScore, 0f);
    assertEquals(Math.nextUp(11f), scorer3.minCompetitiveScore, 0f);


    TopDocs topDocs = manager.reduce(Arrays.asList(collector, collector2, collector3));
    assertEquals(11, topDocs.totalHits.value);
    assertEquals(new TotalHits(11, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), topDocs.totalHits);

    reader.close();
    dir.close();
  }

  public void testRandomMinCompetitiveScore() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
    int numDocs = atLeast(1000);
    for (int i = 0; i < numDocs; ++i) {
      int numAs = 1 + random().nextInt(5);
      int numBs = random().nextFloat() < 0.5f ?  0 : 1 + random().nextInt(5);
      int numCs = random().nextFloat() < 0.1f ?  0 : 1 + random().nextInt(5);
      Document doc = new Document();
      for (int j = 0; j < numAs; ++j) {
        doc.add(new StringField("f", "A", Field.Store.NO));
      }
      for (int j = 0; j < numBs; ++j) {
        doc.add(new StringField("f", "B", Field.Store.NO));
      }
      for (int j = 0; j < numCs; ++j) {
        doc.add(new StringField("f", "C", Field.Store.NO));
      }
      w.addDocument(doc);
    }
    IndexReader indexReader = w.getReader();
    w.close();
    Query[] queries = new Query[]{
        new TermQuery(new Term("f", "A")),
        new TermQuery(new Term("f", "B")),
        new TermQuery(new Term("f", "C")),
        new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f", "A")), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term("f", "B")), BooleanClause.Occur.SHOULD)
            .build()
    };
    for (Query query : queries) {
      TopDocsCollector collector = doSearchWithThreshold(5, 0, query, indexReader);
      TopDocs tdc = doConcurrentSearchWithThreshold(5, 0, query, indexReader);
      TopDocs tdc2 = collector.topDocs();

      assertTrue(tdc.totalHits.value > 0);
      assertTrue(tdc2.totalHits.value > 0);
      CheckHits.checkEqual(query, tdc.scoreDocs, tdc2.scoreDocs);
    }

    indexReader.close();
    dir.close();
  }

  public void testRealisticConcurrentMinimumScore() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    try (LineFileDocs docs = new LineFileDocs(random())) {
      int numDocs = atLeast(100);
      for (int i = 0; i < numDocs; i++) {
        writer.addDocument(docs.nextDoc());
      }
    }

    IndexReader reader = writer.getReader();
    writer.close();

    final IndexSearcher s = newSearcher(reader);
    Terms terms = MultiTerms.getTerms(reader, "body");
    int termCount = 0;
    TermsEnum termsEnum = terms.iterator();
    while(termsEnum.next() != null) {
      termCount++;
    }
    assertTrue(termCount > 0);

    // Target ~10 terms to search:
    double chance = 10.0 / termCount;
    termsEnum = terms.iterator();
    while(termsEnum.next() != null) {
      if (random().nextDouble() <= chance) {
        BytesRef term = BytesRef.deepCopyOf(termsEnum.term());
        Query query = new TermQuery(new Term("body", term));

        TopDocsCollector collector = doSearchWithThreshold(5, 0, query, reader);
        TopDocs tdc = doConcurrentSearchWithThreshold(5, 0, query, reader);
        TopDocs tdc2 = collector.topDocs();

        CheckHits.checkEqual(query, tdc.scoreDocs, tdc2.scoreDocs);
      }
    }

    reader.close();
    dir.close();
  }
}
