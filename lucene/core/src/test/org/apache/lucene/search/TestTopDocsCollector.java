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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

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
      
      float maxScore = Float.NaN;
      if (start == 0) {
        maxScore = results[0].score;
      } else {
        for (int i = pq.size(); i > 1; i--) { pq.pop(); }
        maxScore = pq.pop().score;
      }
      
      return new TopDocs(totalHits, results, maxScore);
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
        public void setScorer(Scorer scorer) {
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
    IndexSearcher searcher = newSearcher(reader);
    TopDocsCollector<ScoreDoc> tdc = new MyTopsDocCollector(numResults);
    searcher.search(q, tdc);
    return tdc;
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
    assertEquals(0, tdc.topDocs(-1).scoreDocs.length);
    
    // start > pq.size()
    assertEquals(0, tdc.topDocs(numResults + 1).scoreDocs.length);
    
    // start == pq.size()
    assertEquals(0, tdc.topDocs(numResults).scoreDocs.length);
    
    // howMany < 0
    assertEquals(0, tdc.topDocs(0, -1).scoreDocs.length);
    
    // howMany == 0
    assertEquals(0, tdc.topDocs(0, 0).scoreDocs.length);
    
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
  
  public void testMaxScore() throws Exception {
    // ask for all results
    TopDocsCollector<ScoreDoc> tdc = doSearch(15);
    TopDocs td = tdc.topDocs();
    assertEquals(MAX_SCORE, td.getMaxScore(), 0f);
    
    // ask for 5 last results
    tdc = doSearch(15);
    td = tdc.topDocs(10);
    assertEquals(MAX_SCORE, td.getMaxScore(), 0f);
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

  private static class FakeScorer extends Scorer {
    int doc = -1;
    float score;
    Float minCompetitiveScore = null;

    FakeScorer() {
      super(null);
    }

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

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
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

    TopScoreDocCollector collector = TopScoreDocCollector.create(2, null, false);
    FakeScorer scorer = new FakeScorer();

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
    scorer = new FakeScorer();
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

  public void testEstimateHitCount() throws Exception {
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

    TopScoreDocCollector collector = TopScoreDocCollector.create(2, null, false);
    FakeScorer scorer = new FakeScorer();

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

    TopDocs topDocs = collector.topDocs();
    // It assumes all docs matched since numHits was 2 and the first 2 collected docs matched
    assertEquals(10, topDocs.totalHits);

    // Now test an index that is more sparsely collected
    collector = TopScoreDocCollector.create(2, null, false);

    leafCollector = collector.getLeafCollector(reader.leaves().get(0));
    leafCollector.setScorer(scorer);

    scorer.doc = 1;
    scorer.score = 3;
    leafCollector.collect(1);

    leafCollector = collector.getLeafCollector(reader.leaves().get(1));
    leafCollector.setScorer(scorer);

    scorer.doc = 0;
    scorer.score = 2;
    leafCollector.collect(0);

    scorer.doc = 2;
    scorer.score = 5;
    leafCollector.collect(2);

    topDocs = collector.topDocs();
    assertEquals(4, topDocs.totalHits);

    // Same 2 first collected docs, but then we collect more docs to make sure
    // that we use the actual number of collected docs as a lower bound
    collector = TopScoreDocCollector.create(2, null, false);

    leafCollector = collector.getLeafCollector(reader.leaves().get(0));
    leafCollector.setScorer(scorer);

    scorer.doc = 1;
    scorer.score = 3;
    leafCollector.collect(1);

    leafCollector = collector.getLeafCollector(reader.leaves().get(1));
    leafCollector.setScorer(scorer);

    scorer.doc = 0;
    scorer.score = 2;
    leafCollector.collect(0);

    scorer.doc = 2;
    scorer.score = 5;
    leafCollector.collect(2);

    scorer.doc = 3;
    scorer.score = 4;
    leafCollector.collect(3);

    scorer.doc = 4;
    scorer.score = 1;
    leafCollector.collect(4);

    topDocs = collector.topDocs();
    assertEquals(5, topDocs.totalHits);

    reader.close();
    dir.close();
  }

}
