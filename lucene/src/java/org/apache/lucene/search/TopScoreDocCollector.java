package org.apache.lucene.search;

/**
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

import org.apache.lucene.index.AtomicReaderContext;

/**
 * A {@link Collector} implementation that collects the top-scoring hits,
 * returning them as a {@link TopDocs}. This is used by {@link IndexSearcher} to
 * implement {@link TopDocs}-based search. Hits are sorted by score descending
 * and then (when the scores are tied) docID ascending. When you create an
 * instance of this collector you should know in advance whether documents are
 * going to be collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and
 * {Float#NEGATIVE_INFINITY} are not valid scores.  This
 * collector will not properly collect hits with such
 * scores.
 */
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  // Assumes docs are scored in order.
  private static class InOrderTopScoreDocCollector extends TopScoreDocCollector {
    private InOrderTopScoreDocCollector(int numHits) {
      super(numHits);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();

      // This collector cannot handle these scores:
      assert score != Float.NEGATIVE_INFINITY;
      assert !Float.isNaN(score);

      totalHits++;
      if (score <= pqTop.score) {
        // Since docs are returned in-order (i.e., increasing doc Id), a document
        // with equal score to pqTop.score cannot compete since HitQueue favors
        // documents with lower doc Ids. Therefore reject those docs too.
        return;
      }
      pqTop.doc = doc + docBase;
      pqTop.score = score;
      pqTop = pq.updateTop();
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }
  }
  
  // Assumes docs are scored in order.
  private static class InOrderPagingScoreDocCollector extends TopScoreDocCollector {
    private final ScoreDoc after;
    // this is always after.doc - docBase, to save an add when score == after.score
    private int afterDoc;
    private int collectedHits;

    private InOrderPagingScoreDocCollector(ScoreDoc after, int numHits) {
      super(numHits);
      this.after = after;
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();

      // This collector cannot handle these scores:
      assert score != Float.NEGATIVE_INFINITY;
      assert !Float.isNaN(score);

      totalHits++;
      
      if (score > after.score || (score == after.score && doc <= afterDoc)) {
        // hit was collected on a previous page
        return;
      }
      
      if (score <= pqTop.score) {
        // Since docs are returned in-order (i.e., increasing doc Id), a document
        // with equal score to pqTop.score cannot compete since HitQueue favors
        // documents with lower doc Ids. Therefore reject those docs too.
        return;
      }
      collectedHits++;
      pqTop.doc = doc + docBase;
      pqTop.score = score;
      pqTop = pq.updateTop();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
      super.setNextReader(context);
      afterDoc = after.doc - docBase;
    }

    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }
    
    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null ? new TopDocs(totalHits, new ScoreDoc[0], Float.NaN) : new TopDocs(totalHits, results);
    }
  }

  // Assumes docs are scored out of order.
  private static class OutOfOrderTopScoreDocCollector extends TopScoreDocCollector {
    private OutOfOrderTopScoreDocCollector(int numHits) {
      super(numHits);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();

      // This collector cannot handle NaN
      assert !Float.isNaN(score);

      totalHits++;
      if (score < pqTop.score) {
        // Doesn't compete w/ bottom entry in queue
        return;
      }
      doc += docBase;
      if (score == pqTop.score && doc > pqTop.doc) {
        // Break tie in score by doc ID:
        return;
      }
      pqTop.doc = doc;
      pqTop.score = score;
      pqTop = pq.updateTop();
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
  }
  
  // Assumes docs are scored out of order.
  private static class OutOfOrderPagingScoreDocCollector extends TopScoreDocCollector {
    private final ScoreDoc after;
    // this is always after.doc - docBase, to save an add when score == after.score
    private int afterDoc;
    private int collectedHits;

    private OutOfOrderPagingScoreDocCollector(ScoreDoc after, int numHits) {
      super(numHits);
      this.after = after;
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();

      // This collector cannot handle NaN
      assert !Float.isNaN(score);

      totalHits++;
      if (score > after.score || (score == after.score && doc <= afterDoc)) {
        // hit was collected on a previous page
        return;
      }
      if (score < pqTop.score) {
        // Doesn't compete w/ bottom entry in queue
        return;
      }
      doc += docBase;
      if (score == pqTop.score && doc > pqTop.doc) {
        // Break tie in score by doc ID:
        return;
      }
      collectedHits++;
      pqTop.doc = doc;
      pqTop.score = score;
      pqTop = pq.updateTop();
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }
    
    @Override
    public void setNextReader(AtomicReaderContext context) {
      super.setNextReader(context);
      afterDoc = after.doc - docBase;
    }
    
    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }
    
    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null ? new TopDocs(totalHits, new ScoreDoc[0], Float.NaN) : new TopDocs(totalHits, results);
    }
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect and whether documents are scored in order by the input
   * {@link Scorer} to {@link #setScorer(Scorer)}.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, boolean docsScoredInOrder) {
    return create(numHits, null, docsScoredInOrder);
  }
  
  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect, the bottom of the previous page, and whether documents are scored in order by the input
   * {@link Scorer} to {@link #setScorer(Scorer)}.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, ScoreDoc after, boolean docsScoredInOrder) {
    
    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }
    
    if (docsScoredInOrder) {
      return after == null 
        ? new InOrderTopScoreDocCollector(numHits) 
        : new InOrderPagingScoreDocCollector(after, numHits);
    } else {
      return after == null
        ? new OutOfOrderTopScoreDocCollector(numHits)
        : new OutOfOrderPagingScoreDocCollector(after, numHits);
    }
    
  }
  
  ScoreDoc pqTop;
  int docBase = 0;
  Scorer scorer;
    
  // prevents instantiation
  private TopScoreDocCollector(int numHits) {
    super(new HitQueue(numHits, true));
    // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
    // that at this point top() is already initialized.
    pqTop = pq.top();
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }
    
    // We need to compute maxScore in order to set it in TopDocs. If start == 0,
    // it means the largest element is already in results, use its score as
    // maxScore. Otherwise pop everything else, until the largest element is
    // extracted and use its score as maxScore.
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
  public void setNextReader(AtomicReaderContext context) {
    docBase = context.docBase;
  }
  
  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }
}
