package org.apache.lucene.index.pruning;

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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;

/**
 * Pruning policy with a search quality parameterized guarantee - configuration
 * of this policy allows to specify two parameters: <b>k</b> and
 * <b>&epsilon;</b> such that:
 * <p>
 * <table border="1">
 * <tr>
 * <td>
 * For any <b>OR</b> query with <b>r</b> terms, the score of each of the top
 * <b>k</b> results in the original index, should be "practically the same" as
 * the score that document in the pruned index: the scores difference should not
 * exceed <b>r * &epsilon;</b>.</td>
 * </tr>
 * </table>
 * <p>
 * See the following paper for more details about this method: <a
 * href="http://portal.acm.org/citation.cfm?id=383958">Static index pruning for
 * information retrieval systems, D. Carmel at al, ACM SIGIR 2001 </a>.
 * <p>
 * The claim of this pruning technique is, quoting from the above paper:
 * <p>
 * <table border="1">
 * <tr>
 * <td>
 * Prune the index in such a way that a human
 * "cannot distinguish the difference" between the results of a search engine
 * whose index is pruned and one whose index is not pruned.</td>
 * </tr>
 * </table>
 * <p>
 * For indexes with a large number of terms this policy might be too slow. In
 * such situations, the uniform pruning approach in
 * {@link CarmelUniformTermPruningPolicy} will be faster, though it might
 * produce inferior search quality, as that policy does not pose a theoretical
 * guarantee on resulted search quality.
 * <p>
 * TODO implement also CarmelTermPruningDeltaTopPolicy
 */
public class CarmelTopKTermPruningPolicy extends TermPruningPolicy {
  
  /**
   * Default number of guaranteed top K scores
   */
  public static final int DEFAULT_TOP_K = 10;
  
  /**
   * Default number of query terms
   */
  public static final int DEFAULT_R = 1;
  
  /**
   * Default largest meaningless score difference
   */
  public static final float DEFAULT_EPSILON = .001f;
  
  private int docsPos = 0;
  private int k;
  private ScoreDoc[] docs = null;
  private IndexSearcher is;
  private boolean noPruningForCurrentTerm;
  private float scoreDelta;
  
  /**
   * Constructor with default parameters
   * 
   * @see #DEFAULT_TOP_K
   * @see #DEFAULT_EPSILON
   * @see #DEFAULT_R
   * @see DefaultSimilarity
   * @see #CarmelTopKTermPruningPolicy(IndexReader, Map, int, float, int, Similarity)
   */
  public CarmelTopKTermPruningPolicy(IndexReader in,
      Map<String,Integer> fieldFlags) {
    this(in, fieldFlags, DEFAULT_TOP_K, DEFAULT_EPSILON, DEFAULT_R, null);
  }
  
  /**
   * Constructor with specific settings
   * 
   * @param in reader for original index
   * @param k number of guaranteed top scores. Each top K results in the pruned
   *          index is either also an original top K result or its original
   *          score is indistinguishable from some original top K result.
   * @param epsilon largest meaningless score difference Results whose scores
   *          difference is smaller or equal to epsilon are considered
   *          indistinguishable.
   * @param r maximal number of terms in a query for which search quaility in
   *          pruned index is guaranteed
   * @param sim similarity to use when selecting top docs fir each index term.
   *          When null, {@link DefaultSimilarity} is used.
   */
  public CarmelTopKTermPruningPolicy(IndexReader in,
      Map<String,Integer> fieldFlags, int k, float epsilon, int r,
      Similarity sim) {
    super(in, fieldFlags);
    this.k = k;
    is = new IndexSearcher(in);
    is.setSimilarity(sim != null ? sim : new DefaultSimilarity());
    scoreDelta = epsilon * r;
  }
  
  // too costly - pass everything at this stage
  @Override
  public boolean pruneTermEnum(TermEnum te) throws IOException {
    return false;
  }
  
  @Override
  public void initPositionsTerm(TermPositions tp, Term t) throws IOException {
    // check if there's any point to prune this term
    int df = in.docFreq(t);
    noPruningForCurrentTerm = (df <= k);
    if (noPruningForCurrentTerm) {
      return;
    }
    // take more results (k2>k), attempting for sufficient results to avoid a
    // second search
    int k2 = Math.min(2 * k, k + 100); // for small k's 2*k will do, but for
    // large ones (1000's) keep overhead
    // smaller
    k2 = Math.min(k2, df); // no more than the potential number of results
    TopScoreDocCollector collector = TopScoreDocCollector.create(k2, true);
    TermQuery tq = new TermQuery(t);
    is.search(tq, collector);
    docs = collector.topDocs().scoreDocs;
    float threshold = docs[k - 1].score - scoreDelta;
    
    int nLast = k2 - 1;
    nLast = Math.min(nLast, docs.length - 1); // protect in case of deleted docs
    if (docs[nLast].score < threshold) {
      // this is the better/faster case - no need to go over docs again - we
      // have top ones
      int n = nLast;
      while (docs[n - 1].score < threshold)
        --n; // n == num-valid-docs == first-invalid-doc
      ScoreDoc[] subset = new ScoreDoc[n];
      System.arraycopy(docs, 0, subset, 0, n);
      docs = subset;
      // sort by doc but only after taking top scores
      Arrays.sort(docs, ByDocComparator.INSTANCE);
    } else {
      // this is the worse case - must go over docs again
      ThresholdCollector thresholdCollector = new ThresholdCollector(threshold);
      is.search(tq, thresholdCollector);
      docs = thresholdCollector.scoreDocs.toArray(new ScoreDoc[0]);
    }
    docsPos = 0;
  }
  
  @Override
  public boolean pruneAllPositions(TermPositions termPositions, Term t)
      throws IOException {
    if (noPruningForCurrentTerm) {
      return false;
    }
    if (docsPos >= docs.length) { // used up all doc id-s
      return true; // skip any remaining docs
    }
    while ((docsPos < docs.length - 1)
        && termPositions.doc() > docs[docsPos].doc) {
      docsPos++;
    }
    if (termPositions.doc() == docs[docsPos].doc) {
      // pass
      docsPos++; // move to next doc id
      return false;
    } else if (termPositions.doc() < docs[docsPos].doc) {
      return true; // skip this one - it's less important
    }
    // should not happen!
    throw new IOException("termPositions.doc > docs[docsPos].doc");
  }
  
  // it probably doesn't make sense to prune term vectors using this method,
  // due to its overhead
  @Override
  public int pruneTermVectorTerms(int docNumber, String field, String[] terms,
      int[] freqs, TermFreqVector tfv) throws IOException {
    return 0;
  }
  
  public static class ByDocComparator implements Comparator<ScoreDoc> {
    public static final ByDocComparator INSTANCE = new ByDocComparator();
    
    public int compare(ScoreDoc o1, ScoreDoc o2) {
      return o1.doc - o2.doc;
    }
  }
  
  @Override
  public int pruneSomePositions(int docNum, int[] positions, Term curTerm) {
    return 0; // this policy either prunes all or none, so nothing to prune here
  }
  
  /**
   * Collect all docs with score >= higher threshold
   */
  private static class ThresholdCollector extends Collector {
    
    private ArrayList<ScoreDoc> scoreDocs = new ArrayList<ScoreDoc>();
    private Scorer scorer;
    private float threshold;
    private int docBase;
    
    public ThresholdCollector(float threshold) {
      this.threshold = threshold;
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }
    
    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();
      if (score >= threshold) {
        scoreDocs.add(new ScoreDoc(docBase + doc, score));
      }
    }
    
    @Override
    public void setNextReader(IndexReader reader, int docBase)
        throws IOException {
      this.docBase = docBase;
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }
    
  }
}
