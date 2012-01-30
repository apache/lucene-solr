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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;

/**
 * Enhanced implementation of Carmel Uniform Pruning,
 * <p>
 * {@link TermPositions} whose in-document frequency is below a specified
 * threshold
 * <p>
 * See {@link CarmelTopKTermPruningPolicy} for link to the paper describing this
 * policy. are pruned.
 * <p>
 * Conclusions of that paper indicate that it's best to compute per-term
 * thresholds, as we do in {@link CarmelTopKTermPruningPolicy}. However for
 * large indexes with a large number of terms that method might be too slow, and
 * the (enhanced) uniform approach implemented here may will be faster, although
 * it might produce inferior search quality.
 * <p>
 * This implementation enhances the Carmel uniform pruning approach, as it
 * allows to specify three levels of thresholds:
 * <ul>
 * <li>one default threshold - globally (for terms in all fields)</li>
 * <li>threshold per field</li>
 * <li>threshold per term</li>
 * </ul>
 * <p>
 * These thresholds are applied so that always the most specific one takes
 * precedence: first a per-term threshold is used if present, then per-field
 * threshold if present, and finally the default threshold.
 * <p>
 * Threshold are maintained in a map, keyed by either field names or terms in
 * <code>field:text</code> format. precedence of these values is the following:
 * <p>
 * Thresholds in this method of pruning are expressed as the percentage of the
 * top-N scoring documents per term that are retained. The list of top-N
 * documents is established by using a regular {@link IndexSearcher} and
 * {@link Similarity} to run a simple {@link TermQuery}.
 * <p>
 * Smaller threshold value will produce a smaller index. See
 * {@link TermPruningPolicy} for size vs performance considerations.
 * <p>
 * For indexes with a large number of terms this policy might be still too slow,
 * since it issues a term query for each term in the index. In such situations,
 * the term frequency pruning approach in {@link TFTermPruningPolicy} will be
 * faster, though it might produce inferior search quality.
 */
public class CarmelUniformTermPruningPolicy extends TermPruningPolicy {
  int docsPos = 0;
  float curThr;
  float defThreshold;
  Map<String,Float> thresholds;
  ScoreDoc[] docs = null;
  IndexSearcher is;
  Similarity sim;
  
  public CarmelUniformTermPruningPolicy(IndexReader in,
      Map<String,Integer> fieldFlags, Map<String,Float> thresholds,
      float defThreshold, Similarity sim) {
    super(in, fieldFlags);
    this.defThreshold = defThreshold;
    if (thresholds != null) {
      this.thresholds = thresholds;
    } else {
      this.thresholds = Collections.emptyMap();
    }
    if (sim != null) {
      this.sim = sim;
    } else {
      sim = new DefaultSimilarity();
    }
    is = new IndexSearcher(in);
    is.setSimilarity(sim);
  }
  
  // too costly - pass everything at this stage
  @Override
  public boolean pruneTermEnum(TermEnum te) throws IOException {
    return false;
  }
  
  @Override
  public void initPositionsTerm(TermPositions tp, Term t) throws IOException {
    curThr = defThreshold;
    String termKey = t.field() + ":" + t.text();
    if (thresholds.containsKey(termKey)) {
      curThr = thresholds.get(termKey);
    } else if (thresholds.containsKey(t.field())) {
      curThr = thresholds.get(t.field());
    }
    // calculate count
    int df = in.docFreq(t);
    int count = Math.round((float) df * curThr);
    if (count < 100) count = 100;
    TopScoreDocCollector collector = TopScoreDocCollector.create(count, true);
    TermQuery tq = new TermQuery(t);
    is.search(tq, collector);
    docs = collector.topDocs().scoreDocs;
    Arrays.sort(docs, ByDocComparator.INSTANCE);
    docsPos = 0;
  }
  
  @Override
  public boolean pruneAllPositions(TermPositions termPositions, Term t)
      throws IOException {
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
  
}
