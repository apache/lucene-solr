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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;

/**
 * Implementation of {@link TermPruningPolicy} that uses "residual IDF"
 * metric to determine the postings of terms to keep/remove, as defined in
 * <a href="">http://www.dc.fi.udc.es/~barreiro/publications/blanco_barreiro_ecir2007.pdf</a>.
 * <p>Residual IDF measures a difference between a collection-wide IDF of a term
 * (which assumes a uniform distribution of occurrences) and the actual
 * observed total number of occurrences of a term in all documents. Positive
 * values indicate that a term is informative (e.g. for rare terms), negative
 * values indicate that a term is not informative (e.g. too popular to offer
 * good selectivity).
 * <p>This metric produces small values close to [-1, 1], so useful ranges for
 * thresholds under this metrics are somewhere between [0, 1]. The higher the
 * threshold the more informative (and more rare) terms will be retained. For
 * filtering of common words a value of close to or slightly below 0 (e.g. -0.1)
 * should be a good starting point. 
 * 
 */
public class RIDFTermPruningPolicy extends TermPruningPolicy {
  double defThreshold;
  Map<String, Double> thresholds;
  double idf;
  double maxDoc;
  double ridf;

  public RIDFTermPruningPolicy(IndexReader in,
          Map<String, Integer> fieldFlags, Map<String, Double> thresholds,
          double defThreshold) {
    super(in, fieldFlags);
    this.defThreshold = defThreshold;
    if (thresholds != null) {
      this.thresholds = thresholds;
    } else {
      this.thresholds = Collections.emptyMap();
    }
    maxDoc = in.maxDoc();
  }

  @Override
  public void initPositionsTerm(TermPositions tp, Term t) throws IOException {
    // from formula [2], not the formula [1]
    // 
    idf = - Math.log((double)in.docFreq(t) / maxDoc);
    // calculate total number of occurrences
    int totalFreq = 0;
    while (tp.next()) {
      totalFreq += tp.freq();
    }
    // reposition the enum
    tp.seek(t);
    // rest of the formula [2] in the paper
    ridf = idf + Math.log(1 - Math.pow(Math.E,  - totalFreq / maxDoc));
  }

  @Override
  public boolean pruneTermEnum(TermEnum te) throws IOException {
    return false;
  }

  @Override
  public boolean pruneAllPositions(TermPositions termPositions, Term t)
          throws IOException {
    double thr = defThreshold;
    String key = t.field() + ":" + t.text();
    if (thresholds.containsKey(key)) {
      thr = thresholds.get(key);
    } else if (thresholds.containsKey(t.field())) {
      thr = thresholds.get(t.field());
    }
    if (ridf > thr) {
      return false; // keep
    } else {
      return true;
    }
  }

  @Override
  public int pruneTermVectorTerms(int docNumber, String field, String[] terms,
          int[] freqs, TermFreqVector v) throws IOException {
    return 0;
  }

  @Override
  public int pruneSomePositions(int docNum, int[] positions, Term curTerm) {
    return 0; //this policy either prunes all or none, so nothing to prune here
  }

}
