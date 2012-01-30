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
 * metric to determine the postings of terms to keep/remove. Residual
 * IDF is a difference between a collection-wide IDF of a term and the
 * observed in-document frequency of the term.
 */
public class RIDFTermPruningPolicy extends TermPruningPolicy {
  double defThreshold;
  Map<String, Double> thresholds;
  double df;
  double maxDoc;

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
    df = Math.log(in.docFreq(t) / maxDoc);
  }

  @Override
  public boolean pruneTermEnum(TermEnum te) throws IOException {
    return false;
  }

  @Override
  public boolean pruneAllPositions(TermPositions termPositions, Term t)
          throws IOException {
    double ridf = Math.log(1 - Math.pow(Math.E, termPositions.freq() / maxDoc)) - df;
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
