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
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;

/**
 * Policy for producing smaller index out of an input index, by removing postings data
 * for those terms where their in-document frequency is below a specified
 * threshold. 
 * <p>
 * Larger threshold value will produce a smaller index.
 * See {@link TermPruningPolicy} for size vs performance considerations.
 * <p>
 * This implementation uses simple term frequency thresholds to remove all postings
 * from documents where a given term occurs rarely (i.e. its TF in a document
 * is smaller than the threshold).
 * <p>
 * Threshold values in this method are expressed as absolute term frequencies.
 */
public class TFTermPruningPolicy extends TermPruningPolicy {
  protected Map<String,Integer> thresholds;
  protected int defThreshold;
  protected int curThr;

  public TFTermPruningPolicy(IndexReader in, Map<String,Integer> fieldFlags,
          Map<String,Integer> thresholds, int defThreshold) {
    super(in, fieldFlags);
    this.defThreshold = defThreshold;
    if (thresholds != null) {
      this.thresholds = thresholds;
    } else {
      this.thresholds = Collections.emptyMap();
    }
  }

  @Override
  public boolean pruneTermEnum(TermEnum te) throws IOException {
    // check that at least one doc exceeds threshold
    int thr = defThreshold;
    String termKey = te.term().field() + ":" + te.term().text();
    if (thresholds.containsKey(termKey)) {
      thr = thresholds.get(termKey);
    } else if (thresholds.containsKey(te.term().field())) {
      thr = thresholds.get(te.term().field());
    }
    TermDocs td = in.termDocs(te.term());
    boolean pass = false;
    do {
      if (td.freq() >= thr) {
        pass = true;
        break;
      }
    } while (td.next());
    td.close();
    return !pass;
  }

  @Override
  public void initPositionsTerm(TermPositions in, Term t) throws IOException {
    // set threshold for this field
    curThr = defThreshold;
    String termKey = t.field() + ":" + t.text();
    if (thresholds.containsKey(termKey)) {
      curThr = thresholds.get(termKey);
    } else if (thresholds.containsKey(t.field())) {
      curThr = thresholds.get(t.field());
    }
  }

  @Override
  public boolean pruneAllPositions(TermPositions termPositions, Term t)
          throws IOException {
    if (termPositions.freq() < curThr) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int pruneTermVectorTerms(int docNumber, String field, String[] terms,
          int[] freqs, TermFreqVector tfv)
          throws IOException {
    int thr = defThreshold;
    if (thresholds.containsKey(field)) {
      thr = thresholds.get(field);
    }
    int removed = 0;
    for (int i = 0; i < terms.length; i++) {
      // check per-term thresholds
      int termThr = thr;
      String t = field + ":" + terms[i];
      if (thresholds.containsKey(t)) {
        termThr = thresholds.get(t);
      }
      if (freqs[i] < termThr) {
        terms[i] = null;
        removed++;
      }      
    }
    return removed;
  }

  @Override
  public int pruneSomePositions(int docNum, int[] positions, Term curTerm) {
    return 0;  //this policy either prunes all or none, so nothing to prune here 
  }

}
