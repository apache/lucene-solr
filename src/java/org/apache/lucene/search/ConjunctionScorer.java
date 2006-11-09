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
import java.util.Arrays;
import java.util.Comparator;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {
  private Scorer[] scorers = new Scorer[2];
  private int length = 0;
  private int first = 0;
  private int last = -1;
  private boolean firstTime = true;
  private boolean more = true;
  private float coord;

  public ConjunctionScorer(Similarity similarity) {
    super(similarity);
  }

  final void add(Scorer scorer) {
    if (length >= scorers.length) {
      // grow the array
      Scorer[] temps = new Scorer[scorers.length * 2];
      System.arraycopy(scorers, 0, temps, 0, length);
      scorers = temps;
    }
    last += 1;
    length += 1;
    scorers[last] = scorer;
  }

  public int doc() { return scorers[first].doc(); }

  public boolean next() throws IOException {
    if (firstTime) {
      init(true);
    } else if (more) {
      more = scorers[last].next();                   // trigger further scanning
    }
    return doNext();
  }
  
  private boolean doNext() throws IOException {
    while (more && scorers[first].doc() < scorers[last].doc()) { // find doc w/ all clauses
      more = scorers[first].skipTo(scorers[last].doc());      // skip first upto last
      last = first; // move first to last
      first = (first == length-1) ? 0 : first+1;
    }
    return more;                                // found a doc with all clauses
  }

  public boolean skipTo(int target) throws IOException {
    if(firstTime) {
      init(false);
    }
    
    for (int i = 0, pos = first; i < length; i++) {
      if (!more) break; 
      more = scorers[pos].skipTo(target);
      pos = (pos == length-1) ? 0 : pos+1;
    }
    
    if (more)
      sortScorers();                              // re-sort scorers
    
    return doNext();
  }

  public float score() throws IOException {
    float sum = 0.0f;
    for (int i = 0; i < length; i++) {
      sum += scorers[i].score();
    }
    return sum * coord;
  }
  
  private void init(boolean initScorers) throws IOException {
    //  compute coord factor
    coord = getSimilarity().coord(length, length);
   
    more = length > 0;

    if(initScorers){
      // move each scorer to its first entry
      for (int i = 0, pos = first; i < length; i++) {
        if (!more) break; 
        more = scorers[pos].next();
        pos = (pos == length-1) ? 0 : pos+1;
      }
      // initial sort of simulated list
      if (more) 
        sortScorers();
    }

    firstTime = false;
  }

  private void sortScorers() {
    // squeeze the array down for the sort
    if (length != scorers.length) {
      Scorer[] temps = new Scorer[length];
      System.arraycopy(scorers, 0, temps, 0, length);
      scorers = temps;
    }
    
    // note that this comparator is not consistent with equals!
    Arrays.sort(scorers, new Comparator() {         // sort the array
        public int compare(Object o1, Object o2) {
          return ((Scorer)o1).doc() - ((Scorer)o2).doc();
        }
      });
   
    first = 0;
    last = length - 1;
  }

  public Explanation explain(int doc) {
    throw new UnsupportedOperationException();
  }

}
