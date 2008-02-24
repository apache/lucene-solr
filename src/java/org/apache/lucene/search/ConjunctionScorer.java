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
import java.util.Collection;
import java.util.Arrays;
import java.util.Comparator;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {
  private final Scorer[] scorers;

  private boolean firstTime=true;
  private boolean more;
  private final float coord;
  private int lastDoc=-1;

  public ConjunctionScorer(Similarity similarity, Collection scorers) throws IOException {
    this(similarity, (Scorer[])scorers.toArray(new Scorer[scorers.size()]));
  }

  public ConjunctionScorer(Similarity similarity, Scorer[] scorers) throws IOException {
    super(similarity);
    this.scorers = scorers;
    coord = getSimilarity().coord(this.scorers.length, this.scorers.length);
  }

  public int doc() { return lastDoc; }

  public boolean next() throws IOException {
    if (firstTime)
      return init(0);
    else if (more)
      more = scorers[(scorers.length-1)].next();
    return doNext();
  }

  private boolean doNext() throws IOException {
    int first=0;
    Scorer lastScorer = scorers[scorers.length-1];
    Scorer firstScorer;
    while (more && (firstScorer=scorers[first]).doc() < (lastDoc=lastScorer.doc())) {
      more = firstScorer.skipTo(lastDoc);
      lastScorer = firstScorer;
      first = (first == (scorers.length-1)) ? 0 : first+1;
    }
    return more;
  }

  public boolean skipTo(int target) throws IOException {
    if (firstTime)
      return init(target);
    else if (more)
      more = scorers[(scorers.length-1)].skipTo(target);
    return doNext();
  }

  // Note... most of this could be done in the constructor
  // thus skipping a check for firstTime per call to next() and skipTo()
  private boolean init(int target) throws IOException {
    firstTime=false;
    more = scorers.length>1;
    for (int i=0; i<scorers.length; i++) {
      more = target==0 ? scorers[i].next() : scorers[i].skipTo(target);
      if (!more)
        return false;
    }

    // Sort the array the first time...
    // We don't need to sort the array in any future calls because we know
    // it will already start off sorted (all scorers on same doc).

    // note that this comparator is not consistent with equals!
    Arrays.sort(scorers, new Comparator() {         // sort the array
        public int compare(Object o1, Object o2) {
          return ((Scorer)o1).doc() - ((Scorer)o2).doc();
        }
      });

    doNext();

    // If first-time skip distance is any predictor of
    // scorer sparseness, then we should always try to skip first on
    // those scorers.
    // Keep last scorer in it's last place (it will be the first
    // to be skipped on), but reverse all of the others so that
    // they will be skipped on in order of original high skip.
    int end=(scorers.length-1);
    for (int i=0; i<(end>>1); i++) {
      Scorer tmp = scorers[i];
      scorers[i] = scorers[end-i-1];
      scorers[end-i-1] = tmp;
    }

    return more;
  }

  public float score() throws IOException {
    float sum = 0.0f;
    for (int i = 0; i < scorers.length; i++) {
      sum += scorers[i].score();
    }
    return sum * coord;
  }

  public Explanation explain(int doc) {
    throw new UnsupportedOperationException();
  }

}
