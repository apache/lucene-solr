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
package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public class ConjunctionNearSpansDocScorer extends SpansDocScorer<ConjunctionNearSpans> {
  protected final SimScorer simScorer;
  protected final Spans[] subSpansArray;
  protected final ArrayList<SpansDocScorer<?>> subSpansDocScorers;
  protected final ConjunctionNearSpans nearSpans;

  /** Create a ConjunctionNearSpansDocScorer for a ConjunctionNearSpans and its subspans.
   * For the subspans use {@link SpansTreeScorer#createSpansDocScorer}.
   */
  public ConjunctionNearSpansDocScorer(
              SpansTreeScorer spansTreeScorer,
              ConjunctionNearSpans nearSpans)
  {
    this.nearSpans = nearSpans;
    this.simScorer = nearSpans.simScorer;
    this.subSpansArray = nearSpans.getSubSpans();
    this.subSpansDocScorers = new ArrayList<>(subSpansArray.length);
    for (Spans subSpans : subSpansArray) {
      SpansDocScorer<?> spansDocScorer = spansTreeScorer.createSpansDocScorer(subSpans);
      subSpansDocScorers.add(spansDocScorer);
    }
  }

  @Override
  public void beginDoc(int doc) throws IOException {
    super.beginDoc(doc);
    for (SpansDocScorer<?> spansDocScorer : subSpansDocScorers) {
      spansDocScorer.beginDoc(doc);
    }
  }

  @Override
  public void extractSpansDocScorersAtDoc(Set<AsSingleTermSpansDocScorer<?>> spansDocScorersAtDoc) {
    for (SpansDocScorer<?> spansDocScorer : subSpansDocScorers) {
      spansDocScorer.extractSpansDocScorersAtDoc(spansDocScorersAtDoc);
    }
  }


  /** Record a matching occurrence for all subspans.
   *  Use a slop factor that is the product of the given slopFactor
   *  and the slop factor of {@link ConjunctionNearSpans#currentSlop}.
   */
  @Override
  public void recordMatch(double slopFactor, int position) {
    int slop = Integer.max(nearSpans.currentSlop(), 0); // avoid infinite localSlopFactor for negative slop
    double localSlopFactor = simScorer.computeSlopFactor(slop);
    double nestedSlopFactor = slopFactor * localSlopFactor;
    for (int i = 0; i < subSpansArray.length; i++) {
      Spans subSpans = subSpansArray[i];
      assert subSpans.startPosition() >= position;
      SpansDocScorer<?> spansDocScorer = subSpansDocScorers.get(i);
      spansDocScorer.recordMatch(nestedSlopFactor, subSpans.startPosition());
    }
  }

  /** Return the sum of the matching frequencies of the subspans. */
  @Override
  public int docMatchFreq() {
    int freq = 0;
    for (SpansDocScorer<?> spansDocScorer : subSpansDocScorers) {
      freq += spansDocScorer.docMatchFreq();
    }
    return freq;
  }

  /** Return the sum of document scores of the subspans. */
  @Override
  public double docScore() throws IOException {
    double score = 0;
    for (SpansDocScorer<?> spansDocScorer : subSpansDocScorers) {
      score += spansDocScorer.docScore();
    }
    return score;
  }
}
