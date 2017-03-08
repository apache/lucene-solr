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
import java.util.Set;
import static java.util.Arrays.sort;

import org.apache.lucene.search.similarities.Similarity.SimScorer;
import static org.apache.lucene.util.ArrayUtil.oversize;


/**
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public abstract class AsSingleTermSpansDocScorer<SpansT extends Spans>
extends SpansDocScorer<SpansT> {

  protected final SimScorer simScorer;
  protected final double nonMatchWeight;

  protected int tf;
  protected int matchTF;
  protected int lastRecordedPosition;
  protected double[] occSlops;

  protected final int INIT_SLOPS_SIZE = 2; // CHECKME: use average term frequency?

  /**
   * @param simScorer      Scores the term occurrences.
   * @param nonMatchWeight The non negative weight to be used for the non matching term occurrences.
   */
  public AsSingleTermSpansDocScorer(SimScorer simScorer, double nonMatchWeight) {
    this.simScorer = simScorer;
    this.nonMatchWeight = nonMatchWeight;
    assert nonMatchWeight >= 0 : ("nonMatchWeight="+ nonMatchWeight);
    this.occSlops = new double[INIT_SLOPS_SIZE];
  }

  /** The total number of occurrences of the term in the current document.
   */
  public abstract int termFreqInDoc() throws IOException;

  @Override
  public void beginDoc(int doc) throws IOException {
    super.beginDoc(doc);

    matchTF = 0;
    lastRecordedPosition = -1;
    // currentDoc = docID(); // only for asserts

    tf = termFreqInDoc();
    assert tf >= 1;
    if (occSlops.length < tf) {
      occSlops = new double[oversize(tf, Double.BYTES)];
    }
  }

  @Override
  public void extractSpansDocScorersAtDoc(Set<AsSingleTermSpansDocScorer<?>> spansDocScorersAtDoc) {
    spansDocScorersAtDoc.add(this);
  }


  /** Record a matching term occurrence and record its slopFactor and position.
   *  When this is called more than once for a document, the position should not decrease.
   *  Keep the largest slop factor when the position has not changed.
   */
  @Override
  public void recordMatch(double slopFactor, int position) {
    assert slopFactor >= 0;
    assert position != Spans.NO_MORE_POSITIONS;
    if (position < lastRecordedPosition) {
      throw new AssertionError("position=" + position + " is before lastRecordedPosition=" + lastRecordedPosition);
      // in case this becomes normal, record all positions and slopFactors and take maximum slopFactor later.
    }
    if (lastRecordedPosition < position) {
      occSlops[matchTF] = slopFactor;
      matchTF += 1;
      assert matchTF <= tf;
      lastRecordedPosition = position;
    } else {
      assert lastRecordedPosition == position;
      assert matchTF >= 1;
      if (slopFactor > occSlops[matchTF-1]) {
        occSlops[matchTF-1] = slopFactor;
      }
    }
  }

  @Override
  public int docMatchFreq() {
    return matchTF;
  }

  /** Compute the document score for the term.
   * <br>
   * For each matching occurrence determine the score contribution
   * and use the given slop factors in decreasing order as weights
   * on this contribution.
   * <br>
   * Use the <code>nonMatchSlop</code> as the weight for the score contribution
   * of the non matching occurrences.
   * <br>
   * For this it is assumed that {@link SimScorer#score(int, float)} provides
   * a diminishing (at least non increasing)
   * score contribution for each extra term occurrence.
   * <br>
   * Return the sum of these weighted contributions over all term occurrences.
   * <p>
   * The implementation is not optimal, especially when there are many
   * matching occurrences with the same slop factors.
   * <p>
   * Aside: The purpose of using the given slop factors in decreasing order
   * is to provide scoring consistency
   * between span near queries that only differ in the maximum allowed slop.
   * This consistency requires that any extra match increases the score of the document,
   * even when an extra match has a bigger slop and corresponding lower slop factor.
   * It is not known whether such scoring consistency is always achieved.
   * <br>
   * Sorting the slop factors could be avoided if an actual score
   * of each single term occurrence was available.
   * In that case the given slop factor could be used as a weight on that score.
   * Perhaps it is possible to estimate an actual score for a single term
   * occurrence from the distances to other occurrences of the same term.
   */
  @Override
  public double docScore() throws IOException {
    double docScore = 0;

    double cumulMatchTFScore = 0;

    if (matchTF > 0) {
      sort(occSlops, 0, matchTF);
      assert occSlops[0] >= nonMatchWeight; // non match distance large enough

      for (int matchOcc = 1; matchOcc <= matchTF; matchOcc++) {
        double prev = cumulMatchTFScore;
        cumulMatchTFScore = simScorer.score(currentDoc, (float) (matchOcc));
        double matchTFScore = cumulMatchTFScore - prev; // matchTFScore should not increase
        // use occurence slop factors in decreasing order:
        docScore += matchTFScore * occSlops[matchTF - matchOcc];
      }
    }

    if (matchTF < tf) { // non matching occurrences
      double tfScore = simScorer.score(currentDoc, (float) tf);
      double nonMatchingFreqScore = tfScore - cumulMatchTFScore;
      double nonMatchScore = nonMatchingFreqScore * nonMatchWeight;
      docScore += nonMatchScore;
    }

    return docScore;
  }
}
