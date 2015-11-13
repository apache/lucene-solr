package org.apache.lucene.search.spans;

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

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/** Iterates through combinations of start/end positions per-doc.
 *  Each start/end position represents a range of term positions within the current document.
 *  These are enumerated in order, by increasing document number, within that by
 *  increasing start position and finally by increasing end position.
 */
public abstract class Spans extends Scorer {

  public static final int NO_MORE_POSITIONS = Integer.MAX_VALUE;

  protected final Similarity.SimScorer docScorer;

  protected Spans(SpanWeight weight, SimScorer docScorer) {
    super(weight);
    this.docScorer = docScorer;
  }

  /** accumulated sloppy freq (computed in setFreqCurrentDoc) */
  protected float freq;
  /** number of matches (computed in setFreqCurrentDoc) */
  protected int numMatches;

  private int lastScoredDoc = -1; // last doc we called setFreqCurrentDoc() for

  /**
   * Returns the next start position for the current doc.
   * There is always at least one start/end position per doc.
   * After the last start/end position at the current doc this returns {@link #NO_MORE_POSITIONS}.
   */
  public abstract int nextStartPosition() throws IOException;

  /**
   * Returns the start position in the current doc, or -1 when {@link #nextStartPosition} was not yet called on the current doc.
   * After the last start/end position at the current doc this returns {@link #NO_MORE_POSITIONS}.
   */
  public abstract int startPosition();

  /**
   * Returns the end position for the current start position, or -1 when {@link #nextStartPosition} was not yet called on the current doc.
   * After the last start/end position at the current doc this returns {@link #NO_MORE_POSITIONS}.
   */
  public abstract int endPosition();

  /**
   * Return the width of the match, which is typically used to compute
   * the {@link SimScorer#computeSlopFactor(int) slop factor}. It is only legal
   * to call this method when the iterator is on a valid doc ID and positioned.
   * The return value must be positive, and lower values means that the match is
   * better.
   */
  public abstract int width();

  /**
   * Collect postings data from the leaves of the current Spans.
   *
   * This method should only be called after {@link #nextStartPosition()}, and before
   * {@link #NO_MORE_POSITIONS} has been reached.
   *
   * @param collector a SpanCollector
   *
   * @lucene.experimental
   */
  public abstract void collect(SpanCollector collector) throws IOException;

  /**
   * Return an estimation of the cost of using the positions of
   * this {@link Spans} for any single document, but only after
   * {@link #asTwoPhaseIterator} returned {@code null}.
   * Otherwise this method should not be called.
   * The returned value is independent of the current document.
   *
   * @lucene.experimental
   */
  public abstract float positionsCost();

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Class<? extends Spans> clazz = getClass();
    sb.append(clazz.isAnonymousClass() ? clazz.getName() : clazz.getSimpleName());
    sb.append("(doc=").append(docID());
    sb.append(",start=").append(startPosition());
    sb.append(",end=").append(endPosition());
    sb.append(")");
    return sb.toString();
  }

  /**
   * Ensure setFreqCurrentDoc is called, if not already called for the current doc.
   */
  private void ensureFreq() throws IOException {
    int currentDoc = docID();
    if (lastScoredDoc != currentDoc) {
      setFreqCurrentDoc();
      lastScoredDoc = currentDoc;
    }
  }

  /**
   * Sets {@link #freq} and {@link #numMatches} for the current document.
   * <p>
   * This will be called at most once per document.
   */
  protected final void setFreqCurrentDoc() throws IOException {
    freq = 0.0f;
    numMatches = 0;

    doStartCurrentDoc();

    assert startPosition() == -1 : "incorrect initial start position, " + this.toString();
    assert endPosition() == -1 : "incorrect initial end position, " + this.toString();
    int prevStartPos = -1;
    int prevEndPos = -1;

    int startPos = nextStartPosition();
    assert startPos != Spans.NO_MORE_POSITIONS : "initial startPos NO_MORE_POSITIONS, " + this.toString();
    do {
      assert startPos >= prevStartPos;
      int endPos = endPosition();
      assert endPos != Spans.NO_MORE_POSITIONS;
      // This assertion can fail for Or spans on the same term:
      // assert (startPos != prevStartPos) || (endPos > prevEndPos) : "non increased endPos="+endPos;
      assert (startPos != prevStartPos) || (endPos >= prevEndPos) : "decreased endPos="+endPos;
      numMatches++;
      if (docScorer == null) {  // scores not required, break out here
        freq = 1;
        return;
      }
      freq += docScorer.computeSlopFactor(width());
      doCurrentSpans();
      prevStartPos = startPos;
      prevEndPos = endPos;
      startPos = nextStartPosition();
    } while (startPos != Spans.NO_MORE_POSITIONS);

    assert startPosition() == Spans.NO_MORE_POSITIONS : "incorrect final start position, " + this.toString();
    assert endPosition() == Spans.NO_MORE_POSITIONS : "incorrect final end position, " + this.toString();
  }

  /**
   * Called before the current doc's frequency is calculated
   */
  protected void doStartCurrentDoc() throws IOException {}

  /**
   * Called each time the scorer's SpanScorer is advanced during frequency calculation
   */
  protected void doCurrentSpans() throws IOException {}

  /**
   * Score the current doc. The default implementation scores the doc
   * with the similarity using the slop-adjusted {@link #freq}.
   */
  protected float scoreCurrentDoc() throws IOException {
    assert docScorer != null : getClass() + " has a null docScorer!";
    return docScorer.score(docID(), freq);
  }

  @Override
  public final float score() throws IOException {
    ensureFreq();
    return scoreCurrentDoc();
  }

  @Override
  public final int freq() throws IOException {
    ensureFreq();
    return numMatches;
  }

  /** Returns the intermediate "sloppy freq" adjusted for edit distance
   *  @lucene.internal */
  public final float sloppyFreq() throws IOException {
    ensureFreq();
    return freq;
  }

}
