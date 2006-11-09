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

import java.util.List;
import java.util.Iterator;
import java.io.IOException;

import org.apache.lucene.util.ScorerDocQueue;

/** A Scorer for OR like queries, counterpart of <code>ConjunctionScorer</code>.
 * This Scorer implements {@link Scorer#skipTo(int)} and uses skipTo() on the given Scorers. 
 * @todo Implement score(HitCollector, int).
 */
class DisjunctionSumScorer extends Scorer {
  /** The number of subscorers. */ 
  private final int nrScorers;
  
  /** The subscorers. */
  protected final List subScorers;
  
  /** The minimum number of scorers that should match. */
  private final int minimumNrMatchers;
  
  /** The scorerDocQueue contains all subscorers ordered by their current doc(),
   * with the minimum at the top.
   * <br>The scorerDocQueue is initialized the first time next() or skipTo() is called.
   * <br>An exhausted scorer is immediately removed from the scorerDocQueue.
   * <br>If less than the minimumNrMatchers scorers
   * remain in the scorerDocQueue next() and skipTo() return false.
   * <p>
   * After each to call to next() or skipTo()
   * <code>currentSumScore</code> is the total score of the current matching doc,
   * <code>nrMatchers</code> is the number of matching scorers,
   * and all scorers are after the matching doc, or are exhausted.
   */
  private ScorerDocQueue scorerDocQueue = null;
  private int queueSize = -1; // used to avoid size() method calls on scorerDocQueue
  
  /** The document number of the current match. */
  private int currentDoc = -1;

  /** The number of subscorers that provide the current match. */
  protected int nrMatchers = -1;

  private float currentScore = Float.NaN;
  
  /** Construct a <code>DisjunctionScorer</code>.
   * @param subScorers A collection of at least two subscorers.
   * @param minimumNrMatchers The positive minimum number of subscorers that should
   * match to match this query.
   * <br>When <code>minimumNrMatchers</code> is bigger than
   * the number of <code>subScorers</code>,
   * no matches will be produced.
   * <br>When minimumNrMatchers equals the number of subScorers,
   * it more efficient to use <code>ConjunctionScorer</code>.
   */
  public DisjunctionSumScorer( List subScorers, int minimumNrMatchers) {
    super(null);
    
    nrScorers = subScorers.size();

    if (minimumNrMatchers <= 0) {
      throw new IllegalArgumentException("Minimum nr of matchers must be positive");
    }
    if (nrScorers <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }

    this.minimumNrMatchers = minimumNrMatchers;
    this.subScorers = subScorers;
  }
  
  /** Construct a <code>DisjunctionScorer</code>, using one as the minimum number
   * of matching subscorers.
   */
  public DisjunctionSumScorer(List subScorers) {
    this(subScorers, 1);
  }

  /** Called the first time next() or skipTo() is called to
   * initialize <code>scorerDocQueue</code>.
   */
  private void initScorerDocQueue() throws IOException {
    Iterator si = subScorers.iterator();
    scorerDocQueue = new ScorerDocQueue(nrScorers);
    queueSize = 0;
    while (si.hasNext()) {
      Scorer se = (Scorer) si.next();
      if (se.next()) { // doc() method will be used in scorerDocQueue.
        if (scorerDocQueue.insert(se)) {
          queueSize++;
        }
      }
    }
  }

  /** Scores and collects all matching documents.
   * @param hc The collector to which all matching documents are passed through
   * {@link HitCollector#collect(int, float)}.
   * <br>When this method is used the {@link #explain(int)} method should not be used.
   */
  public void score(HitCollector hc) throws IOException {
    while (next()) {
      hc.collect(currentDoc, currentScore);
    }
  }

  /** Expert: Collects matching documents in a range.  Hook for optimization.
   * Note that {@link #next()} must be called once before this method is called
   * for the first time.
   * @param hc The collector to which all matching documents are passed through
   * {@link HitCollector#collect(int, float)}.
   * @param max Do not score documents past this.
   * @return true if more matching documents may remain.
   */
  protected boolean score(HitCollector hc, int max) throws IOException {
    while (currentDoc < max) {
      hc.collect(currentDoc, currentScore);
      if (!next()) {
        return false;
      }
    }
    return true;
  }

  public boolean next() throws IOException {
    if (scorerDocQueue == null) {
      initScorerDocQueue();
    }
    return (scorerDocQueue.size() >= minimumNrMatchers)
          && advanceAfterCurrent();
  }


  /** Advance all subscorers after the current document determined by the
   * top of the <code>scorerDocQueue</code>.
   * Repeat until at least the minimum number of subscorers match on the same
   * document and all subscorers are after that document or are exhausted.
   * <br>On entry the <code>scorerDocQueue</code> has at least <code>minimumNrMatchers</code>
   * available. At least the scorer with the minimum document number will be advanced.
   * @return true iff there is a match.
   * <br>In case there is a match, </code>currentDoc</code>, </code>currentSumScore</code>,
   * and </code>nrMatchers</code> describe the match.
   *
   * @todo Investigate whether it is possible to use skipTo() when
   * the minimum number of matchers is bigger than one, ie. try and use the
   * character of ConjunctionScorer for the minimum number of matchers.
   * Also delay calling score() on the sub scorers until the minimum number of
   * matchers is reached.
   * <br>For this, a Scorer array with minimumNrMatchers elements might
   * hold Scorers at currentDoc that are temporarily popped from scorerQueue.
   */
  protected boolean advanceAfterCurrent() throws IOException {
    do { // repeat until minimum nr of matchers
      currentDoc = scorerDocQueue.topDoc();
      currentScore = scorerDocQueue.topScore();
      nrMatchers = 1;
      do { // Until all subscorers are after currentDoc
        if (! scorerDocQueue.topNextAndAdjustElsePop()) {
          if (--queueSize == 0) {
            break; // nothing more to advance, check for last match.
          }
        }
        if (scorerDocQueue.topDoc() != currentDoc) {
          break; // All remaining subscorers are after currentDoc.
        }
        currentScore += scorerDocQueue.topScore();
        nrMatchers++;
      } while (true);
      
      if (nrMatchers >= minimumNrMatchers) {
        return true;
      } else if (queueSize < minimumNrMatchers) {
        return false;
      }
    } while (true);
  }
  
  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #next()} is called the first time.
   */
  public float score() throws IOException { return currentScore; }
   
  public int doc() { return currentDoc; }

  /** Returns the number of subscorers matching the current document.
   * Initially invalid, until {@link #next()} is called the first time.
   */
  public int nrMatchers() {
    return nrMatchers;
  }

  /** Skips to the first match beyond the current whose document number is
   * greater than or equal to a given target.
   * <br>When this method is used the {@link #explain(int)} method should not be used.
   * <br>The implementation uses the skipTo() method on the subscorers.
   * @param target The target document number.
   * @return true iff there is such a match.
   */
  public boolean skipTo(int target) throws IOException {
    if (scorerDocQueue == null) {
      initScorerDocQueue();
    }
    if (queueSize < minimumNrMatchers) {
      return false;
    }
    if (target <= currentDoc) {
      return true;
    }
    do {
      if (scorerDocQueue.topDoc() >= target) {
        return advanceAfterCurrent();
      } else if (! scorerDocQueue.topSkipToAndAdjustElsePop(target)) {
        if (--queueSize < minimumNrMatchers) {
          return false;
        }
      }
    } while (true);
  }

  /** @return An explanation for the score of a given document. */
  public Explanation explain(int doc) throws IOException {
    Explanation res = new Explanation();
    Iterator ssi = subScorers.iterator();
    float sumScore = 0.0f;
    int nrMatches = 0;
    while (ssi.hasNext()) {
      Explanation es = ((Scorer) ssi.next()).explain(doc);
      if (es.getValue() > 0.0f) { // indicates match
        sumScore += es.getValue();
        nrMatches++;
      }
      res.addDetail(es);
    }
    if (nrMatchers >= minimumNrMatchers) {
      res.setValue(sumScore);
      res.setDescription("sum over at least " + minimumNrMatchers
                         + " of " + subScorers.size() + ":");
    } else {
      res.setValue(0.0f);
      res.setDescription(nrMatches + " match(es) but at least "
                         + minimumNrMatchers + " of "
                         + subScorers.size() + " needed");
    }
    return res;
  }
}
