package org.apache.lucene.search;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/** An alternative to BooleanScorer.
 * <br>Uses ConjunctionScorer, DisjunctionScorer, ReqOptScorer and ReqExclScorer.
 * <br>Implements skipTo(), and has no limitations on the numbers of added scorers.
 */
class BooleanScorer2 extends Scorer {
  private ArrayList requiredScorers = new ArrayList();
  private ArrayList optionalScorers = new ArrayList();
  private ArrayList prohibitedScorers = new ArrayList();


  private class Coordinator {
    int maxCoord = 0; // to be increased for each non prohibited scorer
    
    private float[] coordFactors = null;
    
    void init() { // use after all scorers have been added.
      coordFactors = new float[maxCoord + 1];
      Similarity sim = getSimilarity();
      for (int i = 0; i <= maxCoord; i++) {
        coordFactors[i] = sim.coord(i, maxCoord);
      }
    }
    
    int nrMatchers; // to be increased by score() of match counting scorers.

    void initDoc() {
      nrMatchers = 0;
    }
    
    float coordFactor() {
      return coordFactors[nrMatchers];
    }
  }

  private final Coordinator coordinator;

  /** The scorer to which all scoring will be delegated,
   * except for computing and using the coordination factor.
   */
  private Scorer countingSumScorer = null;

  public BooleanScorer2(Similarity similarity) {
    super(similarity);
    coordinator = new Coordinator();
  }

  public void add(final Scorer scorer, boolean required, boolean prohibited) {
    if (!prohibited) {
      coordinator.maxCoord++;
    }

    if (required) {
      if (prohibited) {
        throw new IllegalArgumentException("scorer cannot be required and prohibited");
      }
      requiredScorers.add(scorer);
    } else if (prohibited) {
      prohibitedScorers.add(scorer);
    } else {
      optionalScorers.add(scorer);
    }
  }

  /** Initialize the match counting scorer that sums all the
   * scores. <p>
   * When "counting" is used in a name it means counting the number
   * of matching scorers.<br>
   * When "sum" is used in a name it means score value summing
   * over the matching scorers
   */
  private void initCountingSumScorer() {
    coordinator.init();
    countingSumScorer = makeCountingSumScorer();
  }

  /** Count a scorer as a single match. */
  private class SingleMatchScorer extends Scorer {
    private Scorer scorer;
    SingleMatchScorer(Scorer scorer) {
      super(scorer.getSimilarity());
      this.scorer = scorer;
    }
    public float score() throws IOException {
      coordinator.nrMatchers++;
      return scorer.score();
    }
    public int doc() {
      return scorer.doc();
    }
    public boolean next() throws IOException {
      return scorer.next();
    }
    public boolean skipTo(int docNr) throws IOException {
      return scorer.skipTo(docNr);
    }
    public Explanation explain(int docNr) throws IOException {
      return scorer.explain(docNr);
    }
  }

  private Scorer countingDisjunctionSumScorer(List scorers)
  // each scorer from the list counted as a single matcher
  {
    return new DisjunctionSumScorer(scorers) {
      public float score() throws IOException {
        coordinator.nrMatchers += nrMatchers;
        return super.score();
      }
    };
  }

  private static Similarity defaultSimilarity = new DefaultSimilarity();

  private Scorer countingConjunctionSumScorer(List requiredScorers)
  // each scorer from the list counted as a single matcher
  {
    final int requiredNrMatchers = requiredScorers.size();
    ConjunctionScorer cs = new ConjunctionScorer(defaultSimilarity) {
      public float score() throws IOException {
        coordinator.nrMatchers += requiredNrMatchers;
        // All scorers match, so defaultSimilarity super.score() always has 1 as
        // the coordination factor.
        // Therefore the sum of the scores of the requiredScorers
        // is used as score.
        return super.score();
      }
    };
    Iterator rsi = requiredScorers.iterator();
    while (rsi.hasNext()) {
      cs.add((Scorer) rsi.next());
    }
    return cs;
  }

  /** Returns the scorer to be used for match counting and score summing.
   * Uses requiredScorers, optionalScorers and prohibitedScorers.
   */
  private Scorer makeCountingSumScorer()
  // each scorer counted as a single matcher
  {
    if (requiredScorers.size() == 0) {
      if (optionalScorers.size() == 0) {
        return new NonMatchingScorer();  // only prohibited scorers
      } else if (optionalScorers.size() == 1) {
        return makeCountingSumScorer2( // the only optional scorer is required
                  new SingleMatchScorer((Scorer) optionalScorers.get(0)),
                  new ArrayList()); // no optional scorers left
      } else { // more than 1 optionalScorers, no required scorers
        return makeCountingSumScorer2( // at least one optional scorer is required
                  countingDisjunctionSumScorer(optionalScorers), 
                  new ArrayList()); // no optional scorers left
      }
    } else if (requiredScorers.size() == 1) { // 1 required
      return makeCountingSumScorer2(
                  new SingleMatchScorer((Scorer) requiredScorers.get(0)),
                  optionalScorers);
    } else { // more required scorers
      return makeCountingSumScorer2(
                  countingConjunctionSumScorer(requiredScorers),
                  optionalScorers);
    }
  }

  /** Returns the scorer to be used for match counting and score summing.
   * Uses the arguments and prohibitedScorers.
   * @param requiredCountingSumScorer A required scorer already built.
   * @param optionalScorers A list of optional scorers, possibly empty.
   */
  private Scorer makeCountingSumScorer2(
      Scorer requiredCountingSumScorer,
      List optionalScorers) // not match counting
  {
    if (optionalScorers.size() == 0) { // no optional
      if (prohibitedScorers.size() == 0) { // no prohibited
        return requiredCountingSumScorer;
      } else if (prohibitedScorers.size() == 1) { // no optional, 1 prohibited
        return new ReqExclScorer(
                      requiredCountingSumScorer,
                      (Scorer) prohibitedScorers.get(0)); // not match counting
      } else { // no optional, more prohibited
        return new ReqExclScorer(
                      requiredCountingSumScorer,
                      new DisjunctionSumScorer(prohibitedScorers)); // score unused. not match counting
      }
    } else if (optionalScorers.size() == 1) { // 1 optional
      return makeCountingSumScorer3(
                      requiredCountingSumScorer,
                      new SingleMatchScorer((Scorer) optionalScorers.get(0)));
   } else { // more optional
      return makeCountingSumScorer3(
                      requiredCountingSumScorer,
                      countingDisjunctionSumScorer(optionalScorers));
    }
  }

  /** Returns the scorer to be used for match counting and score summing.
   * Uses the arguments and prohibitedScorers.
   * @param requiredCountingSumScorer A required scorer already built.
   * @param optionalCountingSumScorer An optional scorer already built.
   */
  private Scorer makeCountingSumScorer3(
      Scorer requiredCountingSumScorer,
      Scorer optionalCountingSumScorer)
  {
    if (prohibitedScorers.size() == 0) { // no prohibited
      return new ReqOptSumScorer(requiredCountingSumScorer,
                                 optionalCountingSumScorer);
    } else if (prohibitedScorers.size() == 1) { // 1 prohibited
      return new ReqOptSumScorer(
                    new ReqExclScorer(requiredCountingSumScorer,
                                      (Scorer) prohibitedScorers.get(0)),  // not match counting
                    optionalCountingSumScorer);
    } else { // more prohibited
      return new ReqOptSumScorer(
                    new ReqExclScorer(
                          requiredCountingSumScorer,
                          new DisjunctionSumScorer(prohibitedScorers)), // score unused. not match counting
                    optionalCountingSumScorer);
    }
  }

  /** Scores and collects all matching documents.
   * @param hc The collector to which all matching documents are passed through
   * {@link HitCollector#collect(int, float)}.
   * <br>When this method is used the {@link #explain(int)} method should not be used.
   */
  public void score(HitCollector hc) throws IOException {
    if (countingSumScorer == null) {
      initCountingSumScorer();
    }
    while (countingSumScorer.next()) {
      hc.collect(countingSumScorer.doc(), score());
    }
  }

  /** Expert: Collects matching documents in a range.
   * <br>Note that {@link #next()} must be called once before this method is
   * called for the first time.
   * @param hc The collector to which all matching documents are passed through
   * {@link HitCollector#collect(int, float)}.
   * @param max Do not score documents past this.
   * @return true if more matching documents may remain.
   */
  protected boolean score(HitCollector hc, int max) throws IOException {
    // null pointer exception when next() was not called before:
    int docNr = countingSumScorer.doc();
    while (docNr < max) {
      hc.collect(docNr, score());
      if (! countingSumScorer.next()) {
        return false;
      }
      docNr = countingSumScorer.doc();
    }
    return true;
  }

  public int doc() { return countingSumScorer.doc(); }

  public boolean next() throws IOException {
    if (countingSumScorer == null) {
      initCountingSumScorer();
    }
    return countingSumScorer.next();
  }

  public float score() throws IOException {
    coordinator.initDoc();
    float sum = countingSumScorer.score();
    return sum * coordinator.coordFactor();
  }

  /** Skips to the first match beyond the current whose document number is
   * greater than or equal to a given target.
   * 
   * <p>When this method is used the {@link #explain(int)} method should not be used.
   * 
   * @param target The target document number.
   * @return true iff there is such a match.
   */
  public boolean skipTo(int target) throws IOException {
    if (countingSumScorer == null) {
      initCountingSumScorer();
    }
    return countingSumScorer.skipTo(target);
  }

  /** Throws an UnsupportedOperationException.
   * TODO: Implement an explanation of the coordination factor.
   * @param doc The document number for the explanation.
   * @throws UnsupportedOperationException
   */
  public Explanation explain(int doc) {
    throw new UnsupportedOperationException();
 /* How to explain the coordination factor?
    initCountingSumScorer();
    return countingSumScorer.explain(doc); // misses coord factor. 
  */
  }
}

