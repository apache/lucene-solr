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

  /** The number of optionalScorers that need to match (if there are any) */
  private final int minNrShouldMatch;

  /** Create a BooleanScorer2.
   * @param similarity The similarity to be used.
   * @param minNrShouldMatch The minimum number of optional added scorers
   *                         that should match during the search.
   *                         In case no required scorers are added,
   *                         at least one of the optional scorers will have to
   *                         match during the search.
   */
  public BooleanScorer2(Similarity similarity, int minNrShouldMatch) {
    super(similarity);
    if (minNrShouldMatch < 0) {
      throw new IllegalArgumentException("Minimum number of optional scorers should not be negative");
    }
    coordinator = new Coordinator();
    this.minNrShouldMatch = minNrShouldMatch;
  }

  /** Create a BooleanScorer2.
   *  In no required scorers are added,
   *  at least one of the optional scorers will have to match during the search.
   * @param similarity The similarity to be used.
   */
  public BooleanScorer2(Similarity similarity) {
    this(similarity, 0);
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
    private int lastScoredDoc = -1;

    SingleMatchScorer(Scorer scorer) {
      super(scorer.getSimilarity());
      this.scorer = scorer;
    }
    public float score() throws IOException {
      if (this.doc() > lastScoredDoc) {
        lastScoredDoc = this.doc();
        coordinator.nrMatchers++;
      }
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

  private Scorer countingDisjunctionSumScorer(List scorers,
                                              int minMrShouldMatch)
  // each scorer from the list counted as a single matcher
  {
    return new DisjunctionSumScorer(scorers, minMrShouldMatch) {
      private int lastScoredDoc = -1;
      public float score() throws IOException {
        if (this.doc() > lastScoredDoc) {
          lastScoredDoc = this.doc();
          coordinator.nrMatchers += super.nrMatchers;
        }
        return super.score();
      }
    };
  }

  private static Similarity defaultSimilarity = new DefaultSimilarity();

  private Scorer countingConjunctionSumScorer(List requiredScorers) {
    // each scorer from the list counted as a single matcher
    final int requiredNrMatchers = requiredScorers.size();
    ConjunctionScorer cs = new ConjunctionScorer(defaultSimilarity) {
      private int lastScoredDoc = -1;

      public float score() throws IOException {
        if (this.doc() > lastScoredDoc) {
          lastScoredDoc = this.doc();
          coordinator.nrMatchers += requiredNrMatchers;
        }
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

  private Scorer dualConjunctionSumScorer(Scorer req1, Scorer req2) { // non counting. 
    final int requiredNrMatchers = requiredScorers.size();
    ConjunctionScorer cs = new ConjunctionScorer(defaultSimilarity);
    // All scorers match, so defaultSimilarity super.score() always has 1 as
    // the coordination factor.
    // Therefore the sum of the scores of two scorers
    // is used as score.
    cs.add(req1);
    cs.add(req2);
    return cs;
  }

  /** Returns the scorer to be used for match counting and score summing.
   * Uses requiredScorers, optionalScorers and prohibitedScorers.
   */
  private Scorer makeCountingSumScorer() { // each scorer counted as a single matcher
    return (requiredScorers.size() == 0)
          ? makeCountingSumScorerNoReq()
          : makeCountingSumScorerSomeReq();
  }

  private Scorer makeCountingSumScorerNoReq() { // No required scorers
    if (optionalScorers.size() == 0) {
      return new NonMatchingScorer(); // no clauses or only prohibited clauses
    } else { // No required scorers. At least one optional scorer.
      // minNrShouldMatch optional scorers are required, but at least 1
      int nrOptRequired = (minNrShouldMatch < 1) ? 1 : minNrShouldMatch;
      if (optionalScorers.size() < nrOptRequired) { 
        return new NonMatchingScorer(); // fewer optional clauses than minimum (at least 1) that should match
      } else { // optionalScorers.size() >= nrOptRequired, no required scorers
        Scorer requiredCountingSumScorer =
              (optionalScorers.size() > nrOptRequired)
              ? countingDisjunctionSumScorer(optionalScorers, nrOptRequired)
              : // optionalScorers.size() == nrOptRequired (all optional scorers are required), no required scorers
              (optionalScorers.size() == 1)
              ? new SingleMatchScorer((Scorer) optionalScorers.get(0))
              : countingConjunctionSumScorer(optionalScorers);
        return addProhibitedScorers( requiredCountingSumScorer);
      }
    }
  }

  private Scorer makeCountingSumScorerSomeReq() { // At least one required scorer.
    if (optionalScorers.size() < minNrShouldMatch) {
      return new NonMatchingScorer(); // fewer optional clauses than minimum that should match
    } else if (optionalScorers.size() == minNrShouldMatch) { // all optional scorers also required.
      ArrayList allReq = new ArrayList(requiredScorers);
      allReq.addAll(optionalScorers);
      return addProhibitedScorers( countingConjunctionSumScorer(allReq));
    } else { // optionalScorers.size() > minNrShouldMatch, and at least one required scorer
      Scorer requiredCountingSumScorer =
            (requiredScorers.size() == 1)
            ? new SingleMatchScorer((Scorer) requiredScorers.get(0))
            : countingConjunctionSumScorer(requiredScorers);
      if (minNrShouldMatch > 0) { // use a required disjunction scorer over the optional scorers
        return addProhibitedScorers( 
                      dualConjunctionSumScorer( // non counting
                              requiredCountingSumScorer,
                              countingDisjunctionSumScorer(
                                      optionalScorers,
                                      minNrShouldMatch)));
      } else { // minNrShouldMatch == 0
        return new ReqOptSumScorer(
                      addProhibitedScorers(requiredCountingSumScorer),
                      ((optionalScorers.size() == 1)
                        ? new SingleMatchScorer((Scorer) optionalScorers.get(0))
                        : countingDisjunctionSumScorer(optionalScorers, 1))); // require 1 in combined, optional scorer.
      }
    }
  }
  
  /** Returns the scorer to be used for match counting and score summing.
   * Uses the given required scorer and the prohibitedScorers.
   * @param requiredCountingSumScorer A required scorer already built.
   */
  private Scorer addProhibitedScorers(Scorer requiredCountingSumScorer)
  {
    return (prohibitedScorers.size() == 0)
          ? requiredCountingSumScorer // no prohibited
          : new ReqExclScorer(requiredCountingSumScorer,
                              ((prohibitedScorers.size() == 1)
                                ? (Scorer) prohibitedScorers.get(0)
                                : new DisjunctionSumScorer(prohibitedScorers)));
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

