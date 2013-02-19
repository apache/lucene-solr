package org.apache.lucene.search;

import org.apache.lucene.search.posfilter.Interval;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;

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

public class ExactIntervalPhraseScorer extends ConjunctionTermScorer {

  private final TermScorer[] children;
  private final Interval[] intervals;
  private final Similarity.ExactSimScorer docScorer;
  private final boolean matchOnly;

  private boolean cached;

  /**
   * Constructs a Scorer
   *
   * @param weight The scorers <code>Weight</code>.
   */
  protected ExactIntervalPhraseScorer(Weight weight, Similarity.ExactSimScorer docScorer,
                                      boolean matchOnly, TermScorer... children) {
    super(weight, 1, wrapChildScorers(children));
    this.children = children;
    this.intervals = new Interval[children.length];
    for (int i = 0; i < children.length; i++) {
      this.intervals[i] = new Interval();
    }
    this.matchOnly = matchOnly;
    this.docScorer = docScorer;
  }

  private static DocsAndFreqs[] wrapChildScorers(TermScorer... children) {
    DocsAndFreqs[] docsAndFreqs = new DocsAndFreqs[children.length];
    for (int i = 0; i < children.length; i++) {
      docsAndFreqs[i] = new DocsAndFreqs(children[i]);
    }
    return docsAndFreqs;
  }

  @Override
  public IntervalIterator intervals(boolean collectIntervals) throws IOException {
    return null;
  }

  @Override
  public float score() throws IOException {
    return docScorer.score(docID(), freq());
  }

  @Override
  public int freq() throws IOException {
    if (matchOnly)
      return 1;
    int freq = 0;
    while (nextPosition() != NO_MORE_POSITIONS) //nocommit, should we try cacheing here?
      freq++;
    return freq;
  }

  @Override
  public int docID() {
    return children[0].docID();
  }

  @Override
  public int nextDoc() throws IOException {
    int doc;
    resetIntervals();
    while ((doc = super.nextDoc()) != NO_MORE_DOCS
        && nextPosition() == NO_MORE_POSITIONS) {
      resetIntervals();
    }
    cached = true;
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    int doc = super.advance(target);
    resetIntervals();
    while (doc != NO_MORE_DOCS && nextPosition() == NO_MORE_POSITIONS) {
      doc = super.nextDoc();
      resetIntervals();
    }
    cached = true;
    return doc;
  }

  public int nextPosition() throws IOException {
    if (cached == true) {
      cached = false;
      return children[0].startPosition();
    }
    if (children[0].nextPosition() == NO_MORE_POSITIONS)
      return NO_MORE_POSITIONS;
    intervals[0].update(children[0]);
    int i = 1;
    while (i < children.length) {
      while (intervals[i].begin <= intervals[i - 1].end) {
        if (children[i].nextPosition() == NO_MORE_POSITIONS)
          return NO_MORE_POSITIONS;
        intervals[i].update(children[i]);
      }
      if (intervals[i].begin == intervals[i - 1].end) {
        i++;
      }
      else {
        if (children[0].nextPosition() == NO_MORE_POSITIONS)
          return NO_MORE_POSITIONS;
        intervals[0].update(children[0]);
        i = 1;
      }
    }
    return children[0].startPosition();
  }

  private void resetIntervals() {
    for (Interval it : intervals) {
      it.reset();
    }
  }
}
