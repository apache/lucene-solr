package org.apache.lucene.search;

import org.apache.lucene.search.intervals.Interval;
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

public class ExactIntervalPhraseScorer extends Scorer {

  private final Similarity.ExactSimScorer docScorer;
  private final ChildScorer[] children;
  private final Interval[] intervals;
  private final boolean matchOnly;

  /**
   * Constructs a Scorer
   *
   * @param weight The scorers <code>Weight</code>.
   */
  protected ExactIntervalPhraseScorer(Weight weight, Similarity.ExactSimScorer docScorer,
                                      boolean matchOnly, Scorer... children) {
    super(weight);
    this.docScorer = docScorer;
    this.children = new ChildScorer[children.length];
    this.intervals = new Interval[children.length];
    for (int i = 0; i < children.length; i++) {
      this.children[i] = new ChildScorer(children[i], "subphrase");
      this.intervals[i] = new Interval();
    }
    this.matchOnly = matchOnly;
  }

  @Override
  public IntervalIterator intervals(boolean collectIntervals) throws IOException {
    return null;
  }

  @Override
  public float score() throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int freq() throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int docID() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int nextDoc() throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int advance(int target) throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public int nextPosition() throws IOException {
    if (children[0].child.nextPosition() == NO_MORE_POSITIONS)
      return NO_MORE_POSITIONS;
    intervals[0].update(children[0].child);
    int i = 1;
    while (i < children.length) {
      while (intervals[i].begin <= intervals[i - 1].end) {
        if (children[i].child.nextPosition() == NO_MORE_POSITIONS)
          return NO_MORE_POSITIONS;
        intervals[i].update(children[i].child);
      }
      if (intervals[i].begin == intervals[i - 1].end) {
        i++;
      }
      else {
        if (children[0].child.nextPosition() == NO_MORE_POSITIONS)
          return NO_MORE_POSITIONS;
        intervals[0].update(children[0].child);
        i = 1;
      }
    }
    return children[0].child.startPosition();
  }

  private void resetIntervals() {
    for (Interval it : intervals) {
      it.reset();
    }
  }
}
