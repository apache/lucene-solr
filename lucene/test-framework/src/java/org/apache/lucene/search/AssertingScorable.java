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

package org.apache.lucene.search;

import java.io.IOException;

/**
 * Wraps another Scorable and asserts that scores are reasonable
 * and only called when positioned
 */
public class AssertingScorable extends FilterScorable {

  public AssertingScorable(Scorable in) {
    super(in);
  }

  @Override
  public float score() throws IOException {
    int docId = docID();
    assert docId != -1 && docId != DocIdSetIterator.NO_MORE_DOCS : "score() called on unpositioned Scorable docid=" + docID();
    final float score = in.score();
    assert !Float.isNaN(score) : "NaN score for in="+in;
    return score;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    in.setMinCompetitiveScore(minScore);
  }

  public static Scorable wrap(Scorable in) {
    if (in instanceof AssertingScorable) {
      return in;
    }
    // If `in` is Scorer, we need to wrap it as a Scorer instead of Scorable because
    // NumericComparator uses the iterator cost of a Scorer in sort optimization.
    if (in instanceof Scorer) {
      return new WrappedScorer((Scorer) in);
    } else {
      return new AssertingScorable(in);
    }
  }

  private static class WrappedScorer extends FilterScorer {
    WrappedScorer(Scorer in) {
      super(in);
    }

    @Override
    public float score() throws IOException {
      return new AssertingScorable(in).score();
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      in.setMinCompetitiveScore(minScore);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return in.getMaxScore(upTo);
    }
  }

  public static Scorable unwrap(Scorable in) {
    while (true) {
      if (in instanceof AssertingScorable) in = ((AssertingScorable) in).in;
      else if (in instanceof AssertingScorer) in = ((AssertingScorer) in).in;
      else if (in instanceof WrappedScorer) in = ((WrappedScorer) in).in;
      else return in;
    }
  }
}
