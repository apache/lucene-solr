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
import java.util.List;

/**
 * The Indri implemenation of a disjunction scorer which stores the subscorers for the child
 * queries. The score and smoothingScore methods use the list of all subscorers and not just the
 * matches so that a smoothingScore can be calculated if there is not an exact match.
 */
public abstract class IndriDisjunctionScorer extends IndriScorer {

  private final List<Scorer> subScorersList;
  private final DisiPriorityQueue subScorers;
  private final DocIdSetIterator approximation;

  protected IndriDisjunctionScorer(
      Weight weight, List<Scorer> subScorersList, ScoreMode scoreMode, float boost) {
    super(weight, boost);
    this.subScorersList = subScorersList;
    this.subScorers = new DisiPriorityQueue(subScorersList.size());
    for (Scorer scorer : subScorersList) {
      final DisiWrapper w = new DisiWrapper(scorer);
      this.subScorers.add(w);
    }
    this.approximation = new DisjunctionDISIApproximation(this.subScorers);
  }

  @Override
  public DocIdSetIterator iterator() {
    return approximation;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return 0;
  }

  public List<Scorer> getSubMatches() throws IOException {
    return subScorersList;
  }

  abstract float score(List<Scorer> subScorers) throws IOException;

  public abstract float smoothingScore(List<Scorer> subScorers, int docId) throws IOException;

  @Override
  public float score() throws IOException {
    return score(getSubMatches());
  }

  @Override
  public float smoothingScore(int docId) throws IOException {
    return smoothingScore(getSubMatches(), docId);
  }

  @Override
  public int docID() {
    return subScorers.top().doc;
  }
}
