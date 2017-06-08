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

import org.apache.lucene.search.similarities.Similarity.SimScorer;


/**
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public class DisjunctionNearSpansDocScorer
      extends DisjunctionSpansDocScorer<DisjunctionNearSpans> {
  protected final SimScorer simScorer;

  public DisjunctionNearSpansDocScorer(
              SpansTreeScorer spansTreeScorer,
              DisjunctionNearSpans orNearSpans)
  {
    super(spansTreeScorer, orNearSpans);
    this.simScorer = orNearSpans.simScorer;
  }

  /** Record a match for the subspans at the first position.
   *  Use a slop factor that is the product of the given slopFactor
   *  and the slop factor of {@link DisjunctionNearSpans#currentSlop}.
   */
  @Override
  public void recordMatch(double slopFactor, int position) {
    int slop = orSpans.currentSlop();
    double localSlopFactor = simScorer.computeSlopFactor(slop);
    double nestedSlopFactor = slopFactor * localSlopFactor;
    super.recordMatch(nestedSlopFactor, position);
  }

}
