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
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

/**
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public class DisjunctionSpansDocScorer<DisjunctionSpansT extends DisjunctionSpans>
      extends SpansDocScorer<DisjunctionSpansT> {
  protected final ArrayList<Spans> subSpansAtDoc;
  protected final HashSet<AsSingleTermSpansDocScorer<?>> spansDocScorersAtDoc;
  protected DisjunctionSpansT orSpans;

  /** Create a DisjunctionSpansDocScorer for a DisjunctionSpans and its subspans.
   * For the subspans use {@link SpansTreeScorer#createSpansDocScorer}.
   */
  public DisjunctionSpansDocScorer(
            SpansTreeScorer spansTreeScorer,
            DisjunctionSpansT orSpans)
  {
    this.orSpans = orSpans;
    List<Spans> subSpans = orSpans.subSpans();
    for (Spans spans : subSpans) {
      spansTreeScorer.createSpansDocScorer(spans);
    }
    this.subSpansAtDoc = new ArrayList<>(subSpans.size());
    this.spansDocScorersAtDoc = new HashSet<>();
  }

  @Override
  public void beginDoc(int doc) throws IOException {
    subSpansAtDoc.clear();
    orSpans.extractSubSpansAtCurrentDoc(subSpansAtDoc);
    assert subSpansAtDoc.size() > 0 : "empty subSpansAtDoc docID=" + docID();
    spansDocScorersAtDoc.clear();
    for (Spans subSpans : subSpansAtDoc) {
      assert subSpans.docID() == doc;
      subSpans.spansDocScorer.extractSpansDocScorersAtDoc(spansDocScorersAtDoc);
    }
    for (SpansDocScorer<?> spansDocScorer : spansDocScorersAtDoc) {
      spansDocScorer.beginDoc(doc);
    }
  }

  @Override
  public void extractSpansDocScorersAtDoc(Set<AsSingleTermSpansDocScorer<?>> spansDocScorersAtDoc) {
    spansDocScorersAtDoc.addAll(this.spansDocScorersAtDoc);
  }


  /** Record a match with the given slop factor for the subspans at the first position. */
  @Override
  public void recordMatch(double slopFactor, int position) {
    Spans firstPosSpans = orSpans.getCurrentPositionSpans();
    assert subSpansAtDoc.contains(firstPosSpans);
    assert firstPosSpans.startPosition() == position;
    firstPosSpans.spansDocScorer.recordMatch(slopFactor, position);
  }

  /** Return the sum of the matching frequencies of the subspans. */
  @Override
  public int docMatchFreq() {
    int freq = 0;
    for (SpansDocScorer<?> spansDocScorer : spansDocScorersAtDoc) {
      freq += spansDocScorer.docMatchFreq();
    }
    return freq;
  }

  /** Return the sum of document scores of the subspans. */
  @Override
  public double docScore() throws IOException {
    double score = 0;
    for (SpansDocScorer<?> spansDocScorer : spansDocScorersAtDoc) {
      score += spansDocScorer.docScore();
    }
    return score;
  }
}
