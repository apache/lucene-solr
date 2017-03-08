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

import java.util.List;

import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * A DisjunctionSpans that also provides a slop for each match.
 *
 * See also {@link SpanOrQuery#SpanOrQuery(int, SpanQuery...)}.
 *
 * @lucene.experimental
 */
public class DisjunctionNearSpans extends DisjunctionSpans {
  protected final int maxDistance;
  protected final SimScorer simScorer;

  /** Construct a DisjunctionNearSpans.
   * @param spanOrQuery The query that provides the subSpans.
   * @param subSpans    Over which the disjunction is to be taken.
   * @param maxDistance The maximum distance to be returned as the current match slop.
   * @param simScorer   For computing the slop factor from the slop.
   */
  public DisjunctionNearSpans(
              SpanOrQuery spanOrQuery,
              List<Spans> subSpans,
              int maxDistance,
              SimScorer simScorer)
  {
    super(spanOrQuery, subSpans);
    this.maxDistance = maxDistance;
    this.simScorer = simScorer;
  }

  int currentSlop;
  int lastDoc = -1;

  Spans prevFirstSpans;
  int prevFirstSpansEndPosition;
  int lastDifferentSpansEndPosition;


  /**
   * Compute the minimum slop between the currently matching
   * sub spans and the previous and next matching other sub spans.
   * When this slop is bigger than maxDistance
   * or no other matching spans is available, return maxDistance.
   * <br>
   * The slop is computed from the end of a spans to the beginning
   * of the following different one. When this is negative, zero is used.
   * <br>
   * When this method is used in a document, it must be called once at each match
   * in the document.
   * <br>
   * See also {@link DisjunctionNearSpansDocScorer}.
   */
  public int currentSlop() {
    Spans firstSpans = byPositionQueue.top();
    assert firstSpans.startPosition() != NO_MORE_POSITIONS; // at a disjunction match

    int currentDoc = docID();
    if (lastDoc != currentDoc) { // at first match in currentDoc
      lastDoc = currentDoc;
      prevFirstSpans = null;
      lastDifferentSpansEndPosition = -1;
    }

    int firstSpansEndPosition = firstSpans.endPosition(); // avoid calling more than once below, no spans is moved here.

    int slopBefore;
    if (prevFirstSpans == null) { // at first match in currentDoc
      slopBefore = maxDistance;
    } else if (prevFirstSpans == firstSpans) { // sequence of same subspans.
      if (lastDifferentSpansEndPosition == -1) { // initial sequence of same subspans
        slopBefore = maxDistance;
      } else { // later sequence of same subspans
        slopBefore = Math.max(0, firstSpans.startPosition() - lastDifferentSpansEndPosition);
        slopBefore = Math.min(slopBefore, maxDistance);
      }
    } else { // first spans is different from previous spans
      slopBefore = Math.max(0, firstSpans.startPosition() - prevFirstSpansEndPosition);
      slopBefore = Math.min(slopBefore, maxDistance);
      lastDifferentSpansEndPosition = prevFirstSpansEndPosition;
    }
    prevFirstSpans = firstSpans;
    prevFirstSpansEndPosition = firstSpansEndPosition;

    int slopAfter;
    if (byPositionQueue.size() == 1) { // no other spans at this document
      slopAfter = maxDistance;
    } else {
      Spans secondSpans = byPositionQueue.subTop();
      assert secondSpans != null; // byPositionQueue.size() >= 2
      assert secondSpans != firstSpans;
      if (secondSpans.startPosition() == NO_MORE_POSITIONS) { // second exhausted in current doc
        slopAfter = maxDistance;
      } else {
        slopAfter = Math.max(0, secondSpans.startPosition() - firstSpansEndPosition);
        slopAfter = Math.min(slopAfter, maxDistance);
      }
    }

    currentSlop = Math.min(slopBefore, slopAfter);
    return currentSlop;
  }
}
