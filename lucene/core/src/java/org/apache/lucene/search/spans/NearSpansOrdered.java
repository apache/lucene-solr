package org.apache.lucene.search.spans;

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

import java.io.IOException;
import java.util.List;

/** A Spans that is formed from the ordered subspans of a SpanNearQuery
 * where the subspans do not overlap and have a maximum slop between them.
 * <p>
 * The formed spans only contains minimum slop matches.<br>
 * The matching slop is computed from the distance(s) between
 * the non overlapping matching Spans.<br>
 * Successive matches are always formed from the successive Spans
 * of the SpanNearQuery.
 * <p>
 * The formed spans may contain overlaps when the slop is at least 1.
 * For example, when querying using
 * <pre>t1 t2 t3</pre>
 * with slop at least 1, the fragment:
 * <pre>t1 t2 t1 t3 t2 t3</pre>
 * matches twice:
 * <pre>t1 t2 .. t3      </pre>
 * <pre>      t1 .. t2 t3</pre>
 *
 * Because the algorithm used to minimize the size of a match consumes
 * child Spans eagerly, this uses a BufferedSpanCollector to collect
 * information from subspans.
 *
 * Expert:
 * Only public for subclassing.  Most implementations should not need this class
 */
public class NearSpansOrdered extends NearSpans {

  protected int matchDoc = -1;
  protected int matchStart = -1;
  protected int matchEnd = -1;

  protected final SpanCollector collector;
  protected BufferedSpanCollector buffer;

  public NearSpansOrdered(SpanNearQuery query, List<Spans> subSpans, SpanCollector collector) throws IOException {
    super(query, subSpans);
    this.atFirstInCurrentDoc = true; // -1 startPosition/endPosition also at doc -1
    this.collector = collector;
  }

  @Override
  boolean twoPhaseCurrentDocMatches() throws IOException {
    subSpansToFirstStartPosition();
    while (true) {
      if (! stretchToOrder()) {
        return false;
      }
      if (shrinkToAfterShortestMatch()) {
        atFirstInCurrentDoc = true;
        return true;
      }
      // not a match, after shortest ordered spans
      if (oneExhaustedInCurrentDoc) {
        return false;
      }
    }
  }

  @Override
  public int nextStartPosition() throws IOException {
    if (atFirstInCurrentDoc) {
      atFirstInCurrentDoc = false;
      return matchStart;
    }
    while (true) {
      if (oneExhaustedInCurrentDoc) {
        matchStart = NO_MORE_POSITIONS;
        matchEnd = NO_MORE_POSITIONS;
        return NO_MORE_POSITIONS;
      }
      if (! stretchToOrder()) {
        matchStart = NO_MORE_POSITIONS;
        matchEnd = NO_MORE_POSITIONS;
        return NO_MORE_POSITIONS;
      }
      if (shrinkToAfterShortestMatch()) { // may also leave oneExhaustedInCurrentDoc
        return matchStart;
      }
      // after shortest ordered spans, or oneExhaustedInCurrentDoc
    }
  }

  private void subSpansToFirstStartPosition() throws IOException {
    for (Spans spans : subSpans) {
      assert spans.startPosition() == -1 : "spans="+spans;
      spans.nextStartPosition();
      assert spans.startPosition() != NO_MORE_POSITIONS;
    }
    oneExhaustedInCurrentDoc = false;
  }

  /** Order the subSpans within the same document by using nextStartPosition on all subSpans
   * after the first as little as necessary.
   * Return true when the subSpans could be ordered in this way,
   * otherwise at least one is exhausted in the current doc.
   */
  private boolean stretchToOrder() throws IOException {
    Spans prevSpans = subSpans[0];
    assert prevSpans.startPosition() != NO_MORE_POSITIONS : "prevSpans no start position "+prevSpans;
    assert prevSpans.endPosition() != NO_MORE_POSITIONS;
    for (int i = 1; i < subSpans.length; i++) {
      Spans spans = subSpans[i];
      assert spans.startPosition() != NO_MORE_POSITIONS;
      assert spans.endPosition() != NO_MORE_POSITIONS;

      while (prevSpans.endPosition() > spans.startPosition()) { // while overlapping spans
        if (spans.nextStartPosition() == NO_MORE_POSITIONS) {
          return false;
        }
      }
      prevSpans = spans;
    }
    return true; // all subSpans ordered and non overlapping
  }

  /** The subSpans are ordered in the same doc, so there is a possible match.
   * Compute the slop while making the match as short as possible by using nextStartPosition
   * on all subSpans, except the last one, in reverse order.
   */
  protected boolean shrinkToAfterShortestMatch() throws IOException {
    Spans lastSubSpans = subSpans[subSpans.length - 1];
    matchStart = lastSubSpans.startPosition();
    matchEnd = lastSubSpans.endPosition();

    buffer = collector.buffer();
    buffer.collectCandidate(subSpans[subSpans.length - 1]);
    buffer.accept();

    int matchSlop = 0;
    int lastStart = matchStart;
    for (int i = subSpans.length - 2; i >= 0; i--) {
      Spans prevSpans = subSpans[i];
      buffer.collectCandidate(prevSpans);

      int prevStart = prevSpans.startPosition();
      int prevEnd = prevSpans.endPosition();
      while (true) { // prevSpans nextStartPosition until after (lastStart, lastEnd)
        if (prevSpans.nextStartPosition() == NO_MORE_POSITIONS) {
          oneExhaustedInCurrentDoc = true;
          break; // Check remaining subSpans for match.
        }
        int ppStart = prevSpans.startPosition();
        int ppEnd = prevSpans.endPosition();
        if (ppEnd > lastStart) { // if overlapping spans
          break; // Check remaining subSpans.
        }
        // prevSpans still before (lastStart, lastEnd)
        prevStart = ppStart;
        prevEnd = ppEnd;
        buffer.collectCandidate(prevSpans);
      }

      buffer.accept();

      assert prevStart <= matchStart;
      if (matchStart > prevEnd) { // Only non overlapping spans add to slop.
        matchSlop += (matchStart - prevEnd);
      }

      /* Do not break on (matchSlop > allowedSlop) here to make sure
       * that on return the first subSpans has nextStartPosition called.
       */
      matchStart = prevStart;
      lastStart = prevStart;
    }

    boolean match = matchSlop <= allowedSlop;

    return match; // ordered and allowed slop
  }

  @Override
  public int startPosition() {
    return atFirstInCurrentDoc ? -1 : matchStart;
  }

  @Override
  public int endPosition() {
    return atFirstInCurrentDoc ? -1 : matchEnd;
  }

  @Override
  public void collect(SpanCollector collector) {
    assert collector == this.collector
        : "You must collect using the same SpanCollector as was passed to the NearSpans constructor";
    buffer.replay();
  }

  @Override
  public String toString() {
    return "NearSpansOrdered("+query.toString()+")@"+docID()+": "+startPosition()+" - "+endPosition();
  }
}

