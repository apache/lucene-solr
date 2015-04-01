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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Collection;
import java.util.Set;

/** A Spans that is formed from the ordered subspans of a SpanNearQuery
 * where the subspans do not overlap and have a maximum slop between them,
 * and that does not need to collect payloads.
 * To also collect payloads, see {@link NearSpansPayloadOrdered}.
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
 *
 * Expert:
 * Only public for subclassing.  Most implementations should not need this class
 */
public class NearSpansOrdered extends NearSpans {

  protected int matchDoc = -1;
  protected int matchStart = -1;
  protected int matchEnd = -1;

  public NearSpansOrdered(SpanNearQuery query, List<Spans> subSpans) throws IOException {
    super(query, subSpans);
    this.atFirstInCurrentDoc = true; // -1 startPosition/endPosition also at doc -1
  }

  /** Advances the subSpans to just after an ordered match with a minimum slop
   * that is smaller than the slop allowed by the SpanNearQuery.
   * @return true iff there is such a match.
   */
  @Override
  int toMatchDoc() throws IOException {
    subSpansToFirstStartPosition();
    while (true) {
      if (! stretchToOrder()) {
        if (conjunction.nextDoc() == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        }
        subSpansToFirstStartPosition();
      } else {
        if (shrinkToAfterShortestMatch()) {
          atFirstInCurrentDoc = true;
          return conjunction.docID();
        }
        // not a match, after shortest ordered spans, not at beginning of doc.
        if (oneExhaustedInCurrentDoc) {
          if (conjunction.nextDoc() == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
          }
          subSpansToFirstStartPosition();
        }
      }
    }
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
    Spans prevSpans = subSpans.get(0);
    assert prevSpans.startPosition() != NO_MORE_POSITIONS : "prevSpans no start position "+prevSpans;
    assert prevSpans.endPosition() != NO_MORE_POSITIONS;
    for (int i = 1; i < subSpans.size(); i++) {
      Spans spans = subSpans.get(i);
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
    Spans lastSubSpans = subSpans.get(subSpans.size() - 1);
    matchStart = lastSubSpans.startPosition();
    matchEnd = lastSubSpans.endPosition();

    int matchSlop = 0;
    int lastStart = matchStart;
    int lastEnd = matchEnd;
    for (int i = subSpans.size() - 2; i >= 0; i--) {
      Spans prevSpans = subSpans.get(i);

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
      }

      assert prevStart <= matchStart;
      if (matchStart > prevEnd) { // Only non overlapping spans add to slop.
        matchSlop += (matchStart - prevEnd);
      }

      /* Do not break on (matchSlop > allowedSlop) here to make sure
       * that on return the first subSpans has nextStartPosition called.
       */
      matchStart = prevStart;
      lastStart = prevStart;
      lastEnd = prevEnd;
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

  /** Throws an UnsupportedOperationException */
  @Override
  public Collection<byte[]> getPayload() throws IOException {
    throw new UnsupportedOperationException("Use NearSpansPayloadOrdered instead");
  }

  /** Throws an UnsupportedOperationException */
  @Override
  public boolean isPayloadAvailable() {
    throw new UnsupportedOperationException("Use NearSpansPayloadOrdered instead");
  }

  @Override
  public String toString() {
    return "NearSpansOrdered("+query.toString()+")@"+docID()+": "+startPosition()+" - "+endPosition();
  }
}

