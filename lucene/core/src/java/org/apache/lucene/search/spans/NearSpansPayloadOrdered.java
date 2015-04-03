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

/** A {@link NearSpansOrdered} that allows collecting payloads.
 * Expert:
 * Only public for subclassing.  Most implementations should not need this class
 */
public class NearSpansPayloadOrdered extends NearSpansOrdered {

  private List<byte[]> matchPayload;
  private Set<byte[]> possibleMatchPayloads;

  public NearSpansPayloadOrdered(SpanNearQuery query, List<Spans> subSpans)
  throws IOException {
    super(query, subSpans);
    this.matchPayload = new LinkedList<>();
    this.possibleMatchPayloads = new HashSet<>();
  }

  /** The subSpans are ordered in the same doc, so there is a possible match.
   * Compute the slop while making the match as short as possible by using nextStartPosition
   * on all subSpans, except the last one, in reverse order.
   * Also collect the payloads.
   */
  protected boolean shrinkToAfterShortestMatch() throws IOException {
    Spans lastSubSpans = subSpans[subSpans.length - 1];
    matchStart = lastSubSpans.startPosition();
    matchEnd = lastSubSpans.endPosition();

    matchPayload.clear();
    possibleMatchPayloads.clear();

    if (lastSubSpans.isPayloadAvailable()) {
      possibleMatchPayloads.addAll(lastSubSpans.getPayload());
    }

    Collection<byte[]> possiblePayload = null;

    int matchSlop = 0;
    int lastStart = matchStart;
    for (int i = subSpans.length - 2; i >= 0; i--) {
      Spans prevSpans = subSpans[i];

      if (prevSpans.isPayloadAvailable()) {
        Collection<byte[]> payload = prevSpans.getPayload();
        possiblePayload = new ArrayList<>(payload.size());
        possiblePayload.addAll(payload);
      }

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
        if (prevSpans.isPayloadAvailable()) {
          Collection<byte[]> payload = prevSpans.getPayload();
          if (possiblePayload == null) {
            possiblePayload = new ArrayList<>(payload.size());
          } else {
            possiblePayload.clear();
          }
          possiblePayload.addAll(payload);
        }
      }

      if (possiblePayload != null) {
        possibleMatchPayloads.addAll(possiblePayload);
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
    }

    boolean match = matchSlop <= allowedSlop;

    if (match && possibleMatchPayloads.size() > 0) {
      matchPayload.addAll(possibleMatchPayloads);
    }

    return match; // ordered and allowed slop
  }

  // TODO: Remove warning after API has been finalized
  // TODO: Would be nice to be able to lazy load payloads
  /** Return payloads when available. */
  @Override
  public Collection<byte[]> getPayload() throws IOException {
    return matchPayload;
  }

  /** Indicates whether payloads are available */
  @Override
  public boolean isPayloadAvailable() {
    return ! matchPayload.isEmpty();
  }

  @Override
  public String toString() {
    return "NearSpansPayloadOrdered("+query.toString()+")@"+docID()+": "+startPosition()+" - "+endPosition();
  }
}

