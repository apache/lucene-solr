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
import java.util.Collection;

/** Expert: an enumeration of span matches.  Used to implement span searching.
 * Each span represents a range of term positions within a document.  Matches
 * are enumerated in order, by increasing document number, within that by
 * increasing start position and finally by increasing end position. */
public abstract class Spans {
  /** Move to the next match, returning true iff any such exists. */
  public abstract boolean next() throws IOException;

  /** Skips to the first match beyond the current, whose document number is
   * greater than or equal to <i>target</i>.
   * <p>The behavior of this method is <b>undefined</b> when called with
   * <code> target &le; current</code>, or after the iterator has exhausted.
   * Both cases may result in unpredicted behavior.
   * <p>Returns true iff there is such
   * a match.  <p>Behaves as if written: <pre class="prettyprint">
   *   boolean skipTo(int target) {
   *     do {
   *       if (!next())
   *         return false;
   *     } while (target > doc());
   *     return true;
   *   }
   * </pre>
   * Most implementations are considerably more efficient than that.
   */
  public abstract boolean skipTo(int target) throws IOException;

  /** Returns the document number of the current match.  Initially invalid. */
  public abstract int doc();

  /** Returns the start position of the current match.  Initially invalid. */
  public abstract int start();

  /** Returns the end position of the current match.  Initially invalid. */
  public abstract int end();
  
  /**
   * Returns the payload data for the current span.
   * This is invalid until {@link #next()} is called for
   * the first time.
   * This method must not be called more than once after each call
   * of {@link #next()}. However, most payloads are loaded lazily,
   * so if the payload data for the current position is not needed,
   * this method may not be called at all for performance reasons. An ordered
   * SpanQuery does not lazy load, so if you have payloads in your index and
   * you do not want ordered SpanNearQuerys to collect payloads, you can
   * disable collection with a constructor option.<br>
   * <br>
    * Note that the return type is a collection, thus the ordering should not be relied upon.
    * <br/>
   * @lucene.experimental
   *
   * @return a List of byte arrays containing the data of this payload, otherwise null if isPayloadAvailable is false
   * @throws IOException if there is a low-level I/O error
    */
  // TODO: Remove warning after API has been finalized
  public abstract Collection<byte[]> getPayload() throws IOException;

  /**
   * Checks if a payload can be loaded at this position.
   * <p/>
   * Payloads can only be loaded once per call to
   * {@link #next()}.
   *
   * @return true if there is a payload available at this position that can be loaded
   */
  public abstract boolean isPayloadAvailable() throws IOException;
  
  /**
   * Returns the estimated cost of this spans.
   * <p>
   * This is generally an upper bound of the number of documents this iterator
   * might match, but may be a rough heuristic, hardcoded value, or otherwise
   * completely inaccurate.
   */
  public abstract long cost();
}
