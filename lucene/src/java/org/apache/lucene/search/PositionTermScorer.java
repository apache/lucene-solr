package org.apache.lucene.search;

/**
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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.Similarity.ExactDocScorer;
import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.util.BytesRef;

/**
 * Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 */
final class PositionTermScorer extends Scorer {
  private final DocsAndPositionsEnum docsEnum;
  private int doc = -1;
  private int freq;
  private TermPositions positions;
  private ExactDocScorer docScorer;

  /**
   * Construct a <code>TermScorer</code>.
   * 
   * @param weight
   *          The weight of the <code>Term</code> in the query.
   * @param td
   *          An iterator over the documents matching the <code>Term</code>.
   * @param similarity
   *          The </code>Similarity</code> implementation to be used for score
   *          computations.
   * @param norms
   *          The field norms of the document fields for the <code>Term</code>.
   */
  PositionTermScorer(Weight weight, DocsAndPositionsEnum td, Similarity.ExactDocScorer docScorer, boolean doPayloads) {
    super(weight);
    this.positions = new TermPositions(td, doPayloads);
    this.docsEnum = td;
    this.docScorer = docScorer;
  }

  @Override
  public void score(Collector c) throws IOException {
    score(c, Integer.MAX_VALUE, nextDoc());
  }

  // firstDocID is ignored since nextDoc() sets 'doc'
  @Override
  public boolean score(Collector c, int end, int firstDocID) throws IOException {
    c.setScorer(this);
    while (doc < end) { // for docs in window
      c.collect(doc); // collect score
      doc = docsEnum.nextDoc();
      if (doc != NO_MORE_DOCS) {
        freq = docsEnum.freq();
      }
    }
    return true;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public float freq() {
    return freq;
  }

  /**
   * Advances to the next document matching the query. <br>
   * The iterator over the matching documents is buffered using
   * {@link TermDocs#read(int[],int[])}.
   * 
   * @return the document matching the query or NO_MORE_DOCS if there are no
   *         more documents.
   */
  @Override
  public int nextDoc() throws IOException {

    doc = docsEnum.nextDoc();
    if (doc != NO_MORE_DOCS) {
      positions.positionsPending = freq = docsEnum.freq();
      positions.interval.reset();
    }
    return doc;
  }

  @Override
  public float score() {
    assert doc != -1;
    return docScorer.score(doc, freq);
  }

  /**
   * Advances to the first match beyond the current whose document number is
   * greater than or equal to a given target. <br>
   * The implementation uses {@link TermDocs#skipTo(int)}.
   * 
   * @param target
   *          The target document number.
   * @return the matching document or NO_MORE_DOCS if none exist.
   */
  @Override
  public int advance(int target) throws IOException {
    doc = docsEnum.advance(target);
    if (doc != NO_MORE_DOCS) {
      positions.positionsPending = freq = docsEnum.freq();
      positions.interval.reset();
    }
    return doc;
  }

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() {
    return "scorer(" + weight + ")";
  }

  @Override
  public PositionIntervalIterator positions() throws IOException {
    return positions;
  }

  @SuppressWarnings("serial")
  private final class TermPositions extends PositionIntervalIterator {
    private final PositionInterval interval;
    int positionsPending;
    private final DocsAndPositionsEnum docsAndPos;

    public TermPositions(DocsAndPositionsEnum docsAndPos, boolean doPayloads) {
      super(PositionTermScorer.this);
      this.docsAndPos = docsAndPos;
      this.interval = doPayloads ? new PayloadPosInterval(docsAndPos, this)
          : new PositionInterval();

    }

    @Override
    public PositionInterval next() throws IOException {
      if (--positionsPending >= 0) {
        interval.begin = interval.end = docsAndPos.nextPosition();
        return interval;
      }
      interval.reset();
      positionsPending = 0;
      return null;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public PositionIntervalIterator[] subs(boolean inOrder) {
      return EMPTY;
    }
  }

  private static final class PayloadPosInterval extends PositionInterval {
    private int pos = -1;
    private final DocsAndPositionsEnum payloads;
    private final TermPositions termPos;

    public PayloadPosInterval(DocsAndPositionsEnum payloads, TermPositions pos) {
      this.payloads = payloads;
      this.termPos = pos;
    }

    @Override
    public boolean payloadAvailable() {
      return payloads.hasPayload();
    }

    @Override
    public boolean nextPayload(BytesRef ref) throws IOException {
      if (pos == termPos.positionsPending) {
        return false;
      } else {
        pos = termPos.positionsPending;
        final BytesRef payload = payloads.getPayload();
        ref.bytes = payload.bytes;
        ref.length = payload.length;
        ref.offset = payload.offset;
        return true;
      }
    }

    @Override
    public void reset() {
      super.reset();
      pos = -1;
    }

  }

}
