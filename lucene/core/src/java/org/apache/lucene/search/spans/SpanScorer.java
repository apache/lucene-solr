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
import java.util.Objects;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Public for extension only.
 */
public class SpanScorer extends Scorer {
  /** underlying spans we are scoring from */
  protected final Spans spans;
  /** similarity used in default score impl */
  protected final Similarity.SimScorer docScorer;

  /** accumulated sloppy freq (computed in setFreqCurrentDoc) */
  protected float freq;
  /** number of matches (computed in setFreqCurrentDoc) */
  protected int numMatches;
  
  private int lastScoredDoc = -1; // last doc we called setFreqCurrentDoc() for

  /**
   * Creates a new SpanScorer
   * @lucene.internal
   */
  public SpanScorer(Spans spans, SpanWeight weight, Similarity.SimScorer docScorer) throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.spans = Objects.requireNonNull(spans);
  }

  @Override
  public final int nextDoc() throws IOException {
    return spans.nextDoc();
  }

  @Override
  public final int advance(int target) throws IOException {
    return spans.advance(target);
  }
  
  /** 
   * Ensure setFreqCurrentDoc is called, if not already called for the current doc.
   */
  private final void ensureFreq() throws IOException {
    int currentDoc = spans.docID();
    if (lastScoredDoc != currentDoc) {
      setFreqCurrentDoc();
      lastScoredDoc = currentDoc;
    }
  }

  /**
   * Sets {@link #freq} and {@link #numMatches} for the current document.
   * <p>
   * This will be called at most once per document.
   */
  protected final void setFreqCurrentDoc() throws IOException {
    freq = 0.0f;
    numMatches = 0;

    doStartCurrentDoc();

    assert spans.startPosition() == -1 : "incorrect initial start position, spans="+spans;
    assert spans.endPosition() == -1 : "incorrect initial end position, spans="+spans;
    int prevStartPos = -1;
    int prevEndPos = -1;

    int startPos = spans.nextStartPosition();
    assert startPos != Spans.NO_MORE_POSITIONS : "initial startPos NO_MORE_POSITIONS, spans="+spans;
    do {
      assert startPos >= prevStartPos;
      int endPos = spans.endPosition();
      assert endPos != Spans.NO_MORE_POSITIONS;
      // This assertion can fail for Or spans on the same term:
      // assert (startPos != prevStartPos) || (endPos > prevEndPos) : "non increased endPos="+endPos;
      assert (startPos != prevStartPos) || (endPos >= prevEndPos) : "decreased endPos="+endPos;
      numMatches++;
      if (docScorer == null) {  // scores not required, break out here
        freq = 1;
        return;
      }
      freq += docScorer.computeSlopFactor(spans.width());
      doCurrentSpans();
      prevStartPos = startPos;
      prevEndPos = endPos;
      startPos = spans.nextStartPosition();
    } while (startPos != Spans.NO_MORE_POSITIONS);

    assert spans.startPosition() == Spans.NO_MORE_POSITIONS : "incorrect final start position, spans="+spans;
    assert spans.endPosition() == Spans.NO_MORE_POSITIONS : "incorrect final end position, spans="+spans;
  }

  /**
   * Called before the current doc's frequency is calculated
   */
  protected void doStartCurrentDoc() throws IOException {}

  /**
   * Called each time the scorer's Spans is advanced during frequency calculation
   */
  protected void doCurrentSpans() throws IOException {}
  
  /**
   * Score the current doc. The default implementation scores the doc 
   * with the similarity using the slop-adjusted {@link #freq}.
   */
  protected float scoreCurrentDoc() throws IOException {
    return docScorer.score(spans.docID(), freq);
  }

  @Override
  public final int docID() { return spans.docID(); }

  @Override
  public final float score() throws IOException {
    ensureFreq();
    return scoreCurrentDoc();
  }

  @Override
  public final int freq() throws IOException {
    ensureFreq();
    return numMatches;
  }

  /** Returns the intermediate "sloppy freq" adjusted for edit distance
   *  @lucene.internal */
  // only public so .payloads can see it.
  public final float sloppyFreq() throws IOException {
    ensureFreq();
    return freq;
  }

  @Override
  public final long cost() {
    return spans.cost();
  }

  @Override
  public final TwoPhaseIterator asTwoPhaseIterator() {
    return spans.asTwoPhaseIterator();
  }
}
