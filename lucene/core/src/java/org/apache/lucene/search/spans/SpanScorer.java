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
import org.apache.lucene.search.similarities.Similarity;

/**
 * Public for extension only.
 */
public class SpanScorer extends Scorer {
  protected Spans spans;

  protected int doc;
  protected float freq;
  protected int numMatches;
  protected final Similarity.SimScorer docScorer;

  protected SpanScorer(Spans spans, SpanWeight weight, Similarity.SimScorer docScorer)
  throws IOException {
    super(weight);
    this.docScorer = Objects.requireNonNull(docScorer);
    this.spans = Objects.requireNonNull(spans);
    this.doc = -1;
  }

  @Override
  public int nextDoc() throws IOException {
    int prevDoc = doc;
    doc = spans.nextDoc();
    if (doc != NO_MORE_DOCS) {
      setFreqCurrentDoc();
    }
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    int prevDoc = doc;
    doc = spans.advance(target);
    if (doc != NO_MORE_DOCS) {
      setFreqCurrentDoc();
    }
    return doc;
  }

  protected boolean setFreqCurrentDoc() throws IOException {
    freq = 0.0f;
    numMatches = 0;

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
      int matchLength = endPos - startPos;
      freq += docScorer.computeSlopFactor(matchLength);
      prevStartPos = startPos;
      prevEndPos = endPos;
      startPos = spans.nextStartPosition();
    } while (startPos != Spans.NO_MORE_POSITIONS);

    assert spans.startPosition() == Spans.NO_MORE_POSITIONS : "incorrect final start position, spans="+spans;
    assert spans.endPosition() == Spans.NO_MORE_POSITIONS : "incorrect final end position, spans="+spans;

    return true;
  }

  @Override
  public int docID() { return doc; }

  @Override
  public float score() throws IOException {
    float s = docScorer.score(doc, freq);
    return s;
  }

  @Override
  public int freq() throws IOException {
    return numMatches;
  }

  /** Returns the intermediate "sloppy freq" adjusted for edit distance
   *  @lucene.internal */
  // only public so .payloads can see it.
  public float sloppyFreq() throws IOException {
    return freq;
  }

  @Override
  public long cost() {
    return spans.cost();
  }

}
