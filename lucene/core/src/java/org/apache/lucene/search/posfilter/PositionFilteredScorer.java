package org.apache.lucene.search.posfilter;

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

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

public abstract class PositionFilteredScorer extends Scorer {

  protected final Scorer[] subScorers;
  protected final Scorer child;
  protected final Interval current = new Interval();
  protected final Similarity.SimScorer simScorer;
  protected int matchDistance;

  private boolean buffered;

  public PositionFilteredScorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
    super(filteredScorer.getWeight());
    this.simScorer = simScorer;
    child = filteredScorer;
    subScorers = new Scorer[filteredScorer.getChildren().size()];
    int i = 0;
    for (ChildScorer subScorer : filteredScorer.getChildren()) {
      subScorers[i++] = subScorer.child;
    }
  }

  @Override
  public float score() throws IOException {
    return this.simScorer.score(docID(), intervalFreq());
  }

  private float freq = -1;

  protected final float intervalFreq() throws IOException {
    if (freq != -1)
      return freq;
    freq = 0;
    while (nextPosition() != NO_MORE_POSITIONS)
      freq += intervalScore();
    return freq;
  }

  @Override
  public int docID() {
    return child.docID();
  }

  @Override
  public int freq() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nextDoc() throws IOException {
    while (child.nextDoc() != NO_MORE_DOCS) {
      reset(child.docID());
      if (nextPosition() != NO_MORE_POSITIONS) {
        buffered = true;
        return child.docID();
      }
    }
    return NO_MORE_DOCS;
  }

  @Override
  public int advance(int target) throws IOException {
    if (child.advance(target) == NO_MORE_DOCS)
      return NO_MORE_DOCS;
    do {
      reset(child.docID());
      if (nextPosition() != NO_MORE_POSITIONS) {
        buffered = true;
        return child.docID();
      }
    } while (child.nextDoc() != NO_MORE_DOCS);
    return NO_MORE_DOCS;
  }

  @Override
  public int nextPosition() throws IOException {
    if (buffered) {
      //System.out.println(this.hashCode() + ": returning buffered nextPos");
      buffered = false;
      return current.begin;
    }
    //System.out.println(this.hashCode() + ": returning unbuffered nextPos");
    return doNextPosition();
  }

  protected abstract int doNextPosition() throws IOException;

  protected void reset(int doc) throws IOException {
    buffered = false;
    freq = -1;
  };

  public int getMatchDistance() {
    return matchDistance;
  }

  @Override
  public int startPosition() throws IOException {
    return current.begin;
  }

  @Override
  public int endPosition() throws IOException {
    return current.end;
  }

  @Override
  public int startOffset() throws IOException {
    return current.offsetBegin;
  }

  @Override
  public int endOffset() throws IOException {
    return current.offsetEnd;
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return null; // nocommit
  }

  @Override
  public long cost() {
    return child.cost();
  }
// nocommit Payloads - need to add these to Interval?
}
