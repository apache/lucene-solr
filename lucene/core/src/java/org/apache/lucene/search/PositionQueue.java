package org.apache.lucene.search;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/**
 * Copyright (c) 2013 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class PositionQueue extends PriorityQueue<PositionQueue.ScorerRef> {

  class ScorerRef {

    public final Scorer scorer;
    public Interval interval = new Interval();

    public ScorerRef(Scorer scorer) {
      this.scorer = scorer;
    }

    public int nextPosition() throws IOException {
      if (scorer.docID() == DocsEnum.NO_MORE_DOCS || scorer.docID() != docId
            || scorer.nextPosition() == DocsEnum.NO_MORE_POSITIONS)
        interval.update(Interval.EXHAUSTED_INTERVAL);
      else
        interval.update(this.scorer);
      return interval.begin;
    }

  }

  boolean positioned = false;
  Interval current = new Interval();
  int docId = -1;

  public PositionQueue(Scorer[] subScorers) {
    super(subScorers.length);
    for (int i = 0; i < subScorers.length; i++) {
      add(new ScorerRef(subScorers[i]));
    }
  }

  public int nextPosition() throws IOException {
    if (!positioned) {
      for (Object scorerRef : getHeapArray()) {
        if (scorerRef != null)
          ((ScorerRef) scorerRef).nextPosition();
      }
      positioned = true;
      updateTop();
      current.update(top().interval);
      return current.begin;
    };
    if (current.begin == DocsEnum.NO_MORE_POSITIONS)
      return DocsEnum.NO_MORE_POSITIONS;
    top().nextPosition();
    updateTop();
    current.update(top().interval);
    return current.begin;
  }

  @Override
  protected boolean lessThan(ScorerRef a, ScorerRef b) {
    if (a.scorer.docID() < b.scorer.docID())
      return true;
    if (a.scorer.docID() > b.scorer.docID())
      return false;
    return a.interval.begin < b.interval.begin;
  }

  /**
   * Must be called after the scorers have been advanced
   */
  public void advanceTo(int doc) {
    positioned = false;
    this.docId = doc;
  }

  public int startPosition() throws IOException {
    return current.begin;
  }

  public int endPosition() throws IOException {
    return current.end;
  }

  public int startOffset() throws IOException {
    return current.offsetBegin;
  }

  public int endOffset() throws IOException {
    return current.offsetEnd;
  }
}
