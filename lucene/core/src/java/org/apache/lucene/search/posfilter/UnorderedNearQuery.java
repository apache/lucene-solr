package org.apache.lucene.search.posfilter;

/**
 * Copyright (c) 2012 Lemur Consulting Ltd.
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

import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.PositionQueue;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A query that matches if a set of subqueries also match, and are within
 * a given distance of each other within the document.  The subqueries
 * may appear in the document in any order.
 *
 * N.B. Positions must be included in the index for this query to work
 *
 * Implements the AND operator as defined in <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 *
 * @lucene.experimental
 */

public class UnorderedNearQuery extends PositionFilterQuery {

  /**
   * Constructs an OrderedNearQuery
   * @param slop the maximum distance between the subquery matches
   * @param subqueries the subqueries to match.
   */
  public UnorderedNearQuery(int slop, Query... subqueries) {
    super(buildBooleanQuery(subqueries), new UnorderedNearScorerFactory(slop));
  }

  private static class UnorderedNearScorerFactory implements ScorerFilterFactory {

    private final int slop;

    UnorderedNearScorerFactory(int slop) {
      this.slop = slop;
    }

    @Override
    public Scorer scorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
      return new WithinFilteredScorer(new UnorderedNearScorer(filteredScorer, simScorer), slop, simScorer);
    }

    @Override
    public String getName() {
      return "UnorderedNear/" + slop;
    }
  }

  private static class UnorderedNearScorer extends PositionFilteredScorer {

    SpanningPositionQueue posQueue;

    public UnorderedNearScorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
      super(filteredScorer, simScorer);
      posQueue = new SpanningPositionQueue(subScorers);
    }

    @Override
    protected int doNextPosition() throws IOException {
      while (posQueue.isFull() && posQueue.span.begin == current.begin) {
        posQueue.nextPosition();
      }
      if (!posQueue.isFull())
        return NO_MORE_POSITIONS;
      do {
        //current.update(posQueue.top().interval, posQueue.span);
        posQueue.updateCurrent();
        if (current.equals(posQueue.top().docsEnum))
          return current.begin;
        matchDistance = posQueue.getMatchDistance();
        posQueue.nextPosition();
      } while (posQueue.isFull() && current.end == posQueue.span.end);
      return current.begin;
    }

    @Override
    protected void reset(int doc) throws IOException {
      super.reset(doc);
      current.reset();
      posQueue.advanceTo(doc);
    }


    private class SpanningPositionQueue extends PositionQueue {

      Interval span = new Interval();
      int scorerCount;
      int firstIntervalEnd;
      int lastIntervalBegin;

      public SpanningPositionQueue(Scorer[] subScorers) {
        super(subScorers);
        scorerCount = subScorers.length;
      }

      public int getMatchDistance() {
        return lastIntervalBegin - firstIntervalEnd - scorerCount + 1;
      }

      public boolean isFull() {
        return queuesize == scorerCount;
      }

      public void updateCurrent() throws IOException {
        current.update(this.top().docsEnum, span);
        this.firstIntervalEnd = this.top().end;
      }

      private void updateRightExtreme(DocsEnum newRight) throws IOException {
        if (span.end <= newRight.endPosition()) {
          span.update(span, newRight);
          this.lastIntervalBegin = newRight.startPosition();
        }
      }

      protected void updateInternalIntervals() throws IOException {
        updateRightExtreme(top().docsEnum);
      }

      @Override
      public int nextPosition() throws IOException {
        int position;
        if ((position = super.nextPosition()) == DocsEnum.NO_MORE_POSITIONS) {
          return DocsEnum.NO_MORE_POSITIONS;
        }
        span.update(top().docsEnum, span);
        return position;
      }

      @Override
      protected void init() throws IOException {
        super.init();
        for (Object docsEnumRef : getHeapArray()) {
          if (docsEnumRef != null) {
            updateRightExtreme(((DocsEnumRef) docsEnumRef).docsEnum);
          }
        }
      }

      @Override
      public void advanceTo(int doc) {
        super.advanceTo(doc);
        span.reset();
        firstIntervalEnd = lastIntervalBegin = span.begin;
      }

      @Override
      protected boolean lessThan(DocsEnumRef left, DocsEnumRef right) {
        int c = left.compareTo(right);
        if (c != 0)
          return c < 0;
        return left.end > right.end;
      }

    }

    @Override
    public float score() throws IOException {
      return this.simScorer.score(docID(), sloppyFreq());
    }

    private float sloppyFreq() throws IOException {
      float f = 0.0f;
      while (nextPosition() != NO_MORE_POSITIONS) {
        f += this.simScorer.computeSlopFactor(matchDistance);
      }
      return f;
    }
  }

}

