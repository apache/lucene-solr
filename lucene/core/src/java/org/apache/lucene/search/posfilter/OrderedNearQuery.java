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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A query that matches if a set of subqueries also match, and are within
 * a given distance of each other within the document.  The subqueries
 * must appear in the document in order.
 *
 * N.B. Positions must be included in the index for this query to work
 *
 * Implements the AND&lt; operator as defined in <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 *
 * @lucene.experimental
 */

public class OrderedNearQuery extends PositionFilterQuery {

  /**
   * Constructs an OrderedNearQuery
   * @param slop the maximum distance between the subquery matches
   * @param subqueries the subqueries to match.
   */
  public OrderedNearQuery(int slop, Query... subqueries) {
    super(buildBooleanQuery(subqueries), new OrderedNearScorerFactory(slop));
  }

  private static class OrderedNearScorerFactory implements ScorerFilterFactory {

    private final int slop;

    public OrderedNearScorerFactory(int slop) {
      this.slop = slop;
    }

    @Override
    public Scorer scorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
      return new WithinFilteredScorer(new OrderedNearScorer(filteredScorer, simScorer), slop, simScorer);
    }

    @Override
    public String getName() {
      return "OrderedNear/" + slop;
    }
  }

  private static class OrderedNearScorer extends PositionFilteredScorer {

    private final int lastiter;

    private int index = 1;
    private Interval[] intervals;

    public OrderedNearScorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
      super(filteredScorer, simScorer);
      intervals = new Interval[subScorers.length];
      for (int i = 0; i < subScorers.length; i++) {
        intervals[i] = new Interval();
      }
      lastiter = intervals.length - 1;
    }

    @Override
    public int freq() throws IOException {
      return 1; // nocommit
    }

    @Override
    protected void reset(int doc) throws IOException {
      for (int i = 0; i < subScorers.length; i++) {
        assert subScorers[i].docID() == doc;
        intervals[i].update(Interval.INFINITE_INTERVAL);
      }
      if (subScorers[0].nextPosition() == NO_MORE_POSITIONS)
        intervals[0].setMaximum();
      else
        intervals[0].update(subScorers[0]);
      index = 1;
    }

    @Override
    protected int doNextPosition() throws IOException {
      if (intervals[0].begin == NO_MORE_POSITIONS)
        return NO_MORE_POSITIONS;
      current.setMaximum();
      int b = Integer.MAX_VALUE;
      while (true) {
        while (true) {
          final Interval previous = intervals[index - 1];
          if (previous.end >= b) {
            return current.begin;
          }
          if (index == intervals.length || intervals[index].begin > previous.end)
            break;
          Interval scratch = intervals[index];
          do {
            if (scratch.end >= b || subScorers[index].nextPosition() == NO_MORE_POSITIONS)
              return current.begin;
            intervals[index].update(subScorers[index]);
            scratch = intervals[index];
          } while (scratch.begin <= previous.end);
          index++;
        }
        current.update(intervals[0], intervals[lastiter]);
        matchDistance = (intervals[lastiter].begin - lastiter) - intervals[0].end;
        b = intervals[lastiter].begin;
        index = 1;
        if (subScorers[0].nextPosition() == NO_MORE_POSITIONS) {
          intervals[0].setMaximum();
          return current.begin;
        }
        intervals[0].update(subScorers[0]);
      }
    }
  }
}
