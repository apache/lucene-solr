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

public class RangeFilterQuery extends PositionFilterQuery {

  public RangeFilterQuery(int start, int end, Query innerQuery) {
    super(innerQuery, new RangeFilterScorerFactory(start, end));
  }

  public RangeFilterQuery(int end, Query innerQuery) {
    this(0, end, innerQuery);
  }

  private static class RangeFilterScorerFactory implements ScorerFilterFactory {

    private final int start;
    private final int end;

    public RangeFilterScorerFactory(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public Scorer scorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
      return new RangeFilterScorer(start, end, filteredScorer, simScorer);
    }

    @Override
    public String getName() {
      return "RangeFilter(" + start + "," + end + ")";
    }
  }

  private static class RangeFilterScorer extends PositionFilteredScorer {

    private final int start;
    private final int end;

    public RangeFilterScorer(int start, int end, Scorer filteredScorer, Similarity.SimScorer simScorer) {
      super(filteredScorer, simScorer);
      this.start = start;
      this.end = end;
    }

    @Override
    public float intervalScore() throws IOException {
      return child.intervalScore();
    }

    @Override
    protected int doNextPosition() throws IOException {
      int position;
      while ((position = child.nextPosition()) != NO_MORE_POSITIONS) {
        if (position > end)
          return NO_MORE_POSITIONS;
        if (position >= start) {
          current.update(child);
          return position;
        }
      }
      return NO_MORE_POSITIONS;
    }
  }

}
