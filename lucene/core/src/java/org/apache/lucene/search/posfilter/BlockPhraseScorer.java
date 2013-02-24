package org.apache.lucene.search.posfilter;

import org.apache.lucene.search.Scorer;

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
public class BlockPhraseScorer extends PositionFilteredScorer {

  private final Interval[] subIntervals;

  public BlockPhraseScorer(Scorer filteredScorer) {
    super(filteredScorer);
    subIntervals = new Interval[subScorers.length];
    for (int i = 0; i < subScorers.length; i++) {
      subIntervals[i] = new Interval();
    }
  }

  @Override
  public void reset(int doc) throws IOException {
    super.reset(doc);
    for (int i = 0; i < subScorers.length; i++) {
      subIntervals[i].reset();
    }
  }

  @Override
  protected int doNextPosition() throws IOException {
    if (subScorers[0].nextPosition() == NO_MORE_POSITIONS)
      return NO_MORE_POSITIONS;
    subIntervals[0].update(subScorers[0]);
    int i = 1;
    while (i < subScorers.length) {
      while (subIntervals[i].begin <= subIntervals[i - 1].end) {
        if (subScorers[i].nextPosition() == NO_MORE_POSITIONS)
          return NO_MORE_POSITIONS;
        subIntervals[i].update(subScorers[i]);
      }
      if (subIntervals[i].begin == subIntervals[i - 1].end + 1) {
        i++;
      }
      else {
        if (subScorers[0].nextPosition() == NO_MORE_POSITIONS)
          return NO_MORE_POSITIONS;
        subIntervals[0].update(subScorers[0]);
        i = 1;
      }
    }
    current.update(subIntervals[0], subIntervals[subScorers.length - 1]);
    return subScorers[0].startPosition();
  }
}
