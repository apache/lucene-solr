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
package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

/** A {@link Collector} for the maximum score value. */
public class MaxScoreCollector extends SimpleCollector {
  private Scorable scorer;
  private float maxScore = Float.MIN_VALUE;
  private boolean collectedAnyHits = false;

  public MaxScoreCollector() {}

  public float getMaxScore() {
    return collectedAnyHits ? maxScore : Float.NaN;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.TOP_SCORES;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    if (maxScore == Float.MIN_VALUE) {
      scorer.setMinCompetitiveScore(0f);
    } else {
      scorer.setMinCompetitiveScore(Math.nextUp(maxScore));
    }
    this.scorer = scorer;
  }

  @Override
  public void collect(int doc) throws IOException {
    collectedAnyHits = true;
    float docScore = scorer.score();
    if (Float.compare(docScore, maxScore) > 0) {
      maxScore = docScore;
      scorer.setMinCompetitiveScore(Math.nextUp(maxScore));
    }
  }
}
