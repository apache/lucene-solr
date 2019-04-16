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

package org.apache.lucene.luwak.matchers;

import java.io.IOException;

import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.DocumentBatch.Builder;
import org.apache.lucene.luwak.MatcherFactory;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A Matcher that reports the scores of queries run against its DocumentBatch
 * <p>
 * To change the {@link Similarity} implementation used for scoring here, use
 * {@link Builder#setSimilarity(Similarity)} when building the
 * batch.
 */
public class ScoringMatcher extends CollectingMatcher<ScoringMatch> {

  private ScoringMatcher(DocumentBatch docs) {
    super(docs, ScoreMode.COMPLETE);
  }

  @Override
  protected ScoringMatch doMatch(String queryId, String docId, Scorable scorer) throws IOException {
    float score = scorer.score();
    if (score > 0)
      return new ScoringMatch(queryId, docId, score);
    return null;
  }

  @Override
  public ScoringMatch resolve(ScoringMatch match1, ScoringMatch match2) {
    return new ScoringMatch(match1.getQueryId(), match1.getDocId(), match1.getScore() + match2.getScore());
  }

  /**
   * A MatcherFactory for ScoringMatcher objects
   */
  public static final MatcherFactory<ScoringMatch> FACTORY = ScoringMatcher::new;

}
