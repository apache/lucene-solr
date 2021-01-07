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

package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/** Expert: Weight class for phrase matching */
public abstract class PhraseWeight extends Weight {

  final ScoreMode scoreMode;
  final Similarity.SimScorer stats;
  final Similarity similarity;
  final String field;

  /**
   * Expert: Creates PhraseWeight instance
   *
   * @lucene.internal
   */
  protected PhraseWeight(Query query, String field, IndexSearcher searcher, ScoreMode scoreMode)
      throws IOException {
    super(query);
    this.scoreMode = scoreMode;
    this.field = field;
    this.similarity = searcher.getSimilarity();
    SimScorer stats = getStats(searcher);
    if (stats == null) { // Means no terms or scores are not needed
      stats =
          new SimScorer() {
            @Override
            public float score(float freq, long norm) {
              return 1;
            }
          };
    }
    this.stats = stats;
  }

  protected abstract Similarity.SimScorer getStats(IndexSearcher searcher) throws IOException;

  protected abstract PhraseMatcher getPhraseMatcher(
      LeafReaderContext context, SimScorer scorer, boolean exposeOffsets) throws IOException;

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    PhraseMatcher matcher = getPhraseMatcher(context, stats, false);
    if (matcher == null) return null;
    LeafSimScorer simScorer =
        new LeafSimScorer(stats, context.reader(), field, scoreMode.needsScores());
    return new PhraseScorer(this, matcher, scoreMode, simScorer);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    PhraseMatcher matcher = getPhraseMatcher(context, stats, false);
    if (matcher == null || matcher.approximation().advance(doc) != doc) {
      return Explanation.noMatch("no matching terms");
    }
    matcher.reset();
    if (matcher.nextMatch() == false) {
      return Explanation.noMatch("no matching phrase");
    }
    float freq = matcher.sloppyWeight();
    while (matcher.nextMatch()) {
      freq += matcher.sloppyWeight();
    }
    LeafSimScorer docScorer =
        new LeafSimScorer(stats, context.reader(), field, scoreMode.needsScores());
    Explanation freqExplanation = Explanation.match(freq, "phraseFreq=" + freq);
    Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
    return Explanation.match(
        scoreExplanation.getValue(),
        "weight("
            + getQuery()
            + " in "
            + doc
            + ") ["
            + similarity.getClass().getSimpleName()
            + "], result of:",
        scoreExplanation);
  }

  @Override
  public Matches matches(LeafReaderContext context, int doc) throws IOException {
    return MatchesUtils.forField(
        field,
        () -> {
          PhraseMatcher matcher = getPhraseMatcher(context, stats, true);
          if (matcher == null || matcher.approximation().advance(doc) != doc) {
            return null;
          }
          matcher.reset();
          if (matcher.nextMatch() == false) {
            return null;
          }
          return new MatchesIterator() {
            boolean started = false;

            @Override
            public boolean next() throws IOException {
              if (started == false) {
                return started = true;
              }
              return matcher.nextMatch();
            }

            @Override
            public int startPosition() {
              return matcher.startPosition();
            }

            @Override
            public int endPosition() {
              return matcher.endPosition();
            }

            @Override
            public int startOffset() throws IOException {
              return matcher.startOffset();
            }

            @Override
            public int endOffset() throws IOException {
              return matcher.endOffset();
            }

            @Override
            public MatchesIterator getSubMatches() throws IOException {
              return null; // phrases are treated as leaves
            }

            @Override
            public Query getQuery() {
              return PhraseWeight.this.getQuery();
            }
          };
        });
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
