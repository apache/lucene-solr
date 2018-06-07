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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.ArrayUtil;

/**
 * Compute maximum scores based on {@link Impacts} and keep them in a cache in
 * order not to run expensive similarity score computations multiple times on
 * the same data.
 * @lucene.internal
 */
final class MaxScoreCache {

  private final ImpactsSource impactsSource;
  private final SimScorer scorer;
  private float[] maxScoreCache;
  private int[] maxScoreCacheUpTo;

  /**
   * Sole constructor.
   */
  public MaxScoreCache(ImpactsSource impactsSource, SimScorer scorer) {
    this.impactsSource = impactsSource;
    this.scorer = scorer;
    maxScoreCache = new float[0];
    maxScoreCacheUpTo = new int[0];
  }

  private void ensureCacheSize(int size) {
    if (maxScoreCache.length < size) {
      int oldLength = maxScoreCache.length;
      maxScoreCache = ArrayUtil.grow(maxScoreCache, size);
      maxScoreCacheUpTo = ArrayUtil.growExact(maxScoreCacheUpTo, maxScoreCache.length);
      Arrays.fill(maxScoreCacheUpTo, oldLength, maxScoreCacheUpTo.length, -1);
    }
  }

  private float computeMaxScore(List<Impact> impacts) {
    float maxScore = 0;
    for (Impact impact : impacts) {
      maxScore = Math.max(scorer.score(impact.freq, impact.norm), maxScore);
    }
    return maxScore;
  }

  /**
   * Return the first level that includes all doc IDs up to {@code upTo},
   * or -1 if there is no such level.
   */
  int getLevel(int upTo) throws IOException {
    final Impacts impacts = impactsSource.getImpacts();
    for (int level = 0, numLevels = impacts.numLevels(); level < numLevels; ++level) {
      final int impactsUpTo = impacts.getDocIdUpTo(level);
      if (upTo <= impactsUpTo) {
        return level;
      }
    }
    return -1;
  }

  /**
   * Return the maximum score for the given {@code level}.
   */
  float getMaxScoreForLevel(int level) throws IOException {
    final Impacts impacts = impactsSource.getImpacts();
    ensureCacheSize(level + 1);
    final int levelUpTo = impacts.getDocIdUpTo(level);
    if (maxScoreCacheUpTo[level] < levelUpTo) {
      maxScoreCache[level] = computeMaxScore(impacts.getImpacts(level));
      maxScoreCacheUpTo[level] = levelUpTo;
    }
    return maxScoreCache[level];
  }

  /**
   * Return the maximum level at which scores are all less than {@code minScore},
   * or -1 if none.
   */
  private int getSkipLevel(Impacts impacts, float minScore) throws IOException {
    final int numLevels = impacts.numLevels();
    for (int level = 0; level < numLevels; ++level) {
      if (getMaxScoreForLevel(level) >= minScore) {
        return level - 1;
      }
    }
    return numLevels - 1;
  }

  /**
   * Return the an inclusive upper bound of documents that all have a score that
   * is less than {@code minScore}, or {@code -1} if the current document may
   * be competitive.
   */
  int getSkipUpTo(float minScore) throws IOException {
    final Impacts impacts = impactsSource.getImpacts();
    final int level = getSkipLevel(impacts, minScore);
    if (level == -1) {
      return -1;
    }
    return impacts.getDocIdUpTo(level);
  }

}
