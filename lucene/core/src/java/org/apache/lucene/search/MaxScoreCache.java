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
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.ArrayUtil;

/**
 * Compute maximum scores based on {@link Impacts} and keep them in a cache in
 * order not to run expensive similarity score computations multiple times on
 * the same data.
 */
public final class MaxScoreCache {

  private final ImpactsEnum impactsEnum;
  private final SimScorer scorer;
  private final float globalMaxScore;
  private float[] maxScoreCache;
  private int[] maxScoreCacheUpTo;

  /**
   * Sole constructor.
   */
  public MaxScoreCache(ImpactsEnum impactsEnum, SimScorer scorer) {
    this.impactsEnum = impactsEnum;
    this.scorer = scorer;
    globalMaxScore = scorer.score(Integer.MAX_VALUE, 1L);
    maxScoreCache = new float[0];
    maxScoreCacheUpTo = new int[0];
  }

  private void ensureCacheSize(int size) {
    if (maxScoreCache.length < size) {
      int oldLength = maxScoreCache.length;
      maxScoreCache = ArrayUtil.grow(maxScoreCache, size);
      maxScoreCacheUpTo = Arrays.copyOf(maxScoreCacheUpTo, maxScoreCache.length);
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
  private int getLevel(int upTo) throws IOException {
    final Impacts impacts = impactsEnum.getImpacts();
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
    final Impacts impacts = impactsEnum.getImpacts();
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
  int getSkipLevel(float minScore) throws IOException {
    final Impacts impacts = impactsEnum.getImpacts();
    final int numLevels = impacts.numLevels();
    for (int level = 0; level < numLevels; ++level) {
      if (getMaxScoreForLevel(level) >= minScore) {
        return level - 1;
      }
    }
    return numLevels - 1;
  }

  /**
   * Implement the contract of {@link Scorer#advanceShallow(int)} based on the
   * wrapped {@link ImpactsEnum}.
   * @see Scorer#advanceShallow(int)
   */
  public int advanceShallow(int target) throws IOException {
    impactsEnum.advanceShallow(target);
    Impacts impacts = impactsEnum.getImpacts();
    return impacts.getDocIdUpTo(0);
  }

  /**
   * Implement the contract of {@link Scorer#getMaxScore(int)} based on the
   * wrapped {@link ImpactsEnum} and {@link Scorer}.
   * @see Scorer#getMaxScore(int)
   */
  public float getMaxScore(int upTo) throws IOException {
    final int level = getLevel(upTo);
    if (level == -1) {
      return globalMaxScore;
    } else {
      return getMaxScoreForLevel(level);
    }
  }
}
