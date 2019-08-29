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

/**
 * Default implementation of HitsThresholdChecker to be used for single threaded execution
 */
class LocalHitsThresholdChecker extends HitsThresholdChecker {
  private final int totalHitsThreshold;
  private int hitCount;

  public LocalHitsThresholdChecker(int totalHitsThreshold) {

    if (totalHitsThreshold < 0) {
      throw new IllegalArgumentException("totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
    }

    this.totalHitsThreshold = totalHitsThreshold;
  }

  @Override
  public void incrementHitCount() {
    ++hitCount;
  }

  @Override
  public boolean getAsBoolean() {
    return hitCount > totalHitsThreshold;
  }

  @Override
  public ScoreMode scoreMode() {
    return totalHitsThreshold == Integer.MAX_VALUE ? ScoreMode.COMPLETE : ScoreMode.TOP_SCORES;
  }

  @Override
  public int getHitsThreshold() {
    return totalHitsThreshold;
  }
}
