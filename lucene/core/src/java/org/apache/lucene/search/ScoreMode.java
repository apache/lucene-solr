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
 * Different modes of search.
 */
public enum ScoreMode {
  
  /**
   * Produced scorers will allow visiting all matches and get their score.
   */
  COMPLETE {
    @Override
    public boolean needsScores() {
      return true;
    }
  },

  /**
   * Produced scorers will allow visiting all matches but scores won't be
   * available.
   */
  COMPLETE_NO_SCORES {
    @Override
    public boolean needsScores() {
      return false;
    }
  },

  /**
   * Produced scorers will optionally allow skipping over non-competitive
   * hits using the {@link Scorer#setMinCompetitiveScore(float)} API.
   */
  TOP_SCORES {
    @Override
    public boolean needsScores() {
      return true;
    }
  };

  /**
   * Whether this {@link ScoreMode} needs to compute scores.
   */
  public abstract boolean needsScores();
}
