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
 * Maintains the bottom value across multiple collectors
 */
abstract class BottomValueChecker {
  /** Maintains global bottom score as the maximum of all bottom scores */
  private static class MaximumBottomScoreChecker extends BottomValueChecker {
    private volatile float maxMinScore;

    @Override
    public void updateThreadLocalBottomValue(float value) {
      if (value <= maxMinScore) {
        return;
      }
      synchronized (this) {
        if (value > maxMinScore) {
          maxMinScore = value;
        }
      }
    }

    @Override
    public float getBottomValue() {
      return maxMinScore;
    }
  }

  public static BottomValueChecker createMaxBottomScoreChecker() {
    return new MaximumBottomScoreChecker();
  }

  public abstract void updateThreadLocalBottomValue(float value);
  public abstract float getBottomValue();
}