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

package org.apache.lucene.util.hnsw;

abstract class BoundsChecker {

  float bound;

  /** Update the bound if sample is better */
  abstract void update(float sample);

  /** Update the bound unconditionally */
  void set(float sample) {
    bound = sample;
  }

  /** @return whether the sample exceeds (is worse than) the bound */
  abstract boolean check(float sample);

  static BoundsChecker create(boolean reversed) {
    if (reversed) {
      return new Min();
    } else {
      return new Max();
    }
  }

  static class Max extends BoundsChecker {
    Max() {
      bound = Float.NEGATIVE_INFINITY;
    }

    void update(float sample) {
      if (sample > bound) {
        bound = sample;
      }
    }

    boolean check(float sample) {
      return sample < bound;
    }
  }

  static class Min extends BoundsChecker {

    Min() {
      bound = Float.POSITIVE_INFINITY;
    }

    void update(float sample) {
      if (sample < bound) {
        bound = sample;
      }
    }

    boolean check(float sample) {
      return sample > bound;
    }
  }
}
