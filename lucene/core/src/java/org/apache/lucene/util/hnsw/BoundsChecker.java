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

// Simulated annealing may help avoid getting stuck in local minima
abstract class BoundsChecker {

    private final static float DELTA = 0.2f;
    private final static float BETA = 10f;

    float decay;
    float bound;
    float delta;

    abstract void update(float sample);

    abstract boolean check(float sample);

    abstract void setWorst(float worst);

    static BoundsChecker create(boolean reversed, int ef) {
        if (reversed) {
            return new Min(ef);
        } else {
            return new Max(ef);
        }
    }

    static class Max extends BoundsChecker {
        Max(int ef) {
            bound = -Float.MAX_VALUE;
            decay = 1f - 1f / (BETA * ef);
        }

        void update(float sample) {
            if (sample > bound) {
                bound = sample;
            }
        }

        void setWorst(float worst) {
            delta = (bound - worst) * DELTA;
            bound = worst;
        }

        boolean check(float sample) {
            delta *= decay;
            return sample < bound - delta;
        }
    }

    static class Min extends BoundsChecker {

        Min(int ef) {
            bound = Float.MAX_VALUE;
            decay = 1f - 1f / (BETA * ef);
        }

        void update(float sample) {
            if (sample < bound) {
                bound = sample;
            }
        }

        void setWorst(float worst) {
            delta = (worst - bound) * DELTA;
            bound = worst;
        }

        boolean check(float sample) {
            delta *= decay;
            return sample > bound + delta;
        }
    }
}
