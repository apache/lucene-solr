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

// Simulated annealing seems to help by avoiding getting stuck in local minima
abstract class BoundsChecker {

    private final static float DELTA = 0.2f;
    private final static float BETA = 1000f;

    float decay;
    float bound;
    float delta;

    /**
     * Update the bound if sample is better
     */
    abstract void update(float sample);

    /**
     * Return whether the sample exceeds (is worse than) the bound
     */
    abstract boolean check(float sample);

    /**
     * Set the bound, and scale the annealing delta according to an estimate of the expected range of values
     * @param worst the worst value seen so far
     * @param best the best value seen so far
     */
    abstract void set(float worst, float best);

    static BoundsChecker create(boolean reversed) {
        if (reversed) {
            return new Min();
        } else {
            return new Max();
        }
    }

    static class Max extends BoundsChecker {
        Max() {
            bound = -Float.MAX_VALUE;
            decay = 1f - 1f / BETA;
        }

        void update(float sample) {
            if (sample > bound) {
                bound = sample;
            }
        }

        void set(float worst, float best) {
            assert worst <= best;
            delta = (best - worst) * DELTA;
            bound = worst;
        }

        boolean check(float sample) {
            delta *= decay;
            return sample < bound - delta;
        }
    }

    static class Min extends BoundsChecker {

        Min() {
            bound = Float.MAX_VALUE;
            decay = 1f - 1f / BETA;
        }

        void update(float sample) {
            if (sample < bound) {
                bound = sample;
            }
        }

        void set(float worst, float best) {
            assert worst >= best;
            delta = (worst - best) * DELTA;
            bound = worst;
        }

        boolean check(float sample) {
            delta *= decay;
            return sample > bound + delta;
        }
    }
}
