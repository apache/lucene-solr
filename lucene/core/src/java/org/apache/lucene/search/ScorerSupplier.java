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

/**
 * A supplier of {@link Scorer}. This allows to get an estimate of the cost before
 * building the {@link Scorer}.
 */
public abstract class ScorerSupplier {

  /**
   * Get the {@link Scorer}. This may not return {@code null} and must be called
   * at most once.
   * @param randomAccess A hint about the expected usage of the {@link Scorer}.
   * If {@link DocIdSetIterator#advance} or {@link TwoPhaseIterator} will be
   * used to check whether given doc ids match, then pass {@code true}.
   * Otherwise if the {@link Scorer} will be mostly used to lead the iteration
   * using {@link DocIdSetIterator#nextDoc()}, then {@code false} should be
   * passed. Under doubt, pass {@code false} which usually has a better
   * worst-case.
   */
  public abstract Scorer get(boolean randomAccess) throws IOException;

  /**
   * Get an estimate of the {@link Scorer} that would be returned by {@link #get}.
   * This may be a costly operation, so it should only be called if necessary.
   * @see DocIdSetIterator#cost
   */
  public abstract long cost();

}
