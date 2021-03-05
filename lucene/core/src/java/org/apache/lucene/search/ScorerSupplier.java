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
 * A supplier of {@link Scorer}. This allows to get an estimate of the cost before building the
 * {@link Scorer}.
 */
public abstract class ScorerSupplier {

  /**
   * Get the {@link Scorer}. This may not return {@code null} and must be called at most once.
   *
   * @param leadCost Cost of the scorer that will be used in order to lead iteration. This can be
   *     interpreted as an upper bound of the number of times that {@link DocIdSetIterator#nextDoc},
   *     {@link DocIdSetIterator#advance} and {@link TwoPhaseIterator#matches} will be called. Under
   *     doubt, pass {@link Long#MAX_VALUE}, which will produce a {@link Scorer} that has good
   *     iteration capabilities.
   */
  public abstract Scorer get(long leadCost) throws IOException;

  /**
   * Get an estimate of the {@link Scorer} that would be returned by {@link #get}. This may be a
   * costly operation, so it should only be called if necessary.
   *
   * @see DocIdSetIterator#cost
   */
  public abstract long cost();
}
