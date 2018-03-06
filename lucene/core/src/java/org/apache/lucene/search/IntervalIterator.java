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
 * Defines methods to iterate over the intervals that a {@link Scorer} matches
 * on a document
 */
public interface IntervalIterator {

  /**
   * When returned from {@link #nextInterval()}, indicates that there are no more
   * matching intervals on the current document
   */
  int NO_MORE_INTERVALS = Integer.MAX_VALUE;

  DocIdSetIterator approximation();

  boolean advanceTo(int doc) throws IOException;

  /**
   * The start of the current interval
   */
  int start();

  /**
   * The end of the current interval
   */
  int end();

  /**
   * The width of the current interval
   */
  int innerWidth();

  /**
   * Advance the iterator to the next interval
   *
   * @return the starting interval of the next interval, or {@link IntervalIterator#NO_MORE_INTERVALS} if
   *         there are no more intervals on the current document
   */
  int nextInterval() throws IOException;

  /**
   * The score of the current interval
   */
  default float score() {
    return (float) (1.0 / (1 + innerWidth()));
  }

  float cost();

}
