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
 * Defines methods to iterate over the intervals that a term, phrase or more
 * complex positional query matches on a document
 *
 * The iterator is advanced by calling {@link DocIdSetIterator#advance(int)} on the
 * DocIdSetIterator returned by {@link #approximation()}.  Consumers should then call
 * {@link #nextInterval()} to retrieve intervals until {@link #NO_MORE_INTERVALS} is returned.
 */
public interface IntervalIterator {

  /**
   * When returned from {@link #nextInterval()}, indicates that there are no more
   * matching intervals on the current document
   */
  int NO_MORE_INTERVALS = Integer.MAX_VALUE;

  /**
   * An iterator over documents that might have matching intervals
   */
  DocIdSetIterator approximation();

  /**
   * The start of the current interval
   *
   * Returns -1 if {@link #nextInterval()} has not yet been called
   */
  int start();

  /**
   * The end of the current interval
   *
   * Returns -1 if {@link #nextInterval()} has not yet been called
   */
  int end();

  /**
   * Advance the iterator to the next interval
   *
   * @return the starting interval of the next interval, or {@link IntervalIterator#NO_MORE_INTERVALS} if
   *         there are no more intervals on the current document
   */
  int nextInterval() throws IOException;

  /**
   * An indication of the average cost of iterating over all intervals in a document
   *
   * @see TwoPhaseIterator#matchCost()
   */
  float cost();

}
