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
 * Wraps an {@link IntervalIterator} and passes through those intervals that match the {@link #accept()} function
 */
public abstract class IntervalFilter extends FilterIntervalIterator {

  /**
   * Filter an {@link IntervalIterator} by its outer width, ie the distance between the
   * start and end of the iterator
   */
  public static IntervalIterator widthFilter(IntervalIterator in, int minWidth, int maxWidth) {
    return new IntervalFilter(in) {
      @Override
      protected boolean accept() {
        int width = end() - start();
        return width >= minWidth && width <= maxWidth;
      }
    };
  }

  /**
   * Filter an {@link IntervalIterator} by its inner width, ie the distance between the
   * end of its first subiterator and the beginning of its last
   */
  public static IntervalIterator innerWidthFilter(IntervalIterator in, int minWidth, int maxWidth) {
    return new IntervalFilter(in) {
      @Override
      protected boolean accept() {
        int width = innerWidth();
        return width >= minWidth && width <= maxWidth;
      }

      @Override
      public String toString() {
        return "widthfilter(" + minWidth + "," + maxWidth + "," + in.toString() + ")";
      }
    };
  }

  /**
   * Create a new filter
   */
  public IntervalFilter(IntervalIterator in) {
    super(in);
  }

  /**
   * @return {@code true} if the wrapped iterator's interval should be passed on
   */
  protected abstract boolean accept();

  @Override
  public final int nextInterval() throws IOException {
    int next;
    do {
      next = in.nextInterval();
    }
    while (accept() == false && next != IntervalIterator.NO_MORE_INTERVALS);
    return next;
  }

}
