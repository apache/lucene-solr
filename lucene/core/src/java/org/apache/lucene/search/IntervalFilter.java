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
public abstract class IntervalFilter extends IntervalIterator {

  private final IntervalIterator in;

  /**
   * Create a new filter
   */
  public IntervalFilter(IntervalIterator in) {
    super(in.approximation);
    this.in = in;
  }

  @Override
  public int start() {
    return in.start();
  }

  @Override
  public int end() {
    return in.end();
  }

  @Override
  public float matchCost() {
    return in.matchCost();
  }

  @Override
  protected void reset() throws IOException {
    in.reset();
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
