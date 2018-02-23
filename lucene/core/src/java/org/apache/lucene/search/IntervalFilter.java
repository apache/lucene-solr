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

public abstract class IntervalFilter implements IntervalIterator {

  public static IntervalIterator widthFilter(IntervalIterator in, int minWidth, int maxWidth) {
    return new IntervalFilter(in) {
      @Override
      protected boolean accept() {
        int width = end() - start();
        return width >= minWidth && width <= maxWidth;
      }
    };
  }

  public static IntervalIterator innerWidthFilter(IntervalIterator in, int minWidth, int maxWidth) {
    return new IntervalFilter(in) {
      @Override
      protected boolean accept() {
        int width = innerWidth();
        return width >= minWidth && width <= maxWidth;
      }
    };
  }

  private final IntervalIterator in;

  public IntervalFilter(IntervalIterator in) {
    this.in = in;
  }

  protected abstract boolean accept();

  @Override
  public final int nextInterval() throws IOException {
    int next;
    do {
      next = in.nextInterval();
    }
    while (accept() == false && next != Intervals.NO_MORE_INTERVALS);
    return next;
  }

  @Override
  public final int start() {
    return in.start();
  }

  @Override
  public final int end() {
    return in.end();
  }

  @Override
  public int innerWidth() {
    return in.innerWidth();
  }

  @Override
  public boolean reset(int doc) throws IOException {
    return in.reset(doc);
  }
}
