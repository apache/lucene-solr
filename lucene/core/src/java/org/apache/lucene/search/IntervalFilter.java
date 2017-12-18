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
  public DocIdSetIterator approximation() {
    return in.approximation();
  }

  @Override
  public void advanceTo(int doc) throws IOException {
    in.advanceTo(doc);
  }
}
