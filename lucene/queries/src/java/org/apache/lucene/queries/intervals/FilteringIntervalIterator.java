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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.Arrays;

abstract class FilteringIntervalIterator extends ConjunctionIntervalIterator {

  final IntervalIterator a;
  final IntervalIterator b;

  boolean bpos;

  protected FilteringIntervalIterator(IntervalIterator a, IntervalIterator b) {
    super(Arrays.asList(a, b));
    this.a = a;
    this.b = b;
  }

  @Override
  public int start() {
    if (bpos == false) {
      return NO_MORE_INTERVALS;
    }
    return a.start();
  }

  @Override
  public int end() {
    if (bpos == false) {
      return NO_MORE_INTERVALS;
    }
    return a.end();
  }

  @Override
  public int gaps() {
    return a.gaps();
  }

  @Override
  protected void reset() throws IOException {
    bpos = b.nextInterval() != NO_MORE_INTERVALS;
  }
}
