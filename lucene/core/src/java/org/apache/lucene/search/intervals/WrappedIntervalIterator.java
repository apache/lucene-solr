package org.apache.lucene.search.intervals;

import java.io.IOException;

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

public class WrappedIntervalIterator extends IntervalIterator {

  protected final IntervalIterator inner;

  protected WrappedIntervalIterator(IntervalIterator inner) {
    super(inner.scorer, inner.collectIntervals);
    this.inner = inner;
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    return inner.scorerAdvanced(docId);
  }

  @Override
  public Interval next() throws IOException {
    return inner.next();
  }

  @Override
  public void collect(IntervalCollector collector) {
    inner.collect(collector);
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return inner.subs(inOrder);
  }

  @Override
  public int matchDistance() {
    return inner.matchDistance();
  }

  @Override
  public int docID() {
    return inner.docID();
  }

}
