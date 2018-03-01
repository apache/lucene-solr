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
 * An interval iterator which caches its first invocation.
 *
 * Useful for two-phase queries that confirm matches by checking that at least one
 * interval exists in a given document
 */
class CachedIntervalIterator extends FilterIntervalIterator {

  final Scorer scorer;

  private boolean started = false;

  CachedIntervalIterator(IntervalIterator in, Scorer scorer) {
    super(in);
    this.scorer = scorer;
  }

  @Override
  public boolean reset(int doc) throws IOException {
    // inner iterator already reset() in TwoPhaseIterator.matches()
    started = false;
    return doc == scorer.docID();
  }

  @Override
  public int nextInterval() throws IOException {
    if (started == false) {
      started = true;
      return start();
    }
    return in.nextInterval();
  }
}
