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

class AssertingMatchesIterator implements MatchesIterator {

  private final MatchesIterator in;
  private State state = State.UNPOSITIONED;

  private enum State { UNPOSITIONED, ITERATING, EXHAUSTED }

  AssertingMatchesIterator(MatchesIterator in) {
    this.in = in;
  }

  @Override
  public boolean next() throws IOException {
    assert state != State.EXHAUSTED : state;
    boolean more = in.next();
    if (more == false) {
      state = State.EXHAUSTED;
    }
    else {
      state = State.ITERATING;
    }
    return more;
  }

  @Override
  public int startPosition() {
    assert state == State.ITERATING : state;
    return in.startPosition();
  }

  @Override
  public int endPosition() {
    assert state == State.ITERATING : state;
    return in.endPosition();
  }

  @Override
  public int startOffset() throws IOException {
    assert state == State.ITERATING : state;
    return in.startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    assert state == State.ITERATING : state;
    return in.endOffset();
  }

  @Override
  public MatchesIterator getSubMatches() throws IOException {
    assert state == State.ITERATING : state;
    return in.getSubMatches();
  }

  @Override
  public Query getQuery() {
    assert state == State.ITERATING : state;
    return in.getQuery();
  }
}
