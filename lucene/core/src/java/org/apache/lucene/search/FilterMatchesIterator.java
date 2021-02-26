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

/** A MatchesIterator that delegates all calls to another MatchesIterator */
public abstract class FilterMatchesIterator implements MatchesIterator {

  /** The delegate */
  protected final MatchesIterator in;

  /**
   * Create a new FilterMatchesIterator
   *
   * @param in the delegate
   */
  protected FilterMatchesIterator(MatchesIterator in) {
    this.in = in;
  }

  @Override
  public boolean next() throws IOException {
    return in.next();
  }

  @Override
  public int startPosition() {
    return in.startPosition();
  }

  @Override
  public int endPosition() {
    return in.endPosition();
  }

  @Override
  public int startOffset() throws IOException {
    return in.startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    return in.endOffset();
  }

  @Override
  public MatchesIterator getSubMatches() throws IOException {
    return in.getSubMatches();
  }

  @Override
  public Query getQuery() {
    return in.getQuery();
  }
}
