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
import java.util.Collection;

/**
 * Reports the positions and optionally offsets of all matching terms in a query for a single
 * document
 *
 * <p>To obtain a {@link MatchesIterator} for a particular field, call {@link #getMatches(String)}.
 * Note that you can call {@link #getMatches(String)} multiple times to retrieve new iterators, but
 * it is not thread-safe.
 *
 * @lucene.experimental
 */
public interface Matches extends Iterable<String> {

  /**
   * Returns a {@link MatchesIterator} over the matches for a single field, or {@code null} if there
   * are no matches in that field.
   */
  MatchesIterator getMatches(String field) throws IOException;

  /**
   * Returns a collection of Matches that make up this instance; if it is not a composite, then this
   * returns an empty list
   */
  Collection<Matches> getSubMatches();
}
