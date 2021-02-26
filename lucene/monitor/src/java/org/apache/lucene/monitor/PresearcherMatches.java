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

package org.apache.lucene.monitor;

import java.util.Map;

/**
 * Wraps a {@link MultiMatchingQueries} with information on which presearcher queries were selected
 */
public class PresearcherMatches<T extends QueryMatch> {

  private final Map<String, StringBuilder> matchingTerms;

  /** The wrapped Matches */
  public final MultiMatchingQueries<T> matcher;

  /** Builds a new PresearcherMatches */
  public PresearcherMatches(
      Map<String, StringBuilder> matchingTerms, MultiMatchingQueries<T> matcher) {
    this.matcher = matcher;
    this.matchingTerms = matchingTerms;
  }

  /** Returns match information for a given query */
  public PresearcherMatch<T> match(String queryId, int doc) {
    StringBuilder found = matchingTerms.get(queryId);
    if (found != null)
      return new PresearcherMatch<>(queryId, found.toString(), matcher.matches(queryId, doc));
    return null;
  }
}
