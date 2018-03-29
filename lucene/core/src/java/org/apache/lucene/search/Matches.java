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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reports the positions and optionally offsets of all matching terms in a query
 * for a single document
 *
 * To find all fields that have matches, call {@link #getMatchFields()}
 *
 * To obtain a {@link MatchesIterator} for a particular field, call {@link #getMatches(String)}
 */
public class Matches {

  private final Map<String, MatchesIterator> matches;

  /**
   * Create a simple {@link Matches} for a single field
   */
  public static Matches fromField(String field, MatchesIterator it) {
    if (it == null) {
      return null;
    }
    return new Matches(field, it);
  }

  /**
   * Amalgamate a collection of {@link Matches} into a single object
   */
  public static Matches fromSubMatches(List<Matches> subMatches) throws IOException {
    if (subMatches == null || subMatches.size() == 0) {
      return null;
    }
    if (subMatches.size() == 1) {
      return subMatches.get(0);
    }
    Map<String, MatchesIterator> matches = new HashMap<>();
    Set<String> allFields = new HashSet<>();
    for (Matches m : subMatches) {
      allFields.addAll(m.getMatchFields());
    }
    for (String field : allFields) {
      List<MatchesIterator> mis = new ArrayList<>();
      for (Matches m : subMatches) {
        MatchesIterator mi = m.getMatches(field);
        if (mi != null) {
          mis.add(mi);
        }
      }
      matches.put(field, DisjunctionMatchesIterator.fromSubIterators(mis));
    }
    return new Matches(matches);
  }

  /**
   * Create a {@link Matches} from a map of fields to iterators
   */
  protected Matches(Map<String, MatchesIterator> matches) {
    this.matches = matches;
  }

  private Matches(String field, MatchesIterator iterator) {
    this.matches = new HashMap<>();
    this.matches.put(field, iterator);
  }

  /**
   * Returns a {@link MatchesIterator} over the matches for a single field,
   * or {@code null} if there are no matches in that field
   */
  public MatchesIterator getMatches(String field) {
    return matches.get(field);
  }

  /**
   * Returns the fields with matches for this document
   */
  public Set<String> getMatchFields() {
    return matches.keySet();
  }
}
