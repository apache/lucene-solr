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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Reports the positions and optionally offsets of all matching terms in a query
 * for a single document
 *
 * To obtain a {@link MatchesIterator} for a particular field, call {@link #getMatches(String)}.
 * Note that you can call {@link #getMatches(String)} multiple times to retrieve new
 * iterators, but it is not thread-safe.
 *
 * @lucene.experimental
 */
public interface Matches extends Iterable<String> {

  /**
   * Returns a {@link MatchesIterator} over the matches for a single field,
   * or {@code null} if there are no matches in that field.
   */
  MatchesIterator getMatches(String field) throws IOException;

  /**
   * Indicates a match with no term positions, for example on a Point or DocValues field,
   * or a field indexed as docs and freqs only
   */
  Matches MATCH_WITH_NO_TERMS = new Matches() {
    @Override
    public Iterator<String> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public MatchesIterator getMatches(String field) {
      return null;
    }
  };

  /**
   * Amalgamate a collection of {@link Matches} into a single object
   */
  static Matches fromSubMatches(List<Matches> subMatches) {
    if (subMatches == null || subMatches.size() == 0) {
      return null;
    }
    List<Matches> sm = subMatches.stream().filter(m -> m != MATCH_WITH_NO_TERMS).collect(Collectors.toList());
    if (sm.size() == 0) {
      return MATCH_WITH_NO_TERMS;
    }
    if (sm.size() == 1) {
      return sm.get(0);
    }

    return new Matches() {
      @Override
      public MatchesIterator getMatches(String field) throws IOException {
        List<MatchesIterator> subIterators = new ArrayList<>(sm.size());
        for (Matches m : sm) {
          MatchesIterator it = m.getMatches(field);
          if (it != null) {
            subIterators.add(it);
          }
        }
        return DisjunctionMatchesIterator.fromSubIterators(subIterators);
      }

      @Override
      public Iterator<String> iterator() {
        // for each sub-match, iterate its fields (it's an Iterable of the fields), and return the distinct set
        return sm.stream().flatMap(m -> StreamSupport.stream(m.spliterator(), false)).distinct().iterator();
      }
    };
  }

  /**
   * A functional interface that supplies a {@link MatchesIterator}
   */
  @FunctionalInterface
  interface MatchesIteratorSupplier {
    /** Return a new {@link MatchesIterator} */
    MatchesIterator get() throws IOException;
  }

  /**
   * Create a Matches for a single field
   */
  static Matches forField(String field, MatchesIteratorSupplier mis) throws IOException {

    // The indirection here, using a Supplier object rather than a MatchesIterator
    // directly, is to allow for multiple calls to Matches.getMatches() to return
    // new iterators.  We still need to call MatchesIteratorSupplier.get() eagerly
    // to work out if we have a hit or not.

    MatchesIterator mi = mis.get();
    if (mi == null) {
      return null;
    }
    return new Matches() {
      boolean cached = true;
      @Override
      public MatchesIterator getMatches(String f) throws IOException {
        if (Objects.equals(field, f) == false) {
          return null;
        }
        if (cached == false) {
          return mis.get();
        }
        cached = false;
        return mi;
      }

      @Override
      public Iterator<String> iterator() {
        return Collections.singleton(field).iterator();
      }
    };
  }

}
