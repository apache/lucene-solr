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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IOSupplier;

/**
 * Contains static functions that aid the implementation of {@link Matches} and
 * {@link MatchesIterator} interfaces.
 */
public final class MatchesUtils {

  private MatchesUtils() {}   // static functions only

  /**
   * Indicates a match with no term positions, for example on a Point or DocValues field,
   * or a field indexed as docs and freqs only
   */
  public static final Matches MATCH_WITH_NO_TERMS = new Matches() {
    @Override
    public MatchesIterator getMatches(String field) {
      return null;
    }

    @Override
    public Collection<Matches> getSubMatches() {
      return Collections.emptyList();
    }

    @Override
    public Iterator<String> iterator() {
      return Collections.emptyIterator();
    }
  };

  /**
   * Amalgamate a collection of {@link Matches} into a single object
   */
  public static Matches fromSubMatches(List<Matches> subMatches) {
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

      @Override
      public Collection<Matches> getSubMatches() {
        return subMatches;
      }
    };
  }

  /**
   * Create a Matches for a single field
   */
  public static Matches forField(String field, IOSupplier<MatchesIterator> mis) throws IOException {

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

      @Override
      public Collection<Matches> getSubMatches() {
        return Collections.emptyList();
      }
    };
  }

  /**
   * Create a MatchesIterator that iterates in order over all matches in a set of subiterators
   */
  public static MatchesIterator disjunction(List<MatchesIterator> subMatches) throws IOException {
    return DisjunctionMatchesIterator.fromSubIterators(subMatches);
  }

  /**
   * Create a MatchesIterator that is a disjunction over a list of terms extracted from a {@link BytesRefIterator}.
   *
   * Only terms that have at least one match in the given document will be included
   */
  public static MatchesIterator disjunction(LeafReaderContext context, int doc, Query query, String field, BytesRefIterator terms) throws IOException {
    return DisjunctionMatchesIterator.fromTermsEnum(context, doc, query, field, terms);
  }
}
