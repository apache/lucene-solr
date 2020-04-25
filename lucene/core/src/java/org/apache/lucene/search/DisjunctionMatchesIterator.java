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
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.PriorityQueue;

/**
 * A {@link MatchesIterator} that combines matches from a set of sub-iterators
 *
 * Matches are sorted by their start positions, and then by their end positions, so that
 * prefixes sort first.  Matches may overlap, or be duplicated if they appear in more
 * than one of the sub-iterators.
 */
final class DisjunctionMatchesIterator implements MatchesIterator {

  /**
   * Create a {@link DisjunctionMatchesIterator} over a list of terms
   *
   * Only terms that have at least one match in the given document will be included
   */
  static MatchesIterator fromTerms(LeafReaderContext context, int doc, Query query, String field, List<Term> terms) throws IOException {
    Objects.requireNonNull(field);
    for (Term term : terms) {
      if (Objects.equals(field, term.field()) == false) {
        throw new IllegalArgumentException("Tried to generate iterator from terms in multiple fields: expected [" + field + "] but got [" + term.field() + "]");
      }
    }
    return fromTermsEnum(context, doc, query, field, asBytesRefIterator(terms));
  }

  private static BytesRefIterator asBytesRefIterator(List<Term> terms) {
    return new BytesRefIterator() {
      int i = 0;
      @Override
      public BytesRef next() {
        if (i >= terms.size())
          return null;
        return terms.get(i++).bytes();
      }
    };
  }

  /**
   * Create a {@link DisjunctionMatchesIterator} over a list of terms extracted from a {@link BytesRefIterator}
   *
   * Only terms that have at least one match in the given document will be included
   */
  static MatchesIterator fromTermsEnum(LeafReaderContext context, int doc, Query query, String field, BytesRefIterator terms) throws IOException {
    Objects.requireNonNull(field);
    Terms t = context.reader().terms(field);
    if (t == null)
      return null;
    TermsEnum te = t.iterator();
    PostingsEnum reuse = null;
    for (BytesRef term = terms.next(); term != null; term = terms.next()) {
      if (te.seekExact(term)) {
        PostingsEnum pe = te.postings(reuse, PostingsEnum.OFFSETS);
        if (pe.advance(doc) == doc) {
          return new TermsEnumDisjunctionMatchesIterator(new TermMatchesIterator(query, pe), terms, te, doc, query);
        }
        else {
          reuse = pe;
        }
      }
    }
    return null;
  }

  // MatchesIterator over a set of terms that only loads the first matching term at construction,
  // waiting until the iterator is actually used before it loads all other matching terms.
  private static class TermsEnumDisjunctionMatchesIterator implements MatchesIterator {

    private final MatchesIterator first;
    private final BytesRefIterator terms;
    private final TermsEnum te;
    private final int doc;
    private final Query query;

    private MatchesIterator it = null;

    TermsEnumDisjunctionMatchesIterator(MatchesIterator first, BytesRefIterator terms, TermsEnum te, int doc, Query query) {
      this.first = first;
      this.terms = terms;
      this.te = te;
      this.doc = doc;
      this.query = query;
    }

    private void init() throws IOException {
      List<MatchesIterator> mis = new ArrayList<>();
      mis.add(first);
      PostingsEnum reuse = null;
      for (BytesRef term = terms.next(); term != null; term = terms.next()) {
        if (te.seekExact(term)) {
          PostingsEnum pe = te.postings(reuse, PostingsEnum.OFFSETS);
          if (pe.advance(doc) == doc) {
            mis.add(new TermMatchesIterator(query, pe));
            reuse = null;
          } else {
            reuse = pe;
          }
        }
      }
      it = fromSubIterators(mis);
    }

    @Override
    public boolean next() throws IOException {
      if (it == null) {
        init();
      }
      assert it != null;
      return it.next();
    }

    @Override
    public int startPosition() {
      return it.startPosition();
    }

    @Override
    public int endPosition() {
      return it.endPosition();
    }

    @Override
    public int startOffset() throws IOException {
      return it.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return it.endOffset();
    }

    @Override
    public MatchesIterator getSubMatches() throws IOException {
      return it.getSubMatches();
    }

    @Override
    public Query getQuery() {
      return it.getQuery();
    }
  }

  static MatchesIterator fromSubIterators(List<MatchesIterator> mis) throws IOException {
    if (mis.size() == 0)
      return null;
    if (mis.size() == 1)
      return mis.get(0);
    return new DisjunctionMatchesIterator(mis);
  }

  private final PriorityQueue<MatchesIterator> queue;

  private boolean started = false;

  private DisjunctionMatchesIterator(List<MatchesIterator> matches) throws IOException {
    queue = new PriorityQueue<MatchesIterator>(matches.size()){
      @Override
      protected boolean lessThan(MatchesIterator a, MatchesIterator b) {
        return a.startPosition() < b.startPosition() ||
            (a.startPosition() == b.startPosition() && a.endPosition() < b.endPosition()) ||
            (a.startPosition() == b.startPosition() && a.endPosition() == b.endPosition());
      }
    };
    for (MatchesIterator mi : matches) {
      if (mi.next()) {
        queue.add(mi);
      }
    }
  }

  @Override
  public boolean next() throws IOException {
    if (started == false) {
      return started = true;
    }
    if (queue.top().next() == false) {
      queue.pop();
    }
    if (queue.size() > 0) {
      queue.updateTop();
      return true;
    }
    return false;
  }

  @Override
  public int startPosition() {
    return queue.top().startPosition();
  }

  @Override
  public int endPosition() {
    return queue.top().endPosition();
  }

  @Override
  public int startOffset() throws IOException {
    return queue.top().startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    return queue.top().endOffset();
  }

  @Override
  public MatchesIterator getSubMatches() throws IOException {
    return queue.top().getSubMatches();
  }

  @Override
  public Query getQuery() {
    return queue.top().getQuery();
  }
}
