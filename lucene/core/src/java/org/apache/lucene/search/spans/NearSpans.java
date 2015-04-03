package org.apache.lucene.search.spans;

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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.TwoPhaseIterator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Common super class for un/ordered Spans
 */
abstract class NearSpans extends Spans {
  final SpanNearQuery query;
  final int allowedSlop;

  final Spans[] subSpans; // in query order
  final DocIdSetIterator conjunction; // use to move to next doc with all clauses
  boolean atFirstInCurrentDoc;
  boolean oneExhaustedInCurrentDoc; // no more results possbile in current doc

  NearSpans(SpanNearQuery query, List<Spans> subSpans)
  throws IOException {
    this.query = Objects.requireNonNull(query);
    this.allowedSlop = query.getSlop();
    if (subSpans.size() < 2) {
      throw new IllegalArgumentException("Less than 2 subSpans: " + query);
    }
    this.subSpans = subSpans.toArray(new Spans[subSpans.size()]); // in query order
    this.conjunction = ConjunctionDISI.intersect(subSpans);
  }

  @Override
  public int docID() {
    return conjunction.docID();
  }

  @Override
  public long cost() {
    return conjunction.cost();
  }

  @Override
  public int nextDoc() throws IOException {
    return (conjunction.nextDoc() == NO_MORE_DOCS)
            ? NO_MORE_DOCS
            : toMatchDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return (conjunction.advance(target) == NO_MORE_DOCS)
            ? NO_MORE_DOCS
            : toMatchDoc();
  }

  abstract int toMatchDoc() throws IOException;

  abstract boolean twoPhaseCurrentDocMatches() throws IOException;

  /**
   * Return a {@link TwoPhaseIterator} view of this {@link NearSpans}.
   */
  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    TwoPhaseIterator res = new TwoPhaseIterator(conjunction) {

      @Override
      public boolean matches() throws IOException {
        return twoPhaseCurrentDocMatches();
      }
    };
    return res;
  }

  public Spans[] getSubSpans() {
    return subSpans;
  }

}
