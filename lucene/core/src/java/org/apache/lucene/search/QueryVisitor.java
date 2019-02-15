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

import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * Interface to allow recursion through a query tree
 *
 * @see Query#visit(QueryVisitor)
 */
public interface QueryVisitor {

  /**
   * Called by leaf queries that match on a specific term
   * @param query the leaf query visited
   * @param term  the term the query will match on
   */
  void visitLeaf(Query query, Term term);

  /**
   * Called by leaf queries that do not match against the terms index
   * @param query the leaf query visited
   */
  default void visitLeaf(Query query) {}

  /**
   * Called by leaf queries that match against a set of terms defined by a predicate
   * @param query               the leaf query
   * @param field               the field the query matches against
   * @param predicateSupplier   a supplier for a predicate that will select matching terms
   */
  default void visitLeaf(Query query, String field, Supplier<Predicate<BytesRef>> predicateSupplier) {}

  /**
   * Pulls a visitor instance for visiting matching child clauses of a query
   *
   * The default implementation returns {@code this}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getMatchingVisitor(Query parent) {
    return this;
  }

  /**
   * Pulls a visitor instance for visiting matching 'should-match' child clauses of a query
   *
   * The default implementation returns {@code this}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getShouldMatchVisitor(Query parent) {
    return this;
  }

  /**
   * Pulls a visitor instance for visiting matching non-scoring child clauses of a query
   *
   * The default implementation returns {@code this}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getFilteringVisitor(Query parent) {
    return this;
  }

  /**
   * Pulls a visitor instance for visiting matching 'must-not' child clauses of a query
   *
   * The default implementation returns {@link #NO_OP}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getNonMatchingVisitor(Query parent) {
    return NO_OP;
  }

  /**
   * Builds a predicate for matching a set of bytes from a {@link CompiledAutomaton}
   */
  static Predicate<BytesRef> matchesAutomaton(CompiledAutomaton automaton) {
    return term -> {
      switch (automaton.type) {
        case NONE:
          return false;
        case ALL:
          return true;
        case SINGLE:
          assert automaton.term != null;
          return automaton.term.equals(term);
      }
      assert automaton.runAutomaton != null;
      return automaton.runAutomaton.run(term.bytes, term.offset, term.length);
    };
  }

  /**
   * A QueryVisitor implementation that collects no terms
   */
  QueryVisitor NO_OP = (query, term) -> { };

}
