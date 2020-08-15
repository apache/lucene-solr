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
package org.apache.lucene.search.uhighlight;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

/**
 * Support for highlighting multi-term queries.
 *
 * @lucene.internal
 */
final class MultiTermHighlighting {
  private MultiTermHighlighting() {
  }

  /**
   * Extracts MultiTermQueries that match the provided field predicate.
   * Returns equivalent automata that will match terms.
   */
  static LabelledCharArrayMatcher[] extractAutomata(Query query, Predicate<String> fieldMatcher, boolean lookInSpan) {
    AutomataCollector collector = new AutomataCollector(lookInSpan, fieldMatcher);
    query.visit(collector);
    return collector.runAutomata.toArray(new LabelledCharArrayMatcher[0]);
  }

  /**
   * Indicates if the the leaf query (from {@link QueryVisitor#visitLeaf(Query)}) is a type of query that
   * we can extract automata from.
   */
  public static boolean canExtractAutomataFromLeafQuery(Query query) {
    return query instanceof AutomatonQuery || query instanceof FuzzyQuery;
  }

  private static class AutomataCollector extends QueryVisitor {

    List<LabelledCharArrayMatcher> runAutomata = new ArrayList<>();
    final boolean lookInSpan;
    final Predicate<String> fieldMatcher;

    private AutomataCollector(boolean lookInSpan, Predicate<String> fieldMatcher) {
      this.lookInSpan = lookInSpan;
      this.fieldMatcher = fieldMatcher;
    }

    @Override
    public boolean acceptField(String field) {
      return fieldMatcher.test(field);
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (lookInSpan == false && parent instanceof SpanQuery) {
        return QueryVisitor.EMPTY_VISITOR;
      }
      return super.getSubVisitor(occur, parent);
    }

    @Override
    public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
      runAutomata.add(LabelledCharArrayMatcher.wrap(query.toString(), automaton.get()));
    }

  }

}
