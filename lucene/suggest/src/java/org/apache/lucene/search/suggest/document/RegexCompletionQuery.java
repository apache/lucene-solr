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
package org.apache.lucene.search.suggest.document;

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.BitsProducer;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

/**
 * A {@link CompletionQuery} which takes a regular expression
 * as the prefix of the query term.
 *
 * <p>
 * Example usage of querying a prefix of 'sug' and 'sub'
 * as a regular expression against a suggest field 'suggest_field':
 *
 * <pre class="prettyprint">
 *  CompletionQuery query = new RegexCompletionQuery(new Term("suggest_field", "su[g|b]"));
 * </pre>
 *
 * <p>
 * See {@link RegExp} for the supported regular expression
 * syntax
 *
 * @lucene.experimental
 */
public class RegexCompletionQuery extends CompletionQuery {

  private final int flags;
  private final int determinizeWorkLimit;

  /**
   * Calls {@link RegexCompletionQuery#RegexCompletionQuery(Term, BitsProducer)}
   * with no filter
   */
  public RegexCompletionQuery(Term term) {
    this(term, null);
  }

  /**
   * Calls {@link RegexCompletionQuery#RegexCompletionQuery(Term, int, int, BitsProducer)}
   * enabling all optional regex syntax and <code>maxDeterminizedStates</code> of
   * {@value Operations#DEFAULT_DETERMINIZE_WORK_LIMIT}
   */
  public RegexCompletionQuery(Term term, BitsProducer filter) {
    this(term, RegExp.ALL, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, filter);
  }
  /**
   * Calls {@link RegexCompletionQuery#RegexCompletionQuery(Term, int, int, BitsProducer)} enabling
   * all optional regex syntax and <code>determinizeWorkLimit</code> of {@value
   * Operations#DEFAULT_DETERMINIZE_WORK_LIMIT}
   */
  public RegexCompletionQuery(Term term, int flags, int determinizeWorkLimit) {
    this(term, flags, determinizeWorkLimit, null);
  }

  /**
   * Constructs a regular expression completion query
   *
   * @param term query is run against {@link Term#field()} and {@link Term#text()}
   *             is interpreted as a regular expression
   * @param flags used as syntax_flag in {@link RegExp#RegExp(String, int)}
   * @param determinizeWorkLimit used in {@link RegExp#toAutomaton(int)}
   * @param filter used to query on a sub set of documents
   */
  public RegexCompletionQuery(Term term, int flags, int determinizeWorkLimit, BitsProducer filter) {
    super(term, filter);
    this.flags = flags;
    this.determinizeWorkLimit = determinizeWorkLimit;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    // If an empty regex is provided, we return an automaton that matches nothing. This ensures
    // consistency with PrefixCompletionQuery, which returns no results for an empty term.
    Automaton automaton = getTerm().text().isEmpty()
        ? Automata.makeEmpty()
        : new RegExp(getTerm().text(), flags).toAutomaton(determinizeWorkLimit);
    return new CompletionWeight(this, automaton);
  }

  /**
   * Get the regex flags
   */
  public int getFlags() {
    return flags;
  }

  /** Get the maximum effort permitted to determinize the automaton */
  public int getDeterminizeWorkLimit() {
    return determinizeWorkLimit;
  }

  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
