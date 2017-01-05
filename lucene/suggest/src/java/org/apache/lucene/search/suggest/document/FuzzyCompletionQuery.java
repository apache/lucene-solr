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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.suggest.BitsProducer;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.FiniteStringsIterator;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.UTF32ToUTF8;

/**
 * A {@link CompletionQuery} that match documents containing terms
 * within an edit distance of the specified prefix.
 * <p>
 * This query boost documents relative to how similar the indexed terms are to the
 * provided prefix.
 * <p>
 * Example usage of querying an analyzed prefix within an edit distance of 1 of 'subg'
 * against a field 'suggest_field' is as follows:
 *
 * <pre class="prettyprint">
 *  CompletionQuery query = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "subg"));
 * </pre>
 *
 * @lucene.experimental
 */
public class FuzzyCompletionQuery extends PrefixCompletionQuery {

  /**
   * Measure maxEdits, minFuzzyLength, transpositions and nonFuzzyPrefix
   * parameters in Unicode code points (actual letters)
   * instead of bytes.
   * */
  public static final boolean DEFAULT_UNICODE_AWARE = false;

  /**
   * The default minimum length of the key before any edits are allowed.
   */
  public static final int DEFAULT_MIN_FUZZY_LENGTH = 3;

  /**
   * The default prefix length where edits are not allowed.
   */
  public static final int DEFAULT_NON_FUZZY_PREFIX = 1;

  /**
   * The default maximum number of edits for fuzzy
   * suggestions.
   */
  public static final int DEFAULT_MAX_EDITS = 1;

  /**
   * The default transposition value passed to {@link LevenshteinAutomata}
   */
  public static final boolean DEFAULT_TRANSPOSITIONS = true;

  private final int maxEdits;
  private final boolean transpositions;
  private final int nonFuzzyPrefix;
  private final int minFuzzyLength;
  private final boolean unicodeAware;
  private final int maxDeterminizedStates;

  /**
   * Calls {@link FuzzyCompletionQuery#FuzzyCompletionQuery(Analyzer, Term, BitsProducer)}
   * with no filter
   */
  public FuzzyCompletionQuery(Analyzer analyzer, Term term) {
    this(analyzer, term, null);
  }

  /**
   * Calls {@link FuzzyCompletionQuery#FuzzyCompletionQuery(Analyzer, Term, BitsProducer,
   * int, boolean, int, int, boolean, int)}
   * with defaults for <code>maxEdits</code>, <code>transpositions</code>,
   * <code>nonFuzzyPrefix</code>, <code>minFuzzyLength</code>,
   * <code>unicodeAware</code> and <code>maxDeterminizedStates</code>
   *
   * See {@link #DEFAULT_MAX_EDITS}, {@link #DEFAULT_TRANSPOSITIONS},
   * {@link #DEFAULT_NON_FUZZY_PREFIX}, {@link #DEFAULT_MIN_FUZZY_LENGTH},
   * {@link #DEFAULT_UNICODE_AWARE} and {@link Operations#DEFAULT_MAX_DETERMINIZED_STATES}
   * for defaults
   */
  public FuzzyCompletionQuery(Analyzer analyzer, Term term, BitsProducer filter) {
    this(analyzer, term, filter, DEFAULT_MAX_EDITS, DEFAULT_TRANSPOSITIONS, DEFAULT_NON_FUZZY_PREFIX,
        DEFAULT_MIN_FUZZY_LENGTH, DEFAULT_UNICODE_AWARE, Operations.DEFAULT_MAX_DETERMINIZED_STATES
    );
  }

  /**
   * Constructs an analyzed fuzzy prefix completion query
   *
   * @param analyzer used to analyze the provided {@link Term#text()}
   * @param term query is run against {@link Term#field()} and {@link Term#text()}
   *             is analyzed with <code>analyzer</code>
   * @param filter used to query on a sub set of documents
   * @param maxEdits maximum number of acceptable edits
   * @param transpositions value passed to {@link LevenshteinAutomata}
   * @param nonFuzzyPrefix prefix length where edits are not allowed
   * @param minFuzzyLength minimum prefix length before any edits are allowed
   * @param unicodeAware treat prefix as unicode rather than bytes
   * @param maxDeterminizedStates maximum automaton states allowed for {@link LevenshteinAutomata}
   */
  public FuzzyCompletionQuery(Analyzer analyzer, Term term, BitsProducer filter, int maxEdits,
                              boolean transpositions, int nonFuzzyPrefix, int minFuzzyLength,
                              boolean unicodeAware, int maxDeterminizedStates) {
    super(analyzer, term, filter);
    this.maxEdits = maxEdits;
    this.transpositions = transpositions;
    this.nonFuzzyPrefix = nonFuzzyPrefix;
    this.minFuzzyLength = minFuzzyLength;
    this.unicodeAware = unicodeAware;
    this.maxDeterminizedStates = maxDeterminizedStates;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    CompletionTokenStream stream = (CompletionTokenStream) analyzer.tokenStream(getField(), getTerm().text());
    Set<IntsRef> refs = new HashSet<>();
    Automaton automaton = toLevenshteinAutomata(stream.toAutomaton(unicodeAware), refs);
    if (unicodeAware) {
      Automaton utf8automaton = new UTF32ToUTF8().convert(automaton);
      utf8automaton = Operations.determinize(utf8automaton, maxDeterminizedStates);
      automaton = utf8automaton;
    }
    // TODO Accumulating all refs is bad, because the resulting set may be very big.
    // TODO Better iterate over automaton again inside FuzzyCompletionWeight?
    return new FuzzyCompletionWeight(this, automaton, refs);
  }

  private Automaton toLevenshteinAutomata(Automaton automaton, Set<IntsRef> refs) {
    List<Automaton> subs = new ArrayList<>();
    FiniteStringsIterator finiteStrings = new FiniteStringsIterator(automaton);
    for (IntsRef string; (string = finiteStrings.next()) != null;) {
      refs.add(IntsRef.deepCopyOf(string));

      if (string.length <= nonFuzzyPrefix || string.length < minFuzzyLength) {
        subs.add(Automata.makeString(string.ints, string.offset, string.length));
      } else {
        int ints[] = new int[string.length - nonFuzzyPrefix];
        System.arraycopy(string.ints, string.offset + nonFuzzyPrefix, ints, 0, ints.length);
        // TODO: maybe add alphaMin to LevenshteinAutomata,
        // and pass 1 instead of 0?  We probably don't want
        // to allow the trailing dedup bytes to be
        // edited... but then 0 byte is "in general" allowed
        // on input (but not in UTF8).
        LevenshteinAutomata lev = new LevenshteinAutomata(ints,
            unicodeAware ? Character.MAX_CODE_POINT : 255,
            transpositions);
        subs.add(lev.toAutomaton(maxEdits,
            UnicodeUtil.newString(string.ints, string.offset, nonFuzzyPrefix)));
      }
    }

    if (subs.isEmpty()) {
      // automaton is empty, there is no accepted paths through it
      return Automata.makeEmpty(); // matches nothing
    } else if (subs.size() == 1) {
      // no synonyms or anything: just a single path through the tokenstream
      return subs.get(0);
    } else {
      // multiple paths: this is really scary! is it slow?
      // maybe we should not do this and throw UOE?
      Automaton a = Operations.union(subs);
      // TODO: we could call toLevenshteinAutomata() before det?
      // this only happens if you have multiple paths anyway (e.g. synonyms)
      return Operations.determinize(a, maxDeterminizedStates);
    }
  }

  /**
   * Get the maximum edit distance for fuzzy matches
   */
  public int getMaxEdits() {
    return maxEdits;
  }

  /**
   * Return whether transpositions count as a single edit
   */
  public boolean isTranspositions() {
    return transpositions;
  }

  /**
   * Get the length of a prefix where no edits are permitted
   */
  public int getNonFuzzyPrefix() {
    return nonFuzzyPrefix;
  }

  /**
   * Get the minimum length of a term considered for matching
   */
  public int getMinFuzzyLength() {
    return minFuzzyLength;
  }

  /**
   * Return true if lengths are measured in unicode code-points rather than bytes
   */
  public boolean isUnicodeAware() {
    return unicodeAware;
  }

  /**
   * Get the maximum number of determinized states permitted
   */
  public int getMaxDeterminizedStates() {
    return maxDeterminizedStates;
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    if (!getField().equals(field)) {
      buffer.append(getField());
      buffer.append(":");
    }
    buffer.append(getTerm().text());
    buffer.append('*');
    buffer.append('~');
    buffer.append(Integer.toString(maxEdits));
    if (getFilter() != null) {
      buffer.append(",");
      buffer.append("filter");
      buffer.append(getFilter().toString());
    }
    return buffer.toString();
  }

  private static class FuzzyCompletionWeight extends CompletionWeight {
    private final Set<IntsRef> refs;
    int currentBoost = 0;

    public FuzzyCompletionWeight(CompletionQuery query, Automaton automaton, Set<IntsRef> refs) throws IOException {
      super(query, automaton);
      this.refs = refs;
    }

    @Override
    protected void setNextMatch(IntsRef pathPrefix) {
      // NOTE: the last letter of the matched prefix for the exact
      // match never makes it through here
      // so an exact match and a match with only a edit at the
      // end is boosted the same
      int maxCount = 0;
      for (IntsRef ref : refs) {
        int minLength = Math.min(ref.length, pathPrefix.length);
        int count = 0;
        for (int i = 0; i < minLength; i++) {
          if (ref.ints[i + ref.offset] == pathPrefix.ints[i + pathPrefix.offset]) {
            count++;
          } else {
            break;
          }
        }
        maxCount = Math.max(maxCount, count);
      }
      currentBoost = maxCount;
    }

    @Override
    protected float boost() {
      return currentBoost;
    }
  }
}
