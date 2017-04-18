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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanPositionCheckQuery;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.Operations;

/**
 * Support for highlighting multi-term queries.
 *
 * @lucene.internal
 */
class MultiTermHighlighting {
  private MultiTermHighlighting() {
  }

  /**
   * Extracts MultiTermQueries that match the provided field predicate.
   * Returns equivalent automata that will match terms.
   */
  public static CharacterRunAutomaton[] extractAutomata(Query query,
                                                        Predicate<String> fieldMatcher,
                                                        boolean lookInSpan,
                                                        Function<Query, Collection<Query>> preRewriteFunc) {
    // TODO Lucene needs a Query visitor API!  LUCENE-3041

    List<CharacterRunAutomaton> list = new ArrayList<>();
    Collection<Query> customSubQueries = preRewriteFunc.apply(query);
    if (customSubQueries != null) {
      for (Query sub : customSubQueries) {
        list.addAll(Arrays.asList(extractAutomata(sub, fieldMatcher, lookInSpan, preRewriteFunc)));
      }
    } else if (query instanceof BooleanQuery) {
      for (BooleanClause clause : (BooleanQuery) query) {
        if (!clause.isProhibited()) {
          list.addAll(Arrays.asList(extractAutomata(clause.getQuery(), fieldMatcher, lookInSpan, preRewriteFunc)));
        }
      }
    } else if (query instanceof ConstantScoreQuery) {
      list.addAll(Arrays.asList(extractAutomata(((ConstantScoreQuery) query).getQuery(), fieldMatcher, lookInSpan,
          preRewriteFunc)));
    } else if (query instanceof BoostQuery) {
      list.addAll(Arrays.asList(extractAutomata(((BoostQuery)query).getQuery(), fieldMatcher, lookInSpan,
          preRewriteFunc)));
    } else if (query instanceof DisjunctionMaxQuery) {
      for (Query sub : ((DisjunctionMaxQuery) query).getDisjuncts()) {
        list.addAll(Arrays.asList(extractAutomata(sub, fieldMatcher, lookInSpan, preRewriteFunc)));
      }
    } else if (lookInSpan && query instanceof SpanOrQuery) {
      for (Query sub : ((SpanOrQuery) query).getClauses()) {
        list.addAll(Arrays.asList(extractAutomata(sub, fieldMatcher, lookInSpan, preRewriteFunc)));
      }
    } else if (lookInSpan && query instanceof SpanNearQuery) {
      for (Query sub : ((SpanNearQuery) query).getClauses()) {
        list.addAll(Arrays.asList(extractAutomata(sub, fieldMatcher, lookInSpan, preRewriteFunc)));
      }
    } else if (lookInSpan && query instanceof SpanNotQuery) {
      list.addAll(Arrays.asList(extractAutomata(((SpanNotQuery) query).getInclude(), fieldMatcher, lookInSpan,
          preRewriteFunc)));
    } else if (lookInSpan && query instanceof SpanPositionCheckQuery) {
      list.addAll(Arrays.asList(extractAutomata(((SpanPositionCheckQuery) query).getMatch(), fieldMatcher, lookInSpan,
          preRewriteFunc)));
    } else if (lookInSpan && query instanceof SpanBoostQuery) {
      list.addAll(Arrays.asList(extractAutomata(((SpanBoostQuery) query).getQuery(), fieldMatcher, lookInSpan,
          preRewriteFunc)));
    } else if (lookInSpan && query instanceof SpanMultiTermQueryWrapper) {
      list.addAll(Arrays.asList(extractAutomata(((SpanMultiTermQueryWrapper<?>) query).getWrappedQuery(),
          fieldMatcher, lookInSpan, preRewriteFunc)));
    } else if (query instanceof PrefixQuery) {
      final PrefixQuery pq = (PrefixQuery) query;
      Term prefix = pq.getPrefix();
      if (fieldMatcher.test(prefix.field())) {
        list.add(new CharacterRunAutomaton(Operations.concatenate(Automata.makeString(prefix.text()),
            Automata.makeAnyString())) {
          @Override
          public String toString() {
            return pq.toString();
          }
        });
      }
    } else if (query instanceof FuzzyQuery) {
      final FuzzyQuery fq = (FuzzyQuery) query;
      if (fieldMatcher.test(fq.getField())) {
        String utf16 = fq.getTerm().text();
        int termText[] = new int[utf16.codePointCount(0, utf16.length())];
        for (int cp, i = 0, j = 0; i < utf16.length(); i += Character.charCount(cp)) {
          termText[j++] = cp = utf16.codePointAt(i);
        }
        int termLength = termText.length;
        int prefixLength = Math.min(fq.getPrefixLength(), termLength);
        String suffix = UnicodeUtil.newString(termText, prefixLength, termText.length - prefixLength);
        LevenshteinAutomata builder = new LevenshteinAutomata(suffix, fq.getTranspositions());
        String prefix = UnicodeUtil.newString(termText, 0, prefixLength);
        Automaton automaton = builder.toAutomaton(fq.getMaxEdits(), prefix);
        list.add(new CharacterRunAutomaton(automaton) {
          @Override
          public String toString() {
            return fq.toString();
          }
        });
      }
    } else if (query instanceof TermRangeQuery) {
      final TermRangeQuery tq = (TermRangeQuery) query;
      if (fieldMatcher.test(tq.getField())) {
        final CharsRef lowerBound;
        if (tq.getLowerTerm() == null) {
          lowerBound = null;
        } else {
          lowerBound = new CharsRef(tq.getLowerTerm().utf8ToString());
        }

        final CharsRef upperBound;
        if (tq.getUpperTerm() == null) {
          upperBound = null;
        } else {
          upperBound = new CharsRef(tq.getUpperTerm().utf8ToString());
        }

        final boolean includeLower = tq.includesLower();
        final boolean includeUpper = tq.includesUpper();
        final CharsRef scratch = new CharsRef();

        @SuppressWarnings("deprecation")
        final Comparator<CharsRef> comparator = CharsRef.getUTF16SortedAsUTF8Comparator();

        // this is *not* an automaton, but its very simple
        list.add(new CharacterRunAutomaton(Automata.makeEmpty()) {
          @Override
          public boolean run(char[] s, int offset, int length) {
            scratch.chars = s;
            scratch.offset = offset;
            scratch.length = length;

            if (lowerBound != null) {
              int cmp = comparator.compare(scratch, lowerBound);
              if (cmp < 0 || (!includeLower && cmp == 0)) {
                return false;
              }
            }

            if (upperBound != null) {
              int cmp = comparator.compare(scratch, upperBound);
              if (cmp > 0 || (!includeUpper && cmp == 0)) {
                return false;
              }
            }
            return true;
          }

          @Override
          public String toString() {
            return tq.toString();
          }
        });
      }
    } else if (query instanceof AutomatonQuery) {
      final AutomatonQuery aq = (AutomatonQuery) query;
      if (fieldMatcher.test(aq.getField())) {
        list.add(new CharacterRunAutomaton(aq.getAutomaton()) {
          @Override
          public String toString() {
            return aq.toString();
          }
        });
      }
    }
    return list.toArray(new CharacterRunAutomaton[list.size()]);
  }

}
