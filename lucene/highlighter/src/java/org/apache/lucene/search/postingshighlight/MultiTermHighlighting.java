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
package org.apache.lucene.search.postingshighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanPositionCheckQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.Operations;

/**
 * Support for highlighting multiterm queries in PostingsHighlighter.
 */
class MultiTermHighlighting {
  
  /** 
   * Extracts all MultiTermQueries for {@code field}, and returns equivalent 
   * automata that will match terms.
   */
  static CharacterRunAutomaton[] extractAutomata(Query query, String field) {
    List<CharacterRunAutomaton> list = new ArrayList<>();
    if (query instanceof BooleanQuery) {
      for (BooleanClause clause : (BooleanQuery) query) {
        if (!clause.isProhibited()) {
          list.addAll(Arrays.asList(extractAutomata(clause.getQuery(), field)));
        }
      }
    } else if (query instanceof ConstantScoreQuery) {
      list.addAll(Arrays.asList(extractAutomata(((ConstantScoreQuery) query).getQuery(), field)));
    } else if (query instanceof DisjunctionMaxQuery) {
      for (Query sub : ((DisjunctionMaxQuery) query).getDisjuncts()) {
        list.addAll(Arrays.asList(extractAutomata(sub, field)));
      }
    } else if (query instanceof SpanOrQuery) {
      for (Query sub : ((SpanOrQuery) query).getClauses()) {
        list.addAll(Arrays.asList(extractAutomata(sub, field)));
      }
    } else if (query instanceof SpanNearQuery) {
      for (Query sub : ((SpanNearQuery) query).getClauses()) {
        list.addAll(Arrays.asList(extractAutomata(sub, field)));
      }
    } else if (query instanceof SpanNotQuery) {
      list.addAll(Arrays.asList(extractAutomata(((SpanNotQuery) query).getInclude(), field)));
    } else if (query instanceof SpanPositionCheckQuery) {
      list.addAll(Arrays.asList(extractAutomata(((SpanPositionCheckQuery) query).getMatch(), field)));
    } else if (query instanceof SpanMultiTermQueryWrapper) {
      list.addAll(Arrays.asList(extractAutomata(((SpanMultiTermQueryWrapper<?>) query).getWrappedQuery(), field)));
    } else if (query instanceof PrefixQuery) {
      final PrefixQuery pq = (PrefixQuery) query;
      Term prefix = pq.getPrefix();
      if (prefix.field().equals(field)) {
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
      if (fq.getField().equals(field)) {
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
      if (tq.getField().equals(field)) {
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
        final Comparator<CharsRef> comparator = CharsRef.getUTF16SortedAsUTF8Comparator();
        
        // this is *not* an automaton, but it's very simple
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
      if (aq.getField().equals(field)) {
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
  
  /** 
   * Returns a "fake" DocsAndPositionsEnum over the tokenstream, returning offsets where {@code matchers}
   * matches tokens.
   * <p>
   * This is solely used internally by PostingsHighlighter: <b>DO NOT USE THIS METHOD!</b>
   */
  static PostingsEnum getDocsEnum(final TokenStream ts, final CharacterRunAutomaton[] matchers) throws IOException {
    final CharTermAttribute charTermAtt = ts.addAttribute(CharTermAttribute.class);
    final OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
    ts.reset();
    
    // TODO: we could use CachingWrapperFilter, (or consume twice) to allow us to have a true freq()
    // but this would have a performance cost for likely little gain in the user experience, it
    // would only serve to make this method less bogus.
    // instead, we always return freq() = Integer.MAX_VALUE and let PH terminate based on offset...
    
    return new PostingsEnum() {
      int currentDoc = -1;
      int currentMatch = -1;
      int currentStartOffset = -1;
      int currentEndOffset = -1;
      TokenStream stream = ts;
      
      final BytesRef matchDescriptions[] = new BytesRef[matchers.length];
      
      @Override
      public int nextPosition() throws IOException {
        if (stream != null) {
          while (stream.incrementToken()) {
            for (int i = 0; i < matchers.length; i++) {
              if (matchers[i].run(charTermAtt.buffer(), 0, charTermAtt.length())) {
                currentStartOffset = offsetAtt.startOffset();
                currentEndOffset = offsetAtt.endOffset();
                currentMatch = i;
                return 0;
              }
            }
          }
          stream.end();
          stream.close();
          stream = null;
        }
        // exhausted
        currentStartOffset = currentEndOffset = Integer.MAX_VALUE;
        return Integer.MAX_VALUE;
      }

      @Override
      public int freq() throws IOException {
        return Integer.MAX_VALUE; // lie
      }

      @Override
      public int startOffset() throws IOException {
        assert currentStartOffset >= 0;
        return currentStartOffset;
      }

      @Override
      public int endOffset() throws IOException {
        assert currentEndOffset >= 0;
        return currentEndOffset;
      }

      @Override
      public BytesRef getPayload() throws IOException {
        if (matchDescriptions[currentMatch] == null) {
          matchDescriptions[currentMatch] = new BytesRef(matchers[currentMatch].toString());
        }
        return matchDescriptions[currentMatch];
      }

      @Override
      public int docID() {
        return currentDoc;
      }

      @Override
      public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int advance(int target) throws IOException {
        return currentDoc = target;
      }

      @Override
      public long cost() {
        return 0;
      }
    };
  }
}
