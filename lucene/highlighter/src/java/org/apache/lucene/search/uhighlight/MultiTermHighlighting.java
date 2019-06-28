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

import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;

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
  static CharacterRunAutomaton[] extractAutomata(Query query, Predicate<String> fieldMatcher, boolean lookInSpan) {

    AutomataCollector collector = new AutomataCollector(lookInSpan, fieldMatcher);
    query.visit(collector);
    return collector.runAutomata.toArray(new CharacterRunAutomaton[0]);
  }

  /**
   * Indicates if the the leaf query (from {@link QueryVisitor#visitLeaf(Query)}) is a type of query that
   * we can extract automata from.
   */
  public static boolean canExtractAutomataFromLeafQuery(Query query) {
    return query instanceof AutomatonQuery || query instanceof FuzzyQuery;
  }

  private static class AutomataCollector extends QueryVisitor {

    List<CharacterRunAutomaton> runAutomata = new ArrayList<>();
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
    public void visitLeaf(Query query) {
      if (query instanceof AutomatonQuery) {
        AutomatonQuery aq = (AutomatonQuery) query;
        if (aq.isAutomatonBinary() == false) {
          // WildcardQuery, RegexpQuery
          runAutomata.add(new CharacterRunAutomaton(aq.getAutomaton()) {
            @Override
            public String toString() {
              return query.toString();
            }
          });
        }
        else {
          runAutomata.add(binaryToCharRunAutomaton(aq.getAutomaton(), query.toString()));
        }
      }
      else if (query instanceof FuzzyQuery) {
        FuzzyQuery fq = (FuzzyQuery) query;
        if (fq.getMaxEdits() == 0 || fq.getPrefixLength() >= fq.getTerm().text().length()) {
          consumeTerms(query, fq.getTerm());
        }
        else {
          runAutomata.add(new CharacterRunAutomaton(fq.toAutomaton()){
            @Override
            public String toString() {
              return query.toString();
            }
          });
        }
      }
    }

  }

  private static CharacterRunAutomaton binaryToCharRunAutomaton(Automaton binaryAutomaton, String description) {
    return new CharacterRunAutomaton(Automata.makeEmpty()) { // empty here is bogus just to satisfy API
      //   TODO can we get access to the aq.compiledAutomaton.runAutomaton ?
      ByteRunAutomaton byteRunAutomaton =
          new ByteRunAutomaton(binaryAutomaton, true, Operations.DEFAULT_MAX_DETERMINIZED_STATES);

      @Override
      public String toString() {
        return description;
      }

      @Override
      public boolean run(char[] chars, int offset, int length) {
        int state = 0;
        final int maxIdx = offset + length;
        for (int i = offset; i < maxIdx; i++) {
          final int code = chars[i];
          int b;
          // UTF16 to UTF8   (inlined logic from UnicodeUtil.UTF16toUTF8 )
          if (code < 0x80) {
            state = byteRunAutomaton.step(state, code);
            if (state == -1) return false;
          } else if (code < 0x800) {
            b = (0xC0 | (code >> 6));
            state = byteRunAutomaton.step(state, b);
            if (state == -1) return false;
            b = (0x80 | (code & 0x3F));
            state = byteRunAutomaton.step(state, b);
            if (state == -1) return false;
          } else {
            // more complex
            byte[] utf8Bytes = new byte[4 * (maxIdx - i)];
            int utf8Len = UnicodeUtil.UTF16toUTF8(chars, i, maxIdx - i, utf8Bytes);
            for (int utfIdx = 0; utfIdx < utf8Len; utfIdx++) {
              state = byteRunAutomaton.step(state, utf8Bytes[utfIdx] & 0xFF);
              if (state == -1) return false;
            }
            break;
          }
        }
        return byteRunAutomaton.isAccept(state);
      }
    };
  }



}
