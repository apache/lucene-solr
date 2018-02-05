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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;


/**
 * Uses an {@link Analyzer} on content to get offsets and then populates a {@link MemoryIndex}.
 *
 * @lucene.internal
 */
public class MemoryIndexOffsetStrategy extends AnalysisOffsetStrategy {

  private final MemoryIndex memoryIndex;
  private final LeafReader leafReader;
  private final CharacterRunAutomaton preMemIndexFilterAutomaton;

  public MemoryIndexOffsetStrategy(String field, Predicate<String> fieldMatcher, BytesRef[] extractedTerms, PhraseHelper phraseHelper,
                                   CharacterRunAutomaton[] automata, Analyzer analyzer,
                                   Function<Query, Collection<Query>> multiTermQueryRewrite) {
    super(field, extractedTerms, phraseHelper, automata, analyzer);
    boolean storePayloads = phraseHelper.hasPositionSensitivity(); // might be needed
    memoryIndex = new MemoryIndex(true, storePayloads);//true==store offsets
    leafReader = (LeafReader) memoryIndex.createSearcher().getIndexReader(); // appears to be re-usable
    // preFilter for MemoryIndex
    preMemIndexFilterAutomaton = buildCombinedAutomaton(fieldMatcher, terms, this.automata, phraseHelper, multiTermQueryRewrite);
  }

  /**
   * Build one {@link CharacterRunAutomaton} matching any term the query might match.
   */
  private static CharacterRunAutomaton buildCombinedAutomaton(Predicate<String> fieldMatcher,
                                                              BytesRef[] terms,
                                                              CharacterRunAutomaton[] automata,
                                                              PhraseHelper strictPhrases,
                                                              Function<Query, Collection<Query>> multiTermQueryRewrite) {
    List<CharacterRunAutomaton> allAutomata = new ArrayList<>();
    if (terms.length > 0) {
      allAutomata.add(new CharacterRunAutomaton(Automata.makeStringUnion(Arrays.asList(terms))));
    }
    Collections.addAll(allAutomata, automata);
    for (SpanQuery spanQuery : strictPhrases.getSpanQueries()) {
      Collections.addAll(allAutomata,
          MultiTermHighlighting.extractAutomata(spanQuery, fieldMatcher, true, multiTermQueryRewrite));//true==lookInSpan
    }

    if (allAutomata.size() == 1) {
      return allAutomata.get(0);
    }
    //TODO it'd be nice if we could get at the underlying Automaton in CharacterRunAutomaton so that we
    //  could union them all. But it's not exposed, and sometimes the automaton is byte (not char) oriented

    // Return an aggregate CharacterRunAutomaton of others
    return new CharacterRunAutomaton(Automata.makeEmpty()) {// the makeEmpty() is bogus; won't be used
      @Override
      public boolean run(char[] chars, int offset, int length) {
        for (int i = 0; i < allAutomata.size(); i++) {// don't use foreach to avoid Iterator allocation
          if (allAutomata.get(i).run(chars, offset, length)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  @Override
  public OffsetsEnum getOffsetsEnum(IndexReader reader, int docId, String content) throws IOException {
    // note: don't need LimitTokenOffsetFilter since content is already truncated to maxLength
    TokenStream tokenStream = tokenStream(content);

    // Filter the tokenStream to applicable terms
    tokenStream = newKeepWordFilter(tokenStream, preMemIndexFilterAutomaton);
    memoryIndex.reset();
    memoryIndex.addField(field, tokenStream);//note: calls tokenStream.reset() & close()
    docId = 0;

    return createOffsetsEnumFromReader(leafReader, docId);
  }


  private static FilteringTokenFilter newKeepWordFilter(final TokenStream tokenStream,
                                                        final CharacterRunAutomaton charRunAutomaton) {
    // it'd be nice to use KeepWordFilter but it demands a CharArraySet. TODO File JIRA? Need a new interface?
    return new FilteringTokenFilter(tokenStream) {
      final CharTermAttribute charAtt = addAttribute(CharTermAttribute.class);

      @Override
      protected boolean accept() throws IOException {
        return charRunAutomaton.run(charAtt.buffer(), 0, charAtt.length());
      }
    };
  }

}
