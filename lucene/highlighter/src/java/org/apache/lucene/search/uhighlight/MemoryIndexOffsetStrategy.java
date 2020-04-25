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
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.spans.SpanQuery;


/**
 * Uses an {@link Analyzer} on content to get offsets and then populates a {@link MemoryIndex}.
 *
 * @lucene.internal
 */
public class MemoryIndexOffsetStrategy extends AnalysisOffsetStrategy {

  private final MemoryIndex memoryIndex;
  private final LeafReader memIndexLeafReader;
  private final CharArrayMatcher preMemIndexFilterAutomaton;

  public MemoryIndexOffsetStrategy(UHComponents components, Analyzer analyzer) {
    super(components, analyzer);
    boolean storePayloads = components.getPhraseHelper().hasPositionSensitivity(); // might be needed
    memoryIndex = new MemoryIndex(true, storePayloads);//true==store offsets
    memIndexLeafReader = (LeafReader) memoryIndex.createSearcher().getIndexReader(); // appears to be re-usable
    // preFilter for MemoryIndex
    preMemIndexFilterAutomaton = buildCombinedAutomaton(components);
  }

  /**
   * Build one {@link CharArrayMatcher} matching any term the query might match.
   */
  private static CharArrayMatcher buildCombinedAutomaton(UHComponents components) {
    // We don't know enough about the query to do this confidently
    if (components.getTerms() == null || components.getAutomata() == null) {
      return null;
    }

    List<CharArrayMatcher> allAutomata = new ArrayList<>();
    if (components.getTerms().length > 0) {
      allAutomata.add(CharArrayMatcher.fromTerms(Arrays.asList(components.getTerms())));
    }
    Collections.addAll(allAutomata, components.getAutomata());
    for (SpanQuery spanQuery : components.getPhraseHelper().getSpanQueries()) {
      Collections.addAll(allAutomata,
          MultiTermHighlighting.extractAutomata(spanQuery, components.getFieldMatcher(), true));//true==lookInSpan
    }

    if (allAutomata.size() == 1) {
      return allAutomata.get(0);
    }

    //TODO it'd be nice if we could get at the underlying Automaton in CharacterRunAutomaton so that we
    //  could union them all. But it's not exposed, and sometimes the automaton is byte (not char) oriented

    // Return an aggregate CharArrayMatcher of others
    return (chars, offset, length) -> {
      for (int i = 0; i < allAutomata.size(); i++) {// don't use foreach to avoid Iterator allocation
        if (allAutomata.get(i).match(chars, offset, length)) {
          return true;
        }
      }
      return false;
    };
  }

  @Override
  public OffsetsEnum getOffsetsEnum(LeafReader reader, int docId, String content) throws IOException {
    // note: don't need LimitTokenOffsetFilter since content is already truncated to maxLength
    TokenStream tokenStream = tokenStream(content);

    // Filter the tokenStream to applicable terms
    if (preMemIndexFilterAutomaton != null) {
      tokenStream = newKeepWordFilter(tokenStream, preMemIndexFilterAutomaton);
    }
    memoryIndex.reset();
    memoryIndex.addField(getField(), tokenStream);//note: calls tokenStream.reset() & close()

    if (reader == null) {
      return createOffsetsEnumFromReader(memIndexLeafReader, 0);
    } else {
      return createOffsetsEnumFromReader(
          new OverlaySingleDocTermsLeafReader(
              reader,
              memIndexLeafReader,
              getField(),
              docId),
          docId);
    }
  }

  private static FilteringTokenFilter newKeepWordFilter(final TokenStream tokenStream,
                                                        final CharArrayMatcher matcher) {
    // it'd be nice to use KeepWordFilter but it demands a CharArraySet. TODO File JIRA? Need a new interface?
    return new FilteringTokenFilter(tokenStream) {
      final CharTermAttribute charAtt = addAttribute(CharTermAttribute.class);

      @Override
      protected boolean accept() throws IOException {
        return matcher.match(charAtt.buffer(), 0, charAtt.length());
      }
    };
  }

}
