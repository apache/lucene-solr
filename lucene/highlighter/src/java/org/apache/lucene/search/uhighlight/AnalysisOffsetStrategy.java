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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;


/**
 * Uses an {@link Analyzer} on content to get offsets. It may use a {@link MemoryIndex} too.
 *
 * @lucene.internal
 */
public class AnalysisOffsetStrategy extends FieldOffsetStrategy {

  //TODO: Consider splitting this highlighter into a MemoryIndexFieldHighlighter and a TokenStreamFieldHighlighter
  private static final BytesRef[] ZERO_LEN_BYTES_REF_ARRAY = new BytesRef[0];
  private final Analyzer analyzer;
  private final MemoryIndex memoryIndex;
  private final LeafReader leafReader;
  private final CharacterRunAutomaton preMemIndexFilterAutomaton;

  public AnalysisOffsetStrategy(String field, BytesRef[] extractedTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Analyzer analyzer) {
    super(field, extractedTerms, phraseHelper, automata);
    this.analyzer = analyzer;
    // Automata (Wildcards / MultiTermQuery):
    this.automata = automata;

    if (terms.length > 0 && !strictPhrases.hasPositionSensitivity()) {
      this.automata = convertTermsToAutomata(terms, automata);
      // clear the terms array now that we've moved them to be expressed as automata
      terms = ZERO_LEN_BYTES_REF_ARRAY;
    }

    if (terms.length > 0 || strictPhrases.willRewrite()) { //needs MemoryIndex
      // init MemoryIndex
      boolean storePayloads = strictPhrases.hasPositionSensitivity(); // might be needed
      memoryIndex = new MemoryIndex(true, storePayloads);//true==store offsets
      leafReader = (LeafReader) memoryIndex.createSearcher().getIndexReader();
      // preFilter for MemoryIndex
      preMemIndexFilterAutomaton = buildCombinedAutomaton(field, terms, this.automata, strictPhrases);
    } else {
      memoryIndex = null;
      leafReader = null;
      preMemIndexFilterAutomaton = null;
    }

  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.ANALYSIS;
  }

  @Override
  public List<OffsetsEnum> getOffsetsEnums(IndexReader reader, int docId, String content) throws IOException {
    // note: don't need LimitTokenOffsetFilter since content is already truncated to maxLength
    TokenStream tokenStream = tokenStream(content);

    if (memoryIndex != null) { // also handles automata.length > 0
      // We use a MemoryIndex and index the tokenStream so that later we have the PostingsEnum with offsets.

      // note: An *alternative* strategy is to get PostingsEnums without offsets from the main index
      //  and then marry this up with a fake PostingsEnum backed by a TokenStream (which has the offsets) and
      //  can use that to filter applicable tokens?  It would have the advantage of being able to exit
      //  early and save some re-analysis.  This would be an additional method/offset-source approach
      //  since it's still useful to highlight without any index (so we build MemoryIndex).

      // note: probably unwise to re-use TermsEnum on reset mem index so we don't. But we do re-use the
      //   leaf reader, which is a bit more top level than in the guts.
      memoryIndex.reset();

      // Filter the tokenStream to applicable terms
      if (preMemIndexFilterAutomaton != null) {
        tokenStream = newKeepWordFilter(tokenStream, preMemIndexFilterAutomaton);
      }
      memoryIndex.addField(field, tokenStream);//note: calls tokenStream.reset() & close()
      tokenStream = null; // it's consumed; done.
      docId = 0;

      if (automata.length > 0) {
        Terms foundTerms = leafReader.terms(field);
        if (foundTerms == null) {
          return Collections.emptyList(); //No offsets for this field.
        }
        // Un-invert for the automata. Much more compact than a CachingTokenStream
        tokenStream = MultiTermHighlighting.uninvertAndFilterTerms(foundTerms, 0, automata, content.length());
      }

    }

    return createOffsetsEnums(leafReader, docId, tokenStream);
  }

  protected TokenStream tokenStream(String content) throws IOException {
    return MultiValueTokenStream.wrap(field, analyzer, content, UnifiedHighlighter.MULTIVAL_SEP_CHAR);
  }

  private static CharacterRunAutomaton[] convertTermsToAutomata(BytesRef[] terms, CharacterRunAutomaton[] automata) {
    CharacterRunAutomaton[] newAutomata = new CharacterRunAutomaton[terms.length + automata.length];
    for (int i = 0; i < terms.length; i++) {
      newAutomata[i] = MultiTermHighlighting.makeStringMatchAutomata(terms[i]);
    }
    // Append existing automata (that which is used for MTQs)
    System.arraycopy(automata, 0, newAutomata, terms.length, automata.length);
    return newAutomata;
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


  /**
   * Build one {@link CharacterRunAutomaton} matching any term the query might match.
   */
  private static CharacterRunAutomaton buildCombinedAutomaton(String field, BytesRef[] terms,
                                                              CharacterRunAutomaton[] automata,
                                                              PhraseHelper strictPhrases) {
    List<CharacterRunAutomaton> allAutomata = new ArrayList<>();
    if (terms.length > 0) {
      allAutomata.add(new CharacterRunAutomaton(Automata.makeStringUnion(Arrays.asList(terms))));
    }
    Collections.addAll(allAutomata, automata);
    for (SpanQuery spanQuery : strictPhrases.getSpanQueries()) {
      Collections.addAll(allAutomata,
          MultiTermHighlighting.extractAutomata(spanQuery, field, true));//true==lookInSpan
    }

    if (allAutomata.size() == 1) {
      return allAutomata.get(0);
    }
    //TODO it'd be nice if we could get at the underlying Automaton in CharacterRunAutomaton so that we
    //  could union them all. But it's not exposed, and note TermRangeQuery isn't modelled as an Automaton
    //  by MultiTermHighlighting.

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

}
