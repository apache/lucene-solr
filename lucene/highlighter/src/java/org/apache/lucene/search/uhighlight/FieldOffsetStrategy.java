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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Ultimately returns a list of {@link OffsetsEnum} yielding potentially highlightable words in the text.  Needs
 * information about the query up front.
 *
 * @lucene.internal
 */
public abstract class FieldOffsetStrategy {

  protected final String field;
  protected final PhraseHelper phraseHelper; // Query: position-sensitive information TODO: rename
  protected BytesRef[] terms; // Query: free-standing terms
  protected CharacterRunAutomaton[] automata; // Query: free-standing wildcards (multi-term query)

  public FieldOffsetStrategy(String field, BytesRef[] queryTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata) {
    this.field = field;
    this.terms = queryTerms;
    this.phraseHelper = phraseHelper;
    this.automata = automata;
  }

  public String getField() {
    return field;
  }

  public abstract UnifiedHighlighter.OffsetSource getOffsetSource();

  /**
   * The primary method -- return offsets for highlightable words in the specified document.
   * IMPORTANT: remember to close them all.
   */
  public abstract List<OffsetsEnum> getOffsetsEnums(IndexReader reader, int docId, String content) throws IOException;

  protected List<OffsetsEnum> createOffsetsEnumsFromReader(LeafReader atomicReader, int doc) throws IOException {
    // For strict positions, get a Map of term to Spans:
    //    note: ScriptPhraseHelper.NONE does the right thing for these method calls
    final Map<BytesRef, Spans> strictPhrasesTermToSpans =
        phraseHelper.getTermToSpans(atomicReader, doc);
    // Usually simply wraps terms in a List; but if willRewrite() then can be expanded
    final List<BytesRef> sourceTerms =
        phraseHelper.expandTermsIfRewrite(terms, strictPhrasesTermToSpans);

    final List<OffsetsEnum> offsetsEnums = new ArrayList<>(sourceTerms.size() + 1);

    Terms termsIndex = atomicReader == null || sourceTerms.isEmpty() ? null : atomicReader.terms(field);
    if (termsIndex != null) {
      TermsEnum termsEnum = termsIndex.iterator();//does not return null
      for (BytesRef term : sourceTerms) {
        if (termsEnum.seekExact(term)) {
          PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);

          if (postingsEnum == null) {
            // no offsets or positions available
            throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
          }

          if (doc == postingsEnum.advance(doc)) { // now it's positioned, although may be exhausted
            postingsEnum = phraseHelper.filterPostings(term, postingsEnum, strictPhrasesTermToSpans.get(term));
            if (postingsEnum != null) {
              offsetsEnums.add(new OffsetsEnum(term, postingsEnum));
            }
          }
        }
      }
    }

    if (automata.length > 0) {
      offsetsEnums.addAll(createAutomataOffsetsEnumsFromReader(atomicReader, doc));
    }

    return offsetsEnums;
  }

  protected List<OffsetsEnum> createAutomataOffsetsEnumsFromReader(LeafReader leafReader, int doc) throws IOException {
    List<OffsetsEnum> offsetsEnums = new LinkedList<>();
    TermsEnum termsEnum = leafReader.terms(field).iterator();
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      for (CharacterRunAutomaton automaton : automata) {
        if (automaton.run(term.utf8ToString())) {
          PostingsEnum postings = termsEnum.postings(null, PostingsEnum.OFFSETS);
          if (doc == postings.advance(doc)) { // now it's positioned, although may be exhausted
            offsetsEnums.add(new OffsetsEnum(new BytesRef(automaton.toString()), postings));
          }
        }
      }
    }
    return offsetsEnums;
  }

}
