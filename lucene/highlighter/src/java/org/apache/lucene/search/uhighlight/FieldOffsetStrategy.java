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
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.lucene.util.CharsRefBuilder;
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

  protected List<OffsetsEnum> createOffsetsEnumsFromReader(LeafReader leafReader, int doc) throws IOException {
    final Terms termsIndex = leafReader.terms(field);
    if (termsIndex == null) {
      return Collections.emptyList();
    }

    // For strict positions, get a Map of term to Spans:
    //    note: ScriptPhraseHelper.NONE does the right thing for these method calls
    final Map<BytesRef, Spans> strictPhrasesTermToSpans =
        phraseHelper.getTermToSpans(leafReader, doc);
    // Usually simply wraps terms in a List; but if willRewrite() then can be expanded
    final List<BytesRef> sourceTerms =
        phraseHelper.expandTermsIfRewrite(terms, strictPhrasesTermToSpans);

    final List<OffsetsEnum> offsetsEnums = new ArrayList<>(sourceTerms.size() + automata.length);

    // Handle sourceTerms:
    if (!sourceTerms.isEmpty()) {
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

    // Handle automata
    if (automata.length > 0) {
      offsetsEnums.addAll(createAutomataOffsetsEnumsFromReader(termsIndex, doc));
    }

    return offsetsEnums;
  }

  protected List<OffsetsEnum> createAutomataOffsetsEnumsFromReader(Terms termsIndex, int doc) throws IOException {
    //debatable if a LinkedList is faster, but depends on the number of encountered terms
    Map<CharacterRunAutomaton, List<PostingsEnum>> automataPostings = new HashMap<>(automata.length);
    for (CharacterRunAutomaton automaton : automata) {
      automataPostings.put(automaton, new LinkedList<>());
    }

    TermsEnum termsEnum = termsIndex.iterator();
    BytesRef term;
    CharsRefBuilder refBuilder = new CharsRefBuilder();
    while ((term = termsEnum.next()) != null) {
      for (CharacterRunAutomaton automaton : automata) {
        refBuilder.copyUTF8Bytes(term);
        if (automaton.run(refBuilder.chars(), 0, refBuilder.length())) {
          PostingsEnum postings = termsEnum.postings(null, PostingsEnum.OFFSETS);
          if (doc == postings.advance(doc)) {
            automataPostings.get(automaton).add(postings);
          }
        }
      }
    }

    List<OffsetsEnum> offsetsEnums = new LinkedList<>();
    for (Map.Entry<CharacterRunAutomaton, List<PostingsEnum>> automatonPostings : automataPostings.entrySet()) {
      List<PostingsEnum> postingsEnums = automatonPostings.getValue();
      int size = postingsEnums.size();
      if (size > 0) { //only add if we have offsets
        BytesRef wildcardTerm = new BytesRef(automatonPostings.getKey().toString());
        if (size == 1) { //don't wrap in a composite if there's only one OffsetsEnum
          offsetsEnums.add(new OffsetsEnum(wildcardTerm, postingsEnums.get(0)));
        } else {
          offsetsEnums.add(new OffsetsEnum(wildcardTerm, new CompositePostingsEnum(wildcardTerm, postingsEnums)));
        }
      }
    }

    return offsetsEnums;
  }

}
