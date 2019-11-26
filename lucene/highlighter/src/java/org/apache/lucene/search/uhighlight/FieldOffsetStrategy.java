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
import java.util.List;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;

/**
 * Ultimately returns an {@link OffsetsEnum} yielding potentially highlightable words in the text.  Needs
 * information about the query up front.
 *
 * @lucene.internal
 */
public abstract class FieldOffsetStrategy {

  protected final UHComponents components;

  public FieldOffsetStrategy(UHComponents components) {
    this.components = components;
  }

  public String getField() {
    return components.getField();
  }

  public abstract UnifiedHighlighter.OffsetSource getOffsetSource();

  /**
   * The primary method -- return offsets for highlightable words in the specified document.
   *
   * Callers are expected to close the returned OffsetsEnum when it has been finished with
   */
  public abstract OffsetsEnum getOffsetsEnum(LeafReader reader, int docId, String content) throws IOException;

  protected OffsetsEnum createOffsetsEnumFromReader(LeafReader leafReader, int doc) throws IOException {
    final Terms termsIndex = leafReader.terms(getField());
    if (termsIndex == null) {
      return OffsetsEnum.EMPTY;
    }

    final List<OffsetsEnum> offsetsEnums = new ArrayList<>();

    // Handle Weight.matches approach
    if (components.getHighlightFlags().contains(UnifiedHighlighter.HighlightFlag.WEIGHT_MATCHES)) {

      createOffsetsEnumsWeightMatcher(leafReader, doc, offsetsEnums);

    } else { // classic approach

      // Handle position insensitive terms (a subset of this.terms field):
      final BytesRef[] insensitiveTerms;
      final PhraseHelper phraseHelper = components.getPhraseHelper();
      final BytesRef[] terms = components.getTerms();
      if (phraseHelper.hasPositionSensitivity()) {
        insensitiveTerms = phraseHelper.getAllPositionInsensitiveTerms();
        assert insensitiveTerms.length <= terms.length : "insensitive terms should be smaller set of all terms";
      } else {
        insensitiveTerms = terms;
      }
      if (insensitiveTerms.length > 0) {
        createOffsetsEnumsForTerms(insensitiveTerms, termsIndex, doc, offsetsEnums);
      }

      // Handle spans
      if (phraseHelper.hasPositionSensitivity()) {
        phraseHelper.createOffsetsEnumsForSpans(leafReader, doc, offsetsEnums);
      }

      // Handle automata
      if (components.getAutomata().length > 0) {
        createOffsetsEnumsForAutomata(termsIndex, doc, offsetsEnums);
      }
    }

    switch (offsetsEnums.size()) {
      case 0: return OffsetsEnum.EMPTY;
      case 1: return offsetsEnums.get(0);
      default: return new OffsetsEnum.MultiOffsetsEnum(offsetsEnums);
    }
  }

  protected void createOffsetsEnumsWeightMatcher(LeafReader _leafReader, int docId, List<OffsetsEnum> results) throws IOException {
    // remap fieldMatcher/requireFieldMatch fields to the field we are highlighting
    LeafReader leafReader = new FilterLeafReader(_leafReader) {
      @Override
      public Terms terms(String field) throws IOException {
        if (components.getFieldMatcher().test(field)) {
          return super.terms(components.getField());
        } else {
          return super.terms(field);
        }
      }

      //  So many subclasses do this!
      //these ought to be a default or added via some intermediary like "FilterTransientLeafReader" (exception on close).
      @Override
      public CacheHelper getCoreCacheHelper() {
        return null;
      }

      @Override
      public CacheHelper getReaderCacheHelper() {
        return null;
      }
    };
    IndexSearcher indexSearcher = new IndexSearcher(leafReader);
    indexSearcher.setQueryCache(null);
    Matches matches = indexSearcher.rewrite(components.getQuery())
        .createWeight(indexSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f)
        .matches(leafReader.getContext(), docId);
    if (matches == null) {
      return; // doc doesn't match
    }
    for (String field : matches) {
      if (components.getFieldMatcher().test(field)) {
        MatchesIterator iterator = matches.getMatches(field);
        if (iterator == null) {
          continue;
        }
        results.add(new OffsetsEnum.OfMatchesIteratorWithSubs(iterator));
      }
    }

  }

  protected void createOffsetsEnumsForTerms(BytesRef[] sourceTerms, Terms termsIndex, int doc, List<OffsetsEnum> results) throws IOException {
    TermsEnum termsEnum = termsIndex.iterator();//does not return null
    for (BytesRef term : sourceTerms) {
      if (termsEnum.seekExact(term)) {
        PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);
        if (postingsEnum == null) {
          // no offsets or positions available
          throw new IllegalArgumentException("field '" + getField() + "' was indexed without offsets, cannot highlight");
        }
        if (doc == postingsEnum.advance(doc)) { // now it's positioned, although may be exhausted
          results.add(new OffsetsEnum.OfPostings(term, postingsEnum));
        }
      }
    }
  }

  protected void createOffsetsEnumsForAutomata(Terms termsIndex, int doc, List<OffsetsEnum> results) throws IOException {
    final LabelledCharArrayMatcher[] automata = components.getAutomata();
    List<List<PostingsEnum>> automataPostings = new ArrayList<>(automata.length);
    for (int i = 0; i < automata.length; i++) {
      automataPostings.add(new ArrayList<>());
    }

    TermsEnum termsEnum = termsIndex.iterator();
    BytesRef term;

    CharsRefBuilder refBuilder = new CharsRefBuilder();
    while ((term = termsEnum.next()) != null) {
      for (int i = 0; i < automata.length; i++) {
        CharArrayMatcher automaton = automata[i];
        refBuilder.copyUTF8Bytes(term);
        if (automaton.match(refBuilder.get())) {
          PostingsEnum postings = termsEnum.postings(null, PostingsEnum.OFFSETS);
          if (doc == postings.advance(doc)) {
            automataPostings.get(i).add(postings);
          }
        }
      }
    }

    for (int i = 0; i < automata.length; i++) {
      LabelledCharArrayMatcher automaton = automata[i];
      List<PostingsEnum> postingsEnums = automataPostings.get(i);
      if (postingsEnums.isEmpty()) {
        continue;
      }
      // Build one OffsetsEnum exposing the automaton label as the term, and the sum of freq
      BytesRef wildcardTerm = new BytesRef(automaton.getLabel());
      int sumFreq = 0;
      for (PostingsEnum postingsEnum : postingsEnums) {
        sumFreq += postingsEnum.freq();
      }
      for (PostingsEnum postingsEnum : postingsEnums) {
        results.add(new OffsetsEnum.OfPostings(wildcardTerm, sumFreq, postingsEnum));
      }
    }

  }

}
