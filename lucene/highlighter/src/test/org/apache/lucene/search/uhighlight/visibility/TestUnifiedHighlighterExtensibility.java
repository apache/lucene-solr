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

package org.apache.lucene.search.uhighlight.visibility;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.uhighlight.FieldHighlighter;
import org.apache.lucene.search.uhighlight.FieldOffsetStrategy;
import org.apache.lucene.search.uhighlight.OffsetsEnum;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.PassageScorer;
import org.apache.lucene.search.uhighlight.PhraseHelper;
import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.junit.Test;

/**
 * Helps us be aware of visibility/extensibility concerns.
 */
public class TestUnifiedHighlighterExtensibility extends LuceneTestCase {

  /**
   * This test is for maintaining the extensibility of the FieldOffsetStrategy
   * for customizations out of package.
   */
  @Test
  public void testFieldOffsetStrategyExtensibility() {
    final UnifiedHighlighter.OffsetSource offsetSource = UnifiedHighlighter.OffsetSource.NONE_NEEDED;
    FieldOffsetStrategy strategy = new FieldOffsetStrategy("field",
        new BytesRef[0],
        PhraseHelper.NONE,
        new CharacterRunAutomaton[0]) {
      @Override
      public UnifiedHighlighter.OffsetSource getOffsetSource() {
        return offsetSource;
      }

      @Override
      public List<OffsetsEnum> getOffsetsEnums(IndexReader reader, int docId, String content) throws IOException {
        return Collections.emptyList();
      }

      @Override
      protected List<OffsetsEnum> createOffsetsEnumsFromReader(LeafReader leafReader, int doc) throws IOException {
        return super.createOffsetsEnumsFromReader(leafReader, doc);
      }

    };
    assertEquals(offsetSource, strategy.getOffsetSource());
  }

  /**
   * This test is for maintaining the extensibility of the UnifiedHighlighter
   * for customizations out of package.
   */
  @Test
  public void testUnifiedHighlighterExtensibility() {
    final int maxLength = 1000;
    UnifiedHighlighter uh = new UnifiedHighlighter(null, new MockAnalyzer(random())){

      @Override
      protected Map<String, Object[]> highlightFieldsAsObjects(String[] fieldsIn, Query query, int[] docIdsIn, int[] maxPassagesIn) throws IOException {
        return super.highlightFieldsAsObjects(fieldsIn, query, docIdsIn, maxPassagesIn);
      }

      @Override
      protected OffsetSource getOffsetSource(String field) {
        return super.getOffsetSource(field);
      }

      @Override
      protected BreakIterator getBreakIterator(String field) {
        return super.getBreakIterator(field);
      }

      @Override
      protected PassageScorer getScorer(String field) {
        return super.getScorer(field);
      }

      @Override
      protected PassageFormatter getFormatter(String field) {
        return super.getFormatter(field);
      }

      @Override
      public Analyzer getIndexAnalyzer() {
        return super.getIndexAnalyzer();
      }

      @Override
      public IndexSearcher getIndexSearcher() {
        return super.getIndexSearcher();
      }

      @Override
      protected int getMaxNoHighlightPassages(String field) {
        return super.getMaxNoHighlightPassages(field);
      }

      @Override
      protected Boolean requiresRewrite(SpanQuery spanQuery) {
        return super.requiresRewrite(spanQuery);
      }

      @Override
      protected LimitedStoredFieldVisitor newLimitedStoredFieldsVisitor(String[] fields) {
        return super.newLimitedStoredFieldsVisitor(fields);
      }

      @Override
      protected List<CharSequence[]> loadFieldValues(String[] fields, DocIdSetIterator docIter, int cacheCharsThreshold) throws IOException {
        return super.loadFieldValues(fields, docIter, cacheCharsThreshold);
      }

      @Override
      protected FieldHighlighter getFieldHighlighter(String field, Query query, Set<Term> allTerms, int maxPassages) {
        // THIS IS A COPY of the superclass impl; but use CustomFieldHighlighter
        BytesRef[] terms = filterExtractedTerms(getFieldMatcher(field), allTerms);
        Set<HighlightFlag> highlightFlags = getFlags(field);
        PhraseHelper phraseHelper = getPhraseHelper(field, query, highlightFlags);
        CharacterRunAutomaton[] automata = getAutomata(field, query, highlightFlags);
        OffsetSource offsetSource = getOptimizedOffsetSource(field, terms, phraseHelper, automata);
        return new CustomFieldHighlighter(field,
            getOffsetStrategy(offsetSource, field, terms, phraseHelper, automata, highlightFlags),
            new SplittingBreakIterator(getBreakIterator(field), UnifiedHighlighter.MULTIVAL_SEP_CHAR),
            getScorer(field),
            maxPassages,
            getMaxNoHighlightPassages(field),
            getFormatter(field));
      }

      @Override
      protected FieldOffsetStrategy getOffsetStrategy(OffsetSource offsetSource, String field, BytesRef[] terms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Set<HighlightFlag> highlightFlags) {
        return super.getOffsetStrategy(offsetSource, field, terms, phraseHelper, automata, highlightFlags);
      }

      @Override
      public int getMaxLength() {
        return maxLength;
      }
    };
    assertEquals(uh.getMaxLength(), maxLength);
  }

  @Test
  public void testPassageFormatterExtensibility() {
    final Object formattedResponse = new Object();
    PassageFormatter formatter = new PassageFormatter() {
      @Override
      public Object format(Passage[] passages, String content) {
        return formattedResponse;
      }
    };
    assertEquals(formattedResponse, formatter.format(new Passage[0], ""));
  }

  @Test
  public void testFieldHiglighterExtensibility() {
    final String fieldName = "fieldName";
    FieldHighlighter fieldHighlighter = new FieldHighlighter(fieldName, null, null, null, 1, 1, null) {
      @Override
      protected Passage[] highlightOffsetsEnums(List<OffsetsEnum> offsetsEnums) throws IOException {
        return super.highlightOffsetsEnums(offsetsEnums);
      }
    };

    assertEquals(fieldHighlighter.getField(), fieldName);
  }

  /** Tests maintaining extensibility/visibility of {@link org.apache.lucene.search.uhighlight.FieldHighlighter} out of package. */
  private class CustomFieldHighlighter extends FieldHighlighter {
    CustomFieldHighlighter(String field, FieldOffsetStrategy fieldOffsetStrategy, BreakIterator breakIterator, PassageScorer passageScorer, int maxPassages, int maxNoHighlightPassages, PassageFormatter passageFormatter) {
      super(field, fieldOffsetStrategy, breakIterator, passageScorer, maxPassages, maxNoHighlightPassages, passageFormatter);
    }

    @Override
    public Object highlightFieldForDoc(IndexReader reader, int docId, String content) throws IOException {
      return super.highlightFieldForDoc(reader, docId, content);
    }

    @Override
    protected Passage[] highlightOffsetsEnums(List<OffsetsEnum> offsetsEnums) throws IOException {
      // TEST OffsetsEnums & Passage visibility

      // this code never runs; just for compilation
      OffsetsEnum oe = new OffsetsEnum(null, EMPTY);
      oe.getTerm();
      oe.getPostingsEnum();
      oe.freq();
      oe.hasMorePositions();
      oe.nextPosition();
      oe.startOffset();
      oe.endOffset();
      oe.getWeight();
      oe.setWeight(2f);

      Passage p = new Passage();
      p.setStartOffset(0);
      p.setEndOffset(9);
      p.setScore(1f);
      p.addMatch(1, 2, new BytesRef());
      p.reset();
      p.sort();
      //... getters are all exposed; custom PassageFormatter impls uses them

      return super.highlightOffsetsEnums(offsetsEnums);
    }
  }
}
