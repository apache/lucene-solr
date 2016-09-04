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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
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
  protected BytesRef[] terms; // Query: free-standing terms
  protected PhraseHelper strictPhrases; // Query: position-sensitive information TODO: rename
  protected CharacterRunAutomaton[] automata; // Query: free-standing wildcards (multi-term query)

  public FieldOffsetStrategy(String field, BytesRef[] queryTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata) {
    this.field = field;
    this.terms = queryTerms;
    this.strictPhrases = phraseHelper;
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

  protected List<OffsetsEnum> createOffsetsEnums(LeafReader leafReader, int doc, TokenStream tokenStream) throws IOException {
    List<OffsetsEnum> offsetsEnums = createOffsetsEnumsFromReader(leafReader, doc);
    if (automata.length > 0) {
      offsetsEnums.add(createOffsetsEnumFromTokenStream(doc, tokenStream));
    }
    return offsetsEnums;
  }

  protected List<OffsetsEnum> createOffsetsEnumsFromReader(LeafReader atomicReader, int doc) throws IOException {
    // For strict positions, get a Map of term to Spans:
    //    note: ScriptPhraseHelper.NONE does the right thing for these method calls
    final Map<BytesRef, Spans> strictPhrasesTermToSpans =
        strictPhrases.getTermToSpans(atomicReader, doc);
    // Usually simply wraps terms in a List; but if willRewrite() then can be expanded
    final List<BytesRef> sourceTerms =
        strictPhrases.expandTermsIfRewrite(terms, strictPhrasesTermToSpans);

    final List<OffsetsEnum> offsetsEnums = new ArrayList<>(sourceTerms.size() + 1);

    Terms termsIndex = atomicReader == null || sourceTerms.isEmpty() ? null : atomicReader.terms(field);
    if (termsIndex != null) {
      TermsEnum termsEnum = termsIndex.iterator();//does not return null
      for (BytesRef term : sourceTerms) {
        if (!termsEnum.seekExact(term)) {
          continue; // term not found
        }
        PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.OFFSETS);
        if (postingsEnum == null) {
          // no offsets or positions available
          throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
        }
        if (doc != postingsEnum.advance(doc)) { // now it's positioned, although may be exhausted
          continue;
        }
        postingsEnum = strictPhrases.filterPostings(term, postingsEnum, strictPhrasesTermToSpans.get(term));
        if (postingsEnum == null) {
          continue;// completely filtered out
        }

        offsetsEnums.add(new OffsetsEnum(term, postingsEnum));
      }
    }
    return offsetsEnums;
  }

  protected OffsetsEnum createOffsetsEnumFromTokenStream(int doc, TokenStream tokenStream) throws IOException {
    // if there are automata (MTQ), we have to initialize the "fake" enum wrapping them.
    assert tokenStream != null;
    // TODO Opt: we sometimes evaluate the automata twice when this TS isn't the original; can we avoid?
    PostingsEnum mtqPostingsEnum = MultiTermHighlighting.getDocsEnum(tokenStream, automata);
    assert mtqPostingsEnum instanceof Closeable; // FYI we propagate close() later.
    mtqPostingsEnum.advance(doc);
    return new OffsetsEnum(null, mtqPostingsEnum);
  }
}
