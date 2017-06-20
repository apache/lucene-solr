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
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Analyzes the text, producing a single {@link OffsetsEnum} wrapping the {@link TokenStream} filtered to terms
 * in the query, including wildcards.  It can't handle position-sensitive queries (phrases). Passage accuracy suffers
 * because the freq() is unknown -- it's always {@link Integer#MAX_VALUE} instead.
 */
public class TokenStreamOffsetStrategy extends AnalysisOffsetStrategy {

  private static final BytesRef[] ZERO_LEN_BYTES_REF_ARRAY = new BytesRef[0];

  public TokenStreamOffsetStrategy(String field, BytesRef[] terms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Analyzer indexAnalyzer) {
    super(field, ZERO_LEN_BYTES_REF_ARRAY, phraseHelper, convertTermsToAutomata(terms, automata), indexAnalyzer);
    assert phraseHelper.hasPositionSensitivity() == false;
  }

  private static CharacterRunAutomaton[] convertTermsToAutomata(BytesRef[] terms, CharacterRunAutomaton[] automata) {
    CharacterRunAutomaton[] newAutomata = new CharacterRunAutomaton[terms.length + automata.length];
    for (int i = 0; i < terms.length; i++) {
      String termString = terms[i].utf8ToString();
      newAutomata[i] = new CharacterRunAutomaton(Automata.makeString(termString)) {
        @Override
        public String toString() {
          return termString;
        }
      };
    }
    // Append existing automata (that which is used for MTQs)
    System.arraycopy(automata, 0, newAutomata, terms.length, automata.length);
    return newAutomata;
  }

  @Override
  public List<OffsetsEnum> getOffsetsEnums(IndexReader reader, int docId, String content) throws IOException {
    TokenStream tokenStream = tokenStream(content);
    PostingsEnum mtqPostingsEnum = new TokenStreamPostingsEnum(tokenStream, automata);
    mtqPostingsEnum.advance(docId);
    return Collections.singletonList(new OffsetsEnum(null, mtqPostingsEnum));
  }

  // See class javadocs.
  // TODO: DWS perhaps instead OffsetsEnum could become abstract and this would be an impl?  See TODOs in OffsetsEnum.
  private static class TokenStreamPostingsEnum extends PostingsEnum implements Closeable {
    TokenStream stream; // becomes null when closed
    final CharacterRunAutomaton[] matchers;
    final CharTermAttribute charTermAtt;
    final OffsetAttribute offsetAtt;

    int currentDoc = -1;
    int currentMatch = -1;
    int currentStartOffset = -1;

    int currentEndOffset = -1;

    final BytesRef matchDescriptions[];

    TokenStreamPostingsEnum(TokenStream ts, CharacterRunAutomaton[] matchers) throws IOException {
      this.stream = ts;
      this.matchers = matchers;
      matchDescriptions = new BytesRef[matchers.length];
      charTermAtt = ts.addAttribute(CharTermAttribute.class);
      offsetAtt = ts.addAttribute(OffsetAttribute.class);
      ts.reset();
    }

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
        close();
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

    // TOTAL HACK; used in OffsetsEnum.getTerm()
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

    @Override
    public void close() throws IOException {
      if (stream != null) {
        stream.close();
        stream = null;
      }
    }
  }
}
