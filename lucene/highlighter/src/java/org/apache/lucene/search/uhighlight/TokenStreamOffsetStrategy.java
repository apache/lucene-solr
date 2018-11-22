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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.index.LeafReader;
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

  public TokenStreamOffsetStrategy(UHComponents components, Analyzer indexAnalyzer) {
    super(new UHComponents(
            components.getField(),
            components.getFieldMatcher(),
            components.getQuery(),
            ZERO_LEN_BYTES_REF_ARRAY,
            components.getPhraseHelper(),
            convertTermsToAutomata(components.getTerms(), components.getAutomata()),
            components.getHighlightFlags()),
        indexAnalyzer);
    assert components.getPhraseHelper().hasPositionSensitivity() == false;
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
  public OffsetsEnum getOffsetsEnum(LeafReader reader, int docId, String content) throws IOException {
    return new TokenStreamOffsetsEnum(tokenStream(content), components.getAutomata());
  }

  private static class TokenStreamOffsetsEnum extends OffsetsEnum {
    TokenStream stream; // becomes null when closed
    final CharacterRunAutomaton[] matchers;
    final CharTermAttribute charTermAtt;
    final OffsetAttribute offsetAtt;

    int currentMatch = -1;

    final BytesRef matchDescriptions[];

    TokenStreamOffsetsEnum(TokenStream ts, CharacterRunAutomaton[] matchers) throws IOException {
      this.stream = ts;
      this.matchers = matchers;
      matchDescriptions = new BytesRef[matchers.length];
      charTermAtt = ts.addAttribute(CharTermAttribute.class);
      offsetAtt = ts.addAttribute(OffsetAttribute.class);
      ts.reset();
    }

    @Override
    public boolean nextPosition() throws IOException {
      if (stream != null) {
        while (stream.incrementToken()) {
          for (int i = 0; i < matchers.length; i++) {
            if (matchers[i].run(charTermAtt.buffer(), 0, charTermAtt.length())) {
              currentMatch = i;
              return true;
            }
          }
        }
        stream.end();
        close();
      }
      // exhausted
      return false;
    }

    @Override
    public int freq() throws IOException {
      return Integer.MAX_VALUE; // lie
    }


    @Override
    public int startOffset() throws IOException {
      return offsetAtt.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return offsetAtt.endOffset();
    }

    @Override
    public BytesRef getTerm() throws IOException {
      if (matchDescriptions[currentMatch] == null) {
        // these CharRunAutomata are subclassed so that toString() returns the query
        matchDescriptions[currentMatch] = new BytesRef(matchers[currentMatch].toString());
      }
      return matchDescriptions[currentMatch];
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
