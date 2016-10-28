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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

public class TokenStreamOffsetStrategy extends AnalysisOffsetStrategy {

  private static final BytesRef[] ZERO_LEN_BYTES_REF_ARRAY = new BytesRef[0];

  public TokenStreamOffsetStrategy(String field, BytesRef[] terms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Analyzer indexAnalyzer) {
    super(field, terms, phraseHelper, automata, indexAnalyzer);
    this.automata = convertTermsToAutomata(terms, automata);
    this.terms = ZERO_LEN_BYTES_REF_ARRAY;
  }

  @Override
  public List<OffsetsEnum> getOffsetsEnums(IndexReader reader, int docId, String content) throws IOException {
    TokenStream tokenStream = tokenStream(content);
    PostingsEnum mtqPostingsEnum = MultiTermHighlighting.getDocsEnum(tokenStream, automata);
    assert mtqPostingsEnum instanceof Closeable; // FYI we propagate close() later.
    mtqPostingsEnum.advance(docId);
    return Collections.singletonList(new OffsetsEnum(null, mtqPostingsEnum));
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

}
