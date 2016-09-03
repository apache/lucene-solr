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
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.highlight.TermVectorLeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

public class TermVectorFieldHighlighter extends AbstractFieldHighlighter {

  public TermVectorFieldHighlighter(String field, PhraseHelper phraseHelper, BytesRef[] queryTerms, CharacterRunAutomaton[] automata, PassageStrategy passageStrategy) {
    super(field, passageStrategy, queryTerms, phraseHelper, automata);
  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.TERM_VECTORS;
  }


  @Override
  public List<OffsetsEnum> getOffsetsEnums(IndexReader reader, int docId, String content) throws IOException {
    LeafReader leafReader = null;
    TokenStream tokenStream = null; // needed when automata.length > 0

    Terms tvTerms = reader.getTermVector(docId, field);
    if (tvTerms == null) {
      return Collections.emptyList();
    }

    if ((terms.length > 0) || strictPhrases.willRewrite()) {
      leafReader = new TermVectorLeafReader(field, tvTerms);
      docId = 0;
    }

    if (automata.length > 0) {
      tokenStream = MultiTermHighlighting.uninvertAndFilterTerms(tvTerms, 0, automata, content.length());
    }

    return createOffsetsEnums(leafReader, docId, tokenStream);
  }

}
