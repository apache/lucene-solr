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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.highlight.TermVectorLeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Uses term vectors that contain offsets.
 *
 * @lucene.internal
 */
public class TermVectorOffsetStrategy extends FieldOffsetStrategy {

  public TermVectorOffsetStrategy(String field, BytesRef[] queryTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata) {
    super(field, queryTerms, phraseHelper, automata);
  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.TERM_VECTORS;
  }

  @Override
  public OffsetsEnum getOffsetsEnum(IndexReader reader, int docId, String content) throws IOException {
    Terms tvTerms = reader.getTermVector(docId, field);
    if (tvTerms == null) {
      return OffsetsEnum.EMPTY;
    }

    LeafReader leafReader = new TermVectorLeafReader(field, tvTerms);
    docId = 0;

    return createOffsetsEnumFromReader(leafReader, docId);
  }

}
