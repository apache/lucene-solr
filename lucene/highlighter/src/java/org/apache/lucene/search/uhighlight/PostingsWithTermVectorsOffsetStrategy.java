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
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Like {@link PostingsOffsetStrategy} but also uses term vectors (only terms needed) for multi-term queries.
 *
 * @lucene.internal
 */
public class PostingsWithTermVectorsOffsetStrategy extends FieldOffsetStrategy {

  public PostingsWithTermVectorsOffsetStrategy(String field, BytesRef[] queryTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata) {
    super(field, queryTerms, phraseHelper, automata);
  }

  @Override
  public OffsetsEnum getOffsetsEnum(IndexReader reader, int docId, String content) throws IOException {
    LeafReader leafReader;
    if (reader instanceof LeafReader) {
      leafReader = (LeafReader) reader;
    } else {
      List<LeafReaderContext> leaves = reader.leaves();
      LeafReaderContext LeafReaderContext = leaves.get(ReaderUtil.subIndex(docId, leaves));
      leafReader = LeafReaderContext.reader();
      docId -= LeafReaderContext.docBase; // adjust 'doc' to be within this atomic reader
    }

    Terms docTerms = leafReader.getTermVector(docId, field);
    if (docTerms == null) {
      return OffsetsEnum.EMPTY;
    }
    leafReader = new TermVectorFilteredLeafReader(leafReader, docTerms);

    return createOffsetsEnumFromReader(leafReader, docId);
  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.POSTINGS_WITH_TERM_VECTORS;
  }
}
