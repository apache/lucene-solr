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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;

/**
 * Like {@link PostingsOffsetStrategy} but also uses term vectors (only terms needed) for multi-term queries.
 *
 * @lucene.internal
 */
public class PostingsWithTermVectorsOffsetStrategy extends FieldOffsetStrategy {

  public PostingsWithTermVectorsOffsetStrategy(UHComponents components) {
    super(components);
  }

  @Override
  public OffsetsEnum getOffsetsEnum(LeafReader leafReader, int docId, String content) throws IOException {
    Terms docTerms = leafReader.getTermVector(docId, getField());
    if (docTerms == null) {
      return OffsetsEnum.EMPTY;
    }
    leafReader = new TermVectorFilteredLeafReader(leafReader, docTerms, getField());

    return createOffsetsEnumFromReader(leafReader, docId);
  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.POSTINGS_WITH_TERM_VECTORS;
  }
}
