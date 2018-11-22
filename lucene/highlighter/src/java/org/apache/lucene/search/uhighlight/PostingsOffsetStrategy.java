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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;

/**
 * Uses offsets in postings -- {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}.  This
 * does not support multi-term queries; the highlighter will fallback on analysis for that.
 *
 * @lucene.internal
 */
public class PostingsOffsetStrategy extends FieldOffsetStrategy {

  public PostingsOffsetStrategy(UHComponents components) {
    super(components);
  }

  @Override
  public OffsetsEnum getOffsetsEnum(LeafReader reader, int docId, String content) throws IOException {
    return createOffsetsEnumFromReader(reader, docId);
  }


  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.POSTINGS;
  }
}
