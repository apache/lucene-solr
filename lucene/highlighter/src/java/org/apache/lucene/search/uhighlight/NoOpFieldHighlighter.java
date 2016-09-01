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
import java.text.BreakIterator;

import org.apache.lucene.index.IndexReader;

public class NoOpFieldHighlighter implements FieldHighlighter {

  private final PassageStrategy passageStrategy;
  private final String field;

  public NoOpFieldHighlighter(String field, PassageStrategy passageStrategy) {
    this.field = field;
    this.passageStrategy = passageStrategy;
  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.NONE_NEEDED;
  }

  @Override
  public Object highlightFieldForDoc(IndexReader reader, int docId, String content, int maxPassages) throws IOException {
    // note: it'd be nice to accept a CharSequence for content, but we need a CharacterIterator impl for it.
    if (content.length() == 0) {
      return null; // nothing to do
    }
    BreakIterator breakIterator = passageStrategy.getBreakIterator();
    breakIterator.setText(content);
    Passage[] passages = passageStrategy.getSummaryPassagesNoHighlight(maxPassages);
    if (passages.length > 0) {
      return passageStrategy.getPassageFormatter().format(passages, content);
    } else {
      return null;
    }
  }

  @Override
  public String getField() {
    return field;
  }

}
