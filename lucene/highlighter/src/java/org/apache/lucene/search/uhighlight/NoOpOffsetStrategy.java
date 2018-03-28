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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Never returns offsets. Used when the query would highlight nothing.
 *
 * @lucene.internal
 */
public class NoOpOffsetStrategy extends FieldOffsetStrategy {

  public static final NoOpOffsetStrategy INSTANCE = new NoOpOffsetStrategy();

  private NoOpOffsetStrategy() {
    super("_ignored_", new BytesRef[0], PhraseHelper.NONE, new CharacterRunAutomaton[0]);
  }

  @Override
  public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.NONE_NEEDED;
  }

  @Override
  public OffsetsEnum getOffsetsEnum(IndexReader reader, int docId, String content) throws IOException {
    return OffsetsEnum.EMPTY;
  }

}
