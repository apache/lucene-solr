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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

public abstract class AnalysisOffsetStrategy extends FieldOffsetStrategy {

  protected final Analyzer analyzer;

  public AnalysisOffsetStrategy(String field, BytesRef[] queryTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata, Analyzer analyzer) {
    super(field, queryTerms, phraseHelper, automata);
    this.analyzer = analyzer;
  }

  protected TokenStream tokenStream(String content) throws IOException {
    return MultiValueTokenStream.wrap(field, analyzer, content, UnifiedHighlighter.MULTIVAL_SEP_CHAR);
  }

  @Override
  final public UnifiedHighlighter.OffsetSource getOffsetSource() {
    return UnifiedHighlighter.OffsetSource.ANALYSIS;
  }

}
