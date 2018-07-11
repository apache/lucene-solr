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

import java.util.function.Predicate;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * A parameter object to hold the components a {@link FieldOffsetStrategy} needs.
 *
 * @lucene.internal
 */
public class UHComponents {
  private final String field;
  private final Predicate<String> fieldMatcher;
  private final BytesRef[] extractedTerms;
  private final PhraseHelper phraseHelper;
  private final CharacterRunAutomaton[] automata;

  public UHComponents(String field, Predicate<String> fieldMatcher, BytesRef[] extractedTerms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata) {
    this.field = field;
    this.fieldMatcher = fieldMatcher;
    this.extractedTerms = extractedTerms;
    this.phraseHelper = phraseHelper;
    this.automata = automata;
  }

  public String getField() {
    return field;
  }

  public Predicate<String> getFieldMatcher() {
    return fieldMatcher;
  }

  public BytesRef[] getExtractedTerms() {
    return extractedTerms;
  }

  public PhraseHelper getPhraseHelper() {
    return phraseHelper;
  }

  public CharacterRunAutomaton[] getAutomata() {
    return automata;
  }
}
