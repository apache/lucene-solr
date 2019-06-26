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

import java.util.Set;
import java.util.function.Predicate;

import org.apache.lucene.search.Query;
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
  private final Query query;
  private final BytesRef[] terms; // Query: all terms we extracted (some may be position sensitive)
  private final PhraseHelper phraseHelper; // Query: position-sensitive information
  private final CharacterRunAutomaton[] automata; // Query: wildcards (i.e. multi-term query), not position sensitive
  private final boolean hasUnrecognizedQueryPart; // Query: if part of the query (other than the extracted terms / automata) is a leaf we don't know
  private final Set<UnifiedHighlighter.HighlightFlag> highlightFlags;

  public UHComponents(String field, Predicate<String> fieldMatcher, Query query,
                      BytesRef[] terms, PhraseHelper phraseHelper, CharacterRunAutomaton[] automata,
                      boolean hasUnrecognizedQueryPart, Set<UnifiedHighlighter.HighlightFlag> highlightFlags) {
    this.field = field;
    this.fieldMatcher = fieldMatcher;
    this.query = query;
    this.terms = terms;
    this.phraseHelper = phraseHelper;
    this.automata = automata;
    this.hasUnrecognizedQueryPart = hasUnrecognizedQueryPart;
    this.highlightFlags = highlightFlags;
  }

  public String getField() {
    return field;
  }

  public Predicate<String> getFieldMatcher() {
    return fieldMatcher;
  }

  public Query getQuery() {
    return query;
  }

  public BytesRef[] getTerms() {
    return terms;
  }

  public PhraseHelper getPhraseHelper() {
    return phraseHelper;
  }

  public CharacterRunAutomaton[] getAutomata() {
    return automata;
  }

  public boolean hasUnrecognizedQueryPart() {
    return hasUnrecognizedQueryPart;
  }

  public Set<UnifiedHighlighter.HighlightFlag> getHighlightFlags() {
    return highlightFlags;
  }
}
