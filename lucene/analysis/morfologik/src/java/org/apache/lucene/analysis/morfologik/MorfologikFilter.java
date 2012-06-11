// -*- c-basic-offset: 2 -*-
package org.apache.lucene.analysis.morfologik;

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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import morfologik.stemming.*;
import morfologik.stemming.PolishStemmer.DICTIONARY;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Version;

/**
 * {@link TokenFilter} using Morfologik library.
 *
 * MorfologikFilter contains a {@link MorphosyntacticTagAttribute}, which provides morphosyntactic
 * annotations for produced lemmas. See the Morfologik documentation for details.
 * 
 * @see <a href="http://morfologik.blogspot.com/">Morfologik project page</a>
 */
public class MorfologikFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final MorphosyntacticTagAttribute tagAtt = addAttribute(MorphosyntacticTagAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  private final CharsRef scratch = new CharsRef(0);
  private final CharacterUtils charUtils;

  private State current;
  private final TokenStream input;
  private final IStemmer stemmer;
  
  private List<WordData> lemmaList;
  private int lemmaListIndex;

  /**
   * Builds a filter for given PolishStemmer.DICTIONARY enum.
   * 
   * @param in   input token stream
   * @param dict PolishStemmer.DICTIONARY enum
   * @param version Lucene version compatibility for lowercasing.
   */
  public MorfologikFilter(final TokenStream in, final DICTIONARY dict, final Version version) {
    super(in);
    this.input = in;
    this.stemmer = new PolishStemmer(dict);
    this.charUtils = CharacterUtils.getInstance(version);
    this.lemmaList = Collections.emptyList();
  }

  private void popNextLemma() {
    final WordData lemma = lemmaList.get(lemmaListIndex++);
    termAtt.setEmpty().append(lemma.getStem());
    tagAtt.setTag(lemma.getTag());
  }

  /**
   * Lookup a given surface form of a token and update 
   * {@link #lemmaList} and {@link #lemmaListIndex} accordingly. 
   */
  private boolean lookupSurfaceForm(CharSequence token) {
      lemmaList = this.stemmer.lookup(token);
      lemmaListIndex = 0;
      return lemmaList.size() > 0;
  }

  /** Retrieves the next token (possibly from the list of lemmas). */
  @Override
  public final boolean incrementToken() throws IOException {
    if (lemmaListIndex < lemmaList.size()) {
      restoreState(current);
      posIncrAtt.setPositionIncrement(0);
      popNextLemma();
      return true;
    } else if (this.input.incrementToken()) {
      if (lookupSurfaceForm(termAtt) || lookupSurfaceForm(toLowercase(termAtt))) {
        current = captureState();
        popNextLemma();
      } else {
        tagAtt.clear();
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Convert to lowercase in-place.
   */
  private CharSequence toLowercase(CharSequence chs) {
    final int length = scratch.length = chs.length();
    scratch.grow(length);

    char buffer[] = scratch.chars;
    for (int i = 0; i < length;) {
      i += Character.toChars(
          Character.toLowerCase(charUtils.codePointAt(chs, i)), buffer, i);      
    }

    return scratch;
  }

  /** Resets stems accumulator and hands over to superclass. */
  @Override
  public void reset() throws IOException {
    lemmaListIndex = 0;
    lemmaList = Collections.emptyList();
    super.reset();
  }
}
