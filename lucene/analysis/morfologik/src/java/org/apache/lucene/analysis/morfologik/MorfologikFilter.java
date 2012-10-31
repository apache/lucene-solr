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
import java.util.*;

import morfologik.stemming.*;
import morfologik.stemming.PolishStemmer.DICTIONARY;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.util.*;

/**
 * {@link TokenFilter} using Morfologik library.
 *
 * MorfologikFilter contains a {@link MorphosyntacticTagsAttribute}, which provides morphosyntactic
 * annotations for produced lemmas. See the Morfologik documentation for details.
 * 
 * @see <a href="http://morfologik.blogspot.com/">Morfologik project page</a>
 */
public class MorfologikFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final MorphosyntacticTagsAttribute tagsAtt = addAttribute(MorphosyntacticTagsAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

  private final CharsRef scratch = new CharsRef(0);
  private final CharacterUtils charUtils;

  private State current;
  private final TokenStream input;
  private final IStemmer stemmer;
  
  private List<WordData> lemmaList;
  private final ArrayList<StringBuilder> tagsList = new ArrayList<StringBuilder>();

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
    
    // SOLR-4007: temporarily substitute context class loader to allow finding dictionary resources.
    Thread me = Thread.currentThread();
    ClassLoader cl = me.getContextClassLoader();
    try {
      me.setContextClassLoader(PolishStemmer.class.getClassLoader());
      this.stemmer = new PolishStemmer(dict);
      this.charUtils = CharacterUtils.getInstance(version);
      this.lemmaList = Collections.emptyList();
    } finally {
      me.setContextClassLoader(cl);
    }  
  }

  private void popNextLemma() {
    // Collect all tags for the next unique lemma.
    CharSequence currentStem;
    int tags = 0;
    do {
      final WordData lemma = lemmaList.get(lemmaListIndex++);
      currentStem = lemma.getStem();
      final CharSequence tag = lemma.getTag();
      if (tag != null) {
        if (tagsList.size() <= tags) {
          tagsList.add(new StringBuilder());
        }

        final StringBuilder buffer = tagsList.get(tags++);  
        buffer.setLength(0);
        buffer.append(lemma.getTag());
      }
    } while (lemmaListIndex < lemmaList.size() &&
             equalCharSequences(lemmaList.get(lemmaListIndex).getStem(), currentStem));

    // Set the lemma's base form and tags as attributes.
    termAtt.setEmpty().append(currentStem);
    tagsAtt.setTags(tagsList.subList(0, tags));
  }

  /**
   * Compare two char sequences for equality. Assumes non-null arguments. 
   */
  private static final boolean equalCharSequences(CharSequence s1, CharSequence s2) {
    int len1 = s1.length();
    int len2 = s2.length();
    if (len1 != len2) return false;
    for (int i = len1; --i >= 0;) {
      if (s1.charAt(i) != s2.charAt(i)) { 
        return false; 
      }
    }
    return true;
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
        tagsAtt.clear();
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
    tagsList.clear();
    super.reset();
  }
}
