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
package org.apache.lucene.analysis.morfologik;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRefBuilder;

import morfologik.stemming.Dictionary;
import morfologik.stemming.DictionaryLookup;
import morfologik.stemming.IStemmer;
import morfologik.stemming.WordData;
import morfologik.stemming.polish.PolishStemmer;

/**
 * {@link TokenFilter} using Morfologik library to transform input tokens into lemma and
 * morphosyntactic (POS) tokens. Applies to Polish only.  
 *
 * <p>MorfologikFilter contains a {@link MorphosyntacticTagsAttribute}, which provides morphosyntactic
 * annotations for produced lemmas. See the Morfologik documentation for details.</p>
 * 
 * @see <a href="http://morfologik.blogspot.com/">Morfologik project page</a>
 */
public class MorfologikFilter extends TokenFilter {

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final MorphosyntacticTagsAttribute tagsAtt = addAttribute(MorphosyntacticTagsAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  private final CharsRefBuilder scratch = new CharsRefBuilder();

  private State current;
  private final TokenStream input;
  private final IStemmer stemmer;
  
  private List<WordData> lemmaList;
  private final ArrayList<StringBuilder> tagsList = new ArrayList<>();

  private int lemmaListIndex;

  /**
   * Creates a filter with the default (Polish) dictionary.
   */
  public MorfologikFilter(final TokenStream in) {
    this(in, new PolishStemmer().getDictionary());
  }

  /**
   * Creates a filter with a given dictionary.
   *
   * @param in input token stream.
   * @param dict Dictionary to use for stemming.
   */
  public MorfologikFilter(final TokenStream in, final Dictionary dict) {
    super(in);
    this.input = in;
    this.stemmer = new DictionaryLookup(dict);
    this.lemmaList = Collections.emptyList();
  }
  
  /**
   * A pattern used to split lemma forms.
   */
  private final static Pattern lemmaSplitter = Pattern.compile("\\+|\\|");

  private void popNextLemma() {
    // One tag (concatenated) per lemma.
    final WordData lemma = lemmaList.get(lemmaListIndex++);
    termAtt.setEmpty().append(lemma.getStem());
    CharSequence tag = lemma.getTag();
    if (tag != null) {
      String[] tags = lemmaSplitter.split(tag.toString());
      for (int i = 0; i < tags.length; i++) {
        if (tagsList.size() <= i) {
          tagsList.add(new StringBuilder());
        }
        StringBuilder buffer = tagsList.get(i);
        buffer.setLength(0);
        buffer.append(tags[i]);
      }
      tagsAtt.setTags(tagsList.subList(0, tags.length));
    } else {
      tagsAtt.setTags(Collections.<StringBuilder> emptyList());
    }
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
      if (!keywordAttr.isKeyword() && 
          (lookupSurfaceForm(termAtt) || lookupSurfaceForm(toLowercase(termAtt)))) {
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
    final int length = chs.length();
    scratch.setLength(length);
    scratch.grow(length);

    char buffer[] = scratch.chars();
    for (int i = 0; i < length;) {
      i += Character.toChars(
          Character.toLowerCase(Character.codePointAt(chs, i)), buffer, i);      
    }

    return scratch.get();
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
