package org.apache.lucene.analysis.hunspell;

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
import java.util.List;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.hunspell.HunspellStemmer.Stem;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

/**
 * TokenFilter that uses hunspell affix rules and words to stem tokens.  Since hunspell supports a word having multiple
 * stems, this filter can emit multiple tokens for each consumed token
 *
 * <p>
 * Note: This filter is aware of the {@link KeywordAttribute}. To prevent
 * certain terms from being passed to the stemmer
 * {@link KeywordAttribute#isKeyword()} should be set to <code>true</code>
 * in a previous {@link TokenStream}.
 *
 * Note: For including the original term as well as the stemmed version, see
 * {@link org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilterFactory}
 * </p>
 *
 *
 */
public final class HunspellStemFilter extends TokenFilter {
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private final HunspellStemmer stemmer;
  
  private List<Stem> buffer;
  private State savedState;
  
  private final boolean dedup;

  /**
   * Creates a new HunspellStemFilter that will stem tokens from the given TokenStream using affix rules in the provided
   * HunspellDictionary
   *
   * @param input TokenStream whose tokens will be stemmed
   * @param dictionary HunspellDictionary containing the affix rules and words that will be used to stem the tokens
   */
  public HunspellStemFilter(TokenStream input, HunspellDictionary dictionary) {
    this(input, dictionary, true);
  }
  
  /**
   * Creates a new HunspellStemFilter that will stem tokens from the given TokenStream using affix rules in the provided
   * HunspellDictionary
   *
   * @param input TokenStream whose tokens will be stemmed
   * @param dictionary HunspellDictionary containing the affix rules and words that will be used to stem the tokens
   * @param dedup true if only unique terms should be output.
   */
  public HunspellStemFilter(TokenStream input, HunspellDictionary dictionary, boolean dedup) {
    super(input);
    this.dedup = dedup;
    this.stemmer = new HunspellStemmer(dictionary);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean incrementToken() throws IOException {
    if (buffer != null && !buffer.isEmpty()) {
      Stem nextStem = buffer.remove(0);
      restoreState(savedState);
      posIncAtt.setPositionIncrement(0);
      termAtt.copyBuffer(nextStem.getStem(), 0, nextStem.getStemLength());
      termAtt.setLength(nextStem.getStemLength());
      return true;
    }
    
    if (!input.incrementToken()) {
      return false;
    }
    
    if (keywordAtt.isKeyword()) {
      return true;
    }
    
    buffer = dedup ? stemmer.uniqueStems(termAtt.buffer(), termAtt.length()) : stemmer.stem(termAtt.buffer(), termAtt.length());

    if (buffer.isEmpty()) { // we do not know this word, return it unchanged
      return true;
    }     

    Stem stem = buffer.remove(0);
    termAtt.copyBuffer(stem.getStem(), 0, stem.getStemLength());
    termAtt.setLength(stem.getStemLength());

    if (!buffer.isEmpty()) {
      savedState = captureState();
    }

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
    super.reset();
    buffer = null;
  }
}
