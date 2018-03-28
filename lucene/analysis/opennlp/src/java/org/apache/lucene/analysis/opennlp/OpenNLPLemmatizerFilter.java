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

package org.apache.lucene.analysis.opennlp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.opennlp.tools.NLPLemmatizerOp;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * <p>Runs OpenNLP dictionary-based and/or MaxEnt lemmatizers.</p>
 * <p>
 *   Both a dictionary-based lemmatizer and a MaxEnt lemmatizer are supported,
 *   via the "dictionary" and "lemmatizerModel" params, respectively.
 *   If both are configured, the dictionary-based lemmatizer is tried first,
 *   and then the MaxEnt lemmatizer is consulted for out-of-vocabulary tokens.
 * </p>
 * <p>
 *   The dictionary file must be encoded as UTF-8, with one entry per line,
 *   in the form <tt>word[tab]lemma[tab]part-of-speech</tt>
 * </p>
 */
public class OpenNLPLemmatizerFilter extends TokenFilter {
  private final NLPLemmatizerOp lemmatizerOp;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);
  private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
  private List<AttributeSource> sentenceTokenAttrs = new ArrayList<>();
  private Iterator<AttributeSource> sentenceTokenAttrsIter = null;
  private boolean moreTokensAvailable = true;
  private String[] sentenceTokens = null;     // non-keyword tokens
  private String[] sentenceTokenTypes = null; // types for non-keyword tokens
  private String[] lemmas = null;             // lemmas for non-keyword tokens
  private int lemmaNum = 0;                   // lemma counter

  public OpenNLPLemmatizerFilter(TokenStream input, NLPLemmatizerOp lemmatizerOp) {
    super(input);
    this.lemmatizerOp = lemmatizerOp;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if ( ! moreTokensAvailable) {
      clear();
      return false;
    }
    if (sentenceTokenAttrsIter == null || ! sentenceTokenAttrsIter.hasNext()) {
      nextSentence();
      if (sentenceTokens == null) { // zero non-keyword tokens
        clear();
        return false;
      }
      lemmas = lemmatizerOp.lemmatize(sentenceTokens, sentenceTokenTypes);
      lemmaNum = 0;
      sentenceTokenAttrsIter = sentenceTokenAttrs.iterator();
    }
    clearAttributes();
    sentenceTokenAttrsIter.next().copyTo(this);
    if ( ! keywordAtt.isKeyword()) {
      termAtt.setEmpty().append(lemmas[lemmaNum++]);
    }
    return true;

  }

  private void nextSentence() throws IOException {
    List<String> tokenList = new ArrayList<>();
    List<String> typeList = new ArrayList<>();
    sentenceTokenAttrs.clear();
    boolean endOfSentence = false;
    while ( ! endOfSentence && (moreTokensAvailable = input.incrementToken())) {
      if ( ! keywordAtt.isKeyword()) {
        tokenList.add(termAtt.toString());
        typeList.add(typeAtt.type());
      }
      endOfSentence = 0 != (flagsAtt.getFlags() & OpenNLPTokenizer.EOS_FLAG_BIT);
      sentenceTokenAttrs.add(input.cloneAttributes());
    }
    sentenceTokens = tokenList.size() > 0 ? tokenList.toArray(new String[tokenList.size()]) : null;
    sentenceTokenTypes = typeList.size() > 0 ? typeList.toArray(new String[typeList.size()]) : null;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    moreTokensAvailable = true;
    clear();
  }

  private void clear() {
    sentenceTokenAttrs.clear();
    sentenceTokenAttrsIter = null;
    sentenceTokens = null;
    sentenceTokenTypes = null;
    lemmas = null;
    lemmaNum = 0;
  }
}
