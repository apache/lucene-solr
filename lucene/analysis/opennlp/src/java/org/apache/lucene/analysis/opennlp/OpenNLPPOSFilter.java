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
import java.util.List;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.opennlp.tools.NLPPOSTaggerOp;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * Run OpenNLP POS tagger.  Tags all terms in the TypeAttribute.
 */
public final class OpenNLPPOSFilter extends TokenFilter {

  private List<AttributeSource> sentenceTokenAttrs = new ArrayList<>();
  String[] tags = null;
  private int tokenNum = 0;
  private boolean moreTokensAvailable = true;

  private final NLPPOSTaggerOp posTaggerOp;
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  public OpenNLPPOSFilter(TokenStream input, NLPPOSTaggerOp posTaggerOp) {
    super(input);
    this.posTaggerOp = posTaggerOp;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if ( ! moreTokensAvailable) {
      clear();
      return false;
    }
    if (tokenNum == sentenceTokenAttrs.size()) { // beginning of stream, or previous sentence exhausted
      String[] sentenceTokens = nextSentence();
      if (sentenceTokens == null) {
        clear();
        return false;
      }
      tags = posTaggerOp.getPOSTags(sentenceTokens);
      tokenNum = 0;
    }
    clearAttributes();
    sentenceTokenAttrs.get(tokenNum).copyTo(this);
    typeAtt.setType(tags[tokenNum++]);
    return true;
  }

  private String[] nextSentence() throws IOException {
    List<String> termList = new ArrayList<>();
    sentenceTokenAttrs.clear();
    boolean endOfSentence = false;
    while ( ! endOfSentence && (moreTokensAvailable = input.incrementToken())) {
      termList.add(termAtt.toString());
      endOfSentence = 0 != (flagsAtt.getFlags() & OpenNLPTokenizer.EOS_FLAG_BIT);
      sentenceTokenAttrs.add(input.cloneAttributes());
    }
    return termList.size() > 0 ? termList.toArray(new String[termList.size()]) : null;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    moreTokensAvailable = true;
    clear();
  }

  private void clear() {
    sentenceTokenAttrs.clear();
    tags = null;
    tokenNum = 0;
  }
}
