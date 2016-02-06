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
package org.apache.lucene.analysis.cn.smart;


import java.io.IOException;
import java.text.BreakIterator;
import java.util.Iterator;
import java.util.Locale;

import org.apache.lucene.analysis.cn.smart.hhmm.SegToken;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.SegmentingTokenizerBase;
import org.apache.lucene.util.AttributeFactory;

/**
 * Tokenizer for Chinese or mixed Chinese-English text.
 * <p>
 * The analyzer uses probabilistic knowledge to find the optimal word segmentation for Simplified Chinese text.
 * The text is first broken into sentences, then each sentence is segmented into words.
 */
public class HMMChineseTokenizer extends SegmentingTokenizerBase {
  /** used for breaking the text into sentences */
  private static final BreakIterator sentenceProto = BreakIterator.getSentenceInstance(Locale.ROOT);
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  
  private final WordSegmenter wordSegmenter = new WordSegmenter();
  private Iterator<SegToken> tokens;

  /** Creates a new HMMChineseTokenizer */
  public HMMChineseTokenizer() {
    this(DEFAULT_TOKEN_ATTRIBUTE_FACTORY);
  }
      
  /** Creates a new HMMChineseTokenizer, supplying the AttributeFactory */
  public HMMChineseTokenizer(AttributeFactory factory) {
    super(factory, (BreakIterator)sentenceProto.clone());
  }

  @Override
  protected void setNextSentence(int sentenceStart, int sentenceEnd) {
    String sentence = new String(buffer, sentenceStart, sentenceEnd - sentenceStart);
    tokens = wordSegmenter.segmentSentence(sentence, offset + sentenceStart).iterator();
  }

  @Override
  protected boolean incrementWord() {
    if (tokens == null || !tokens.hasNext()) {
      return false;
    } else {
      SegToken token = tokens.next();
      clearAttributes();
      termAtt.copyBuffer(token.charArray, 0, token.charArray.length);
      offsetAtt.setOffset(correctOffset(token.startOffset), correctOffset(token.endOffset));
      typeAtt.setType("word");
      return true;
    }
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    tokens = null;
  }
}
