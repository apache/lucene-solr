package org.apache.lucene.analysis.kuromoji;

/**
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

import java.io.Reader;
import java.text.BreakIterator;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.analysis.kuromoji.tokenattributes.BaseFormAttribute;
import org.apache.lucene.analysis.kuromoji.tokenattributes.InflectionAttribute;
import org.apache.lucene.analysis.kuromoji.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.kuromoji.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.SegmentingTokenizerBase;

public final class KuromojiTokenizer extends SegmentingTokenizerBase {
  private static final BreakIterator proto = BreakIterator.getSentenceInstance(Locale.JAPAN);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final BaseFormAttribute basicFormAtt = addAttribute(BaseFormAttribute.class);
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
  private final ReadingAttribute readingAtt = addAttribute(ReadingAttribute.class);
  private final InflectionAttribute inflectionAtt = addAttribute(InflectionAttribute.class);
  private final Segmenter segmenter;
  
  private List<Token> tokens; 
  private int tokenIndex = 0;
  private int sentenceStart = 0;
  
  public KuromojiTokenizer(Reader input) {
    this(new Segmenter(), input);
  }
  
  public KuromojiTokenizer(Segmenter segmenter, Reader input) {
    super(input, (BreakIterator) proto.clone());
    this.segmenter = segmenter;
  }
  
  @Override
  protected void setNextSentence(int sentenceStart, int sentenceEnd) {
    this.sentenceStart = sentenceStart;
    // TODO: maybe don't pass 0 here, so kuromoji tracks offsets for us?
    tokens = segmenter.doTokenize(0, buffer, sentenceStart, sentenceEnd-sentenceStart, true);
    tokenIndex = 0;
  }

  @Override
  protected boolean incrementWord() {
    if (tokenIndex == tokens.size()) {
      return false;
    }
    Token token = tokens.get(tokenIndex);
    int position = token.getPosition();
    int length = token.getLength();
    clearAttributes();
    termAtt.copyBuffer(buffer, sentenceStart + position, length);
    int startOffset = offset + sentenceStart + position;
    offsetAtt.setOffset(correctOffset(startOffset), correctOffset(startOffset+length));
    basicFormAtt.setToken(token);
    posAtt.setToken(token);
    readingAtt.setToken(token);
    inflectionAtt.setToken(token);
    tokenIndex++;
    return true;
  }
}
