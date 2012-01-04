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

import org.apache.lucene.analysis.kuromoji.tokenAttributes.BasicFormAttribute;
import org.apache.lucene.analysis.kuromoji.tokenAttributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.SegmentingTokenizerBase;

public final class KuromojiTokenizer extends SegmentingTokenizerBase {
  private static final BreakIterator proto = BreakIterator.getSentenceInstance(Locale.JAPAN);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final BasicFormAttribute basicFormAtt = addAttribute(BasicFormAttribute.class);
  private final PartOfSpeechAttribute posAtt = addAttribute(PartOfSpeechAttribute.class);
  private final org.apache.lucene.analysis.kuromoji.Tokenizer tokenizer;
  
  private List<Token> tokens; 
  private int tokenIndex = 0;
  private int sentenceStart = 0;
  
  public KuromojiTokenizer(org.apache.lucene.analysis.kuromoji.Tokenizer tokenizer, Reader input) {
    super(input, (BreakIterator) proto.clone());
    this.tokenizer = tokenizer;
  }
  
  @Override
  protected void setNextSentence(int sentenceStart, int sentenceEnd) {
    this.sentenceStart = sentenceStart;
    // TODO: allow the tokenizer, at least maybe doTokenize to take char[] or charsequence or characteriterator?
    tokens = tokenizer.tokenize(new String(buffer, sentenceStart, sentenceEnd-sentenceStart));
    tokenIndex = 0;
  }

  @Override
  protected boolean incrementWord() {
    if (tokenIndex == tokens.size()) {
      return false;
    }
    Token token = tokens.get(tokenIndex);
    // TODO: we don't really need the surface form except for its length? (its in the buffer already)
    String surfaceForm = token.getSurfaceForm();
    int position = token.getPosition();
    int length = surfaceForm.length();
    clearAttributes();
    termAtt.copyBuffer(buffer, sentenceStart + position, length);
    int startOffset = offset + sentenceStart + position;
    offsetAtt.setOffset(correctOffset(startOffset), correctOffset(startOffset+length));
    basicFormAtt.setToken(token);
    posAtt.setToken(token);
    tokenIndex++;
    return true;
  }
}
