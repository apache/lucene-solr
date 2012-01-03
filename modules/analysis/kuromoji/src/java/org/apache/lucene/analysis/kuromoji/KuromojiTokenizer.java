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

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class KuromojiTokenizer extends Tokenizer {
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final org.apache.lucene.analysis.kuromoji.Tokenizer tokenizer;
  
  private String str;
  
  private List<Token> tokens;
  
  private int tokenIndex = 0;
  
  public KuromojiTokenizer(org.apache.lucene.analysis.kuromoji.Tokenizer tokenizer, Reader input) throws IOException {
    super(input);
    this.tokenizer = tokenizer;
    // nocommit: this won't really work for large docs.
    // what kind of context does kuromoji need? just sentence maybe?
    str = IOUtils.toString(input);
    init();
  }
  
  private void init() {
    tokenIndex = 0;
    tokens = tokenizer.tokenize(str);
  }
  
  @Override
  public boolean incrementToken() {
    if(tokenIndex == tokens.size()) {
      return false;
    }
    
    Token token = tokens.get(tokenIndex);
    String surfaceForm = token.getSurfaceForm();
    int position = token.getPosition();
    int length = surfaceForm.length();
    
    termAtt.setEmpty().append(str, position, length);
    offsetAtt.setOffset(correctOffset(position), correctOffset(position + length));
    typeAtt.setType(token.getPartOfSpeech());
    tokenIndex++;
    return true;
  }
  
  @Override
  public void end() {
    final int ofs = correctOffset(str.length());
    offsetAtt.setOffset(ofs, ofs);
  }
  
  @Override
  public void reset(Reader input) throws IOException{
    super.reset(input);
    str = IOUtils.toString(input);
    init();
  }
  
}
