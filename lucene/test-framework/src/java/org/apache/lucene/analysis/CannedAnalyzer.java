package org.apache.lucene.analysis;

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
import java.io.IOException;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;

public class CannedAnalyzer extends Analyzer {
  private final Token[] tokens;

  public CannedAnalyzer(Token[] tokens) {
    this.tokens = tokens;
  }

  @Override
  public TokenStreamComponents createComponents(String fieldName, Reader reader) {
    return new TokenStreamComponents(new CannedTokenizer(tokens));
  }

  public static class CannedTokenizer extends Tokenizer {
    private final Token[] tokens;
    private int upto = 0;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

    public CannedTokenizer(Token[] tokens) {
      this.tokens = tokens;
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (upto < tokens.length) {
        final Token token = tokens[upto++];     
        // TODO: can we just capture/restoreState so
        // we get all attrs...?
        clearAttributes();      
        termAtt.setEmpty();
        termAtt.append(token.toString());
        posIncrAtt.setPositionIncrement(token.getPositionIncrement());
        offsetAtt.setOffset(token.startOffset(), token.endOffset());
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      this.upto = 0;
    }
  }
}
