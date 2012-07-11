package org.apache.lucene.analysis.miscellaneous;

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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;

/**
 * Links two {@link PrefixAwareTokenFilter}.
 * <p/>
 * <b>NOTE:</b> This filter might not behave correctly if used with custom Attributes, i.e. Attributes other than
 * the ones located in org.apache.lucene.analysis.tokenattributes. 
 */
public class PrefixAndSuffixAwareTokenFilter extends TokenStream {

  private PrefixAwareTokenFilter suffix;

  public PrefixAndSuffixAwareTokenFilter(TokenStream prefix, TokenStream input, TokenStream suffix) {
    super(suffix);
    prefix = new PrefixAwareTokenFilter(prefix, input) {
      @Override
      public Token updateSuffixToken(Token suffixToken, Token lastInputToken) {
        return PrefixAndSuffixAwareTokenFilter.this.updateInputToken(suffixToken, lastInputToken);
      }
    };
    this.suffix = new PrefixAwareTokenFilter(prefix, suffix) {
      @Override
      public Token updateSuffixToken(Token suffixToken, Token lastInputToken) {
        return PrefixAndSuffixAwareTokenFilter.this.updateSuffixToken(suffixToken, lastInputToken);
      }
    };
  }

  public Token updateInputToken(Token inputToken, Token lastPrefixToken) {
    inputToken.setOffset(lastPrefixToken.endOffset() + inputToken.startOffset(), 
                         lastPrefixToken.endOffset() + inputToken.endOffset());
    return inputToken;
  }

  public Token updateSuffixToken(Token suffixToken, Token lastInputToken) {
    suffixToken.setOffset(lastInputToken.endOffset() + suffixToken.startOffset(),
                          lastInputToken.endOffset() + suffixToken.endOffset());
    return suffixToken;
  }


  @Override
  public final boolean incrementToken() throws IOException {
    return suffix.incrementToken();
  }

  @Override
  public void reset() throws IOException {
    suffix.reset();
  }


  @Override
  public void close() throws IOException {
    suffix.close();
  }

  @Override
  public void end() throws IOException {
    suffix.end();
  }
}
