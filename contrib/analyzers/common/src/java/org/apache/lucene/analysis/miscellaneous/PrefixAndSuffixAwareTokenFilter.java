package org.apache.lucene.analysis.miscellaneous;

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
      public Token updateSuffixToken(Token suffixToken, Token lastInputToken) {
        return PrefixAndSuffixAwareTokenFilter.this.updateInputToken(suffixToken, lastInputToken);
      }
    };
    this.suffix = new PrefixAwareTokenFilter(prefix, suffix) {
      public Token updateSuffixToken(Token suffixToken, Token lastInputToken) {
        return PrefixAndSuffixAwareTokenFilter.this.updateSuffixToken(suffixToken, lastInputToken);
      }
    };
  }

  public Token updateInputToken(Token inputToken, Token lastPrefixToken) {
    inputToken.setStartOffset(lastPrefixToken.endOffset() + inputToken.startOffset());
    inputToken.setEndOffset(lastPrefixToken.endOffset() + inputToken.endOffset());
    return inputToken;
  }

  public Token updateSuffixToken(Token suffixToken, Token lastInputToken) {
    suffixToken.setStartOffset(lastInputToken.endOffset() + suffixToken.startOffset());
    suffixToken.setEndOffset(lastInputToken.endOffset() + suffixToken.endOffset());
    return suffixToken;
  }


  public final boolean incrementToken() throws IOException {
    return suffix.incrementToken();
  }
  
  /** @deprecated Will be removed in Lucene 3.0. This method is final, as it should
   * not be overridden. Delegates to the backwards compatibility layer. */
  public final Token next(final Token reusableToken) throws java.io.IOException {
    return super.next(reusableToken);
  }

  /** @deprecated Will be removed in Lucene 3.0. This method is final, as it should
   * not be overridden. Delegates to the backwards compatibility layer. */
  public final Token next() throws java.io.IOException {
    return super.next();
  }

  public void reset() throws IOException {
    suffix.reset();
  }


  public void close() throws IOException {
    suffix.close();
  }
}
