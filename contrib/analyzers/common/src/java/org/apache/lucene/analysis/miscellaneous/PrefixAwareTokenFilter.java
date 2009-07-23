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
import org.apache.lucene.index.Payload;

import java.io.IOException;


/**
 * Joins two token streams and leaves the last token of the first stream available
 * to be used when updating the token values in the second stream based on that token.
 *
 * The default implementation adds last prefix token end offset to the suffix token start and end offsets.
 */
public class PrefixAwareTokenFilter extends TokenStream {

  private TokenStream prefix;
  private TokenStream suffix;

  public PrefixAwareTokenFilter(TokenStream prefix, TokenStream suffix) {
    this.suffix = suffix;
    this.prefix = prefix;
    prefixExhausted = false;
  }

  private Token previousPrefixToken = new Token();

  private boolean prefixExhausted;

  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;

    if (!prefixExhausted) {
      Token nextToken = prefix.next(reusableToken);
      if (nextToken == null) {
        prefixExhausted = true;
      } else {
        previousPrefixToken.reinit(nextToken);
        // Make it a deep copy
        Payload p = previousPrefixToken.getPayload();
        if (p != null) {
          previousPrefixToken.setPayload((Payload) p.clone());
        }
        return nextToken;
      }
    }

    Token nextToken = suffix.next(reusableToken);
    if (nextToken == null) {
      return null;
    }

    return updateSuffixToken(nextToken, previousPrefixToken);
  }

  /**
   * The default implementation adds last prefix token end offset to the suffix token start and end offsets.
   *
   * @param suffixToken a token from the suffix stream
   * @param lastPrefixToken the last token from the prefix stream
   * @return consumer token
   */
  public Token updateSuffixToken(Token suffixToken, Token lastPrefixToken) {
    suffixToken.setStartOffset(lastPrefixToken.endOffset() + suffixToken.startOffset());
    suffixToken.setEndOffset(lastPrefixToken.endOffset() + suffixToken.endOffset());
    return suffixToken;
  }

  public void close() throws IOException {
    prefix.close();
    suffix.close();
  }

  public void reset() throws IOException {
    super.reset();
    if (prefix != null) {
      prefixExhausted = false;
      prefix.reset();
    }
    if (suffix != null) {
      suffix.reset();
    }


  }

  public TokenStream getPrefix() {
    return prefix;
  }

  public void setPrefix(TokenStream prefix) {
    this.prefix = prefix;
  }

  public TokenStream getSuffix() {
    return suffix;
  }

  public void setSuffix(TokenStream suffix) {
    this.suffix = suffix;
  }
}
