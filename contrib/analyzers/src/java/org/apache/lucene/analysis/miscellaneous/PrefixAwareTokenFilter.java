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

  private CopyableToken previousPrefixToken = new CopyableToken();

  private boolean prefixExhausted;

  public Token next(Token result) throws IOException {

    Token buf = result;

    if (!prefixExhausted) {
      result = prefix.next(result);
      if (result == null) {
        prefixExhausted = true;
      } else {
        previousPrefixToken.copyFrom(result);        
        return result;
      }
    }

    result = suffix.next(buf);
    if (result == null) {
      return null;
    }

    return updateSuffixToken(result, previousPrefixToken);
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


  public static class CopyableToken extends Token {

    private Payload buf = new Payload();

    public void copyFrom(Token source) {
      if (source.termBuffer() != null) {
        setTermBuffer(source.termBuffer(), 0, source.termLength());
      } else {
        setTermText(null);
        setTermLength(0);
      }

      setPositionIncrement(source.getPositionIncrement());
      setFlags(source.getFlags());
      setStartOffset(source.startOffset());
      setEndOffset(source.endOffset());
      setType(source.type());
      if (source.getPayload() == null) {
        setPayload(null);
      } else {
        setPayload(buf);        
        if (buf.getData() == null || buf.getData().length < source.getPayload().length()) {
          buf.setData(new byte[source.getPayload().length()]);
        }
        source.getPayload().copyTo(buf.getData(), 0);
        buf.setData(buf.getData(), 0, source.getPayload().length());
      }
    }
  }
}
