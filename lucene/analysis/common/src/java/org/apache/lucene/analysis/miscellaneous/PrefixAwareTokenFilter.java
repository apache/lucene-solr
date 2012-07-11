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
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;


/**
 * Joins two token streams and leaves the last token of the first stream available
 * to be used when updating the token values in the second stream based on that token.
 *
 * The default implementation adds last prefix token end offset to the suffix token start and end offsets.
 * <p/>
 * <b>NOTE:</b> This filter might not behave correctly if used with custom Attributes, i.e. Attributes other than
 * the ones located in org.apache.lucene.analysis.tokenattributes. 
 */
public class PrefixAwareTokenFilter extends TokenStream {

  private TokenStream prefix;
  private TokenStream suffix;
  
  private CharTermAttribute termAtt;
  private PositionIncrementAttribute posIncrAtt;
  private PayloadAttribute payloadAtt;
  private OffsetAttribute offsetAtt;
  private TypeAttribute typeAtt;
  private FlagsAttribute flagsAtt;

  private CharTermAttribute p_termAtt;
  private PositionIncrementAttribute p_posIncrAtt;
  private PayloadAttribute p_payloadAtt;
  private OffsetAttribute p_offsetAtt;
  private TypeAttribute p_typeAtt;
  private FlagsAttribute p_flagsAtt;

  public PrefixAwareTokenFilter(TokenStream prefix, TokenStream suffix) {
    super(suffix);
    this.suffix = suffix;
    this.prefix = prefix;
    prefixExhausted = false;
    
    termAtt = addAttribute(CharTermAttribute.class);
    posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    payloadAtt = addAttribute(PayloadAttribute.class);
    offsetAtt = addAttribute(OffsetAttribute.class);
    typeAtt = addAttribute(TypeAttribute.class);
    flagsAtt = addAttribute(FlagsAttribute.class);

    p_termAtt = prefix.addAttribute(CharTermAttribute.class);
    p_posIncrAtt = prefix.addAttribute(PositionIncrementAttribute.class);
    p_payloadAtt = prefix.addAttribute(PayloadAttribute.class);
    p_offsetAtt = prefix.addAttribute(OffsetAttribute.class);
    p_typeAtt = prefix.addAttribute(TypeAttribute.class);
    p_flagsAtt = prefix.addAttribute(FlagsAttribute.class);
  }

  private Token previousPrefixToken = new Token();
  private Token reusableToken = new Token();

  private boolean prefixExhausted;

  @Override
  public final boolean incrementToken() throws IOException {
    if (!prefixExhausted) {
      Token nextToken = getNextPrefixInputToken(reusableToken);
      if (nextToken == null) {
        prefixExhausted = true;
      } else {
        previousPrefixToken.reinit(nextToken);
        // Make it a deep copy
        BytesRef p = previousPrefixToken.getPayload();
        if (p != null) {
          previousPrefixToken.setPayload(p.clone());
        }
        setCurrentToken(nextToken);
        return true;
      }
    }

    Token nextToken = getNextSuffixInputToken(reusableToken);
    if (nextToken == null) {
      return false;
    }

    nextToken = updateSuffixToken(nextToken, previousPrefixToken);
    setCurrentToken(nextToken);
    return true;
  }
  
  private void setCurrentToken(Token token) {
    if (token == null) return;
    clearAttributes();
    termAtt.copyBuffer(token.buffer(), 0, token.length());
    posIncrAtt.setPositionIncrement(token.getPositionIncrement());
    flagsAtt.setFlags(token.getFlags());
    offsetAtt.setOffset(token.startOffset(), token.endOffset());
    typeAtt.setType(token.type());
    payloadAtt.setPayload(token.getPayload());
  }
  
  private Token getNextPrefixInputToken(Token token) throws IOException {
    if (!prefix.incrementToken()) return null;
    token.copyBuffer(p_termAtt.buffer(), 0, p_termAtt.length());
    token.setPositionIncrement(p_posIncrAtt.getPositionIncrement());
    token.setFlags(p_flagsAtt.getFlags());
    token.setOffset(p_offsetAtt.startOffset(), p_offsetAtt.endOffset());
    token.setType(p_typeAtt.type());
    token.setPayload(p_payloadAtt.getPayload());
    return token;
  }

  private Token getNextSuffixInputToken(Token token) throws IOException {
    if (!suffix.incrementToken()) return null;
    token.copyBuffer(termAtt.buffer(), 0, termAtt.length());
    token.setPositionIncrement(posIncrAtt.getPositionIncrement());
    token.setFlags(flagsAtt.getFlags());
    token.setOffset(offsetAtt.startOffset(), offsetAtt.endOffset());
    token.setType(typeAtt.type());
    token.setPayload(payloadAtt.getPayload());
    return token;
  }

  /**
   * The default implementation adds last prefix token end offset to the suffix token start and end offsets.
   *
   * @param suffixToken a token from the suffix stream
   * @param lastPrefixToken the last token from the prefix stream
   * @return consumer token
   */
  public Token updateSuffixToken(Token suffixToken, Token lastPrefixToken) {
    suffixToken.setOffset(lastPrefixToken.endOffset() + suffixToken.startOffset(),
                          lastPrefixToken.endOffset() + suffixToken.endOffset());
    return suffixToken;
  }

  @Override
  public void end() throws IOException {
    prefix.end();
    suffix.end();
  }

  @Override
  public void close() throws IOException {
    prefix.close();
    suffix.close();
  }

  @Override
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
