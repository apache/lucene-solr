package org.apache.lucene.analysis.payloads;
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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.index.Payload;


/**
 * Adds the {@link org.apache.lucene.analysis.Token#setStartOffset(int)}
 * and {@link org.apache.lucene.analysis.Token#setEndOffset(int)}
 * First 4 bytes are the start
 *
 **/
public class TokenOffsetPayloadTokenFilter extends TokenFilter {
  protected OffsetAttribute offsetAtt;
  protected PayloadAttribute payAtt;

  public TokenOffsetPayloadTokenFilter(TokenStream input) {
    super(input);
    offsetAtt = (OffsetAttribute) addAttribute(OffsetAttribute.class);
    payAtt = (PayloadAttribute) addAttribute(PayloadAttribute.class);
  }

  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      byte[] data = new byte[8];
      PayloadHelper.encodeInt(offsetAtt.startOffset(), data, 0);
      PayloadHelper.encodeInt(offsetAtt.endOffset(), data, 4);
      Payload payload = new Payload(data);
      payAtt.setPayload(payload);
      return true;
    } else {
    return false;
    }
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
}