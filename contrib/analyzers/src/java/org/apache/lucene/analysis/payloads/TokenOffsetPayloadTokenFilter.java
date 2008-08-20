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


import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Payload;

import java.io.IOException;


/**
 * Adds the {@link org.apache.lucene.analysis.Token#setStartOffset(int)}
 * and {@link org.apache.lucene.analysis.Token#setEndOffset(int)}
 * First 4 bytes are the start
 *
 **/
public class TokenOffsetPayloadTokenFilter extends TokenFilter {


  public TokenOffsetPayloadTokenFilter(TokenStream input) {
    super(input);
  }

  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    Token nextToken = input.next(reusableToken);
    if (nextToken != null){
      byte[] data = new byte[8];
      PayloadHelper.encodeInt(nextToken.startOffset(), data, 0);
      PayloadHelper.encodeInt(nextToken.endOffset(), data, 4);
      Payload payload = new Payload(data);
      nextToken.setPayload(payload);
    }
    return nextToken;
  }
}