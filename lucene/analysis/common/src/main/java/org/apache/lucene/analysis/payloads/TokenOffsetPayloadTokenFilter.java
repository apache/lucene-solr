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
package org.apache.lucene.analysis.payloads;


import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;


/**
 * Adds the {@link OffsetAttribute#startOffset()}
 * and {@link OffsetAttribute#endOffset()}
 * First 4 bytes are the start
 *
 **/
public class TokenOffsetPayloadTokenFilter extends TokenFilter {
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PayloadAttribute payAtt = addAttribute(PayloadAttribute.class);

  public TokenOffsetPayloadTokenFilter(TokenStream input) {
    super(input);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      byte[] data = new byte[8];
      PayloadHelper.encodeInt(offsetAtt.startOffset(), data, 0);
      PayloadHelper.encodeInt(offsetAtt.endOffset(), data, 4);
      BytesRef payload = new BytesRef(data);
      payAtt.setPayload(payload);
      return true;
    } else {
    return false;
    }
  }
}