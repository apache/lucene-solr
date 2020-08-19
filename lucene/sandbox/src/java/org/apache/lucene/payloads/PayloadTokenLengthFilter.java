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

package org.apache.lucene.payloads;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.BytesRef;

/**
 * Encodes a token's position length into its associated payload
 *
 * @see PayloadLengthTermIntervalsSource
 */
public final class PayloadTokenLengthFilter extends TokenFilter {

  private final PayloadAttribute payloadAttribute = addAttribute(PayloadAttribute.class);
  private final PositionLengthAttribute lengthAttribute = addAttribute(PositionLengthAttribute.class);

  private final BytesRef encodedLength = new BytesRef(4);

  /**
   * Create a PaylaodTokenLengthFilter
   */
  public PayloadTokenLengthFilter(TokenStream input) {
    super(input);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken() == false) {
      return false;
    }

    if (lengthAttribute.getPositionLength() > 1) {
      encodeLength(lengthAttribute.getPositionLength());
      payloadAttribute.setPayload(encodedLength);
    }

    return true;
  }

  private void encodeLength(int positionLength) {
    int numBitsRequired = 32 - Integer.numberOfLeadingZeros(positionLength);
    int numBytesRequired = (numBitsRequired + 7) / 8;
    encodedLength.length = numBytesRequired;
    for (int index = numBytesRequired - 1; index >= 0; index--) {
      encodedLength.bytes[index] = (byte) positionLength;
      positionLength >>>= 8;
    }
    assert positionLength == 0;
  }
}
