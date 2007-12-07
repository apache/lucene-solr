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


import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.Payload;

import java.io.IOException;


/**
 * Assigns a payload to a token based on the {@link org.apache.lucene.analysis.Token#type()}
 *
 **/
public class NumericPayloadTokenFilter extends TokenFilter {

  private String typeMatch;
  private Payload thePayload;

  public NumericPayloadTokenFilter(TokenStream input, float payload, String typeMatch) {
    super(input);
    //Need to encode the payload
    thePayload = new Payload(encodePayload(payload));
    this.typeMatch = typeMatch;
  }

  public static byte[] encodePayload(float payload) {
    byte[] result = new byte[4];
    int tmp = Float.floatToIntBits(payload);
    result[0] = (byte)(tmp >> 24);
    result[1] = (byte)(tmp >> 16);
    result[2] = (byte)(tmp >>  8);
    result[3] = (byte) tmp;

    return result;
  }

  /**
   * @see #decodePayload(byte[], int)
   * @see #encodePayload(float)
   */
  public static float decodePayload(byte [] bytes){
    return decodePayload(bytes, 0);
  }

  /**
   * Decode the payload that was encoded using {@link #encodePayload(float)}.
   * NOTE: the length of the array must be at least offset + 4 long.
   * @param bytes The bytes to decode
   * @param offset The offset into the array.
   * @return The float that was encoded
   *
   * @see #encodePayload(float) 
   */
  public static final float decodePayload(byte [] bytes, int offset){
    int tmp = ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16)
         | ((bytes[offset + 2] & 0xFF) <<  8) |  (bytes[offset + 3] & 0xFF);
    return Float.intBitsToFloat(tmp);
  }

  public Token next(Token result) throws IOException {
    result = input.next(result);
    if (result != null && result.type().equals(typeMatch)){
      result.setPayload(thePayload);
    }
    return result;
  }
}
