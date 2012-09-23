package org.apache.lucene.analysis.payloads;
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


/**
 * Utility methods for encoding payloads.
 *
 **/
public class PayloadHelper {

  public static byte[] encodeFloat(float payload) {
    return encodeFloat(payload, new byte[4], 0);
  }

  public static byte[] encodeFloat(float payload, byte[] data, int offset){
    return encodeInt(Float.floatToIntBits(payload), data, offset);
  }

  public static byte[] encodeInt(int payload){
    return encodeInt(payload, new byte[4], 0);
  }

  public static byte[] encodeInt(int payload, byte[] data, int offset){
    data[offset] = (byte)(payload >> 24);
    data[offset + 1] = (byte)(payload >> 16);
    data[offset + 2] = (byte)(payload >>  8);
    data[offset + 3] = (byte) payload;
    return data;
  }

  /**
   * @see #decodeFloat(byte[], int)
   * @see #encodeFloat(float)
   * @return the decoded float
   */
  public static float decodeFloat(byte [] bytes){
    return decodeFloat(bytes, 0);
  }

  /**
   * Decode the payload that was encoded using {@link #encodeFloat(float)}.
   * NOTE: the length of the array must be at least offset + 4 long.
   * @param bytes The bytes to decode
   * @param offset The offset into the array.
   * @return The float that was encoded
   *
   * @see #encodeFloat(float)
   */
  public static final float decodeFloat(byte [] bytes, int offset){

    return Float.intBitsToFloat(decodeInt(bytes, offset));
  }

  public static final int decodeInt(byte [] bytes, int offset){
    return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16)
         | ((bytes[offset + 2] & 0xFF) <<  8) |  (bytes[offset + 3] & 0xFF);
  }
}
