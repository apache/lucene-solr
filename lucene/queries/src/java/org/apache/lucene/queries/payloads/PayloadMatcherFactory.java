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
package org.apache.lucene.queries.payloads;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery.MatchOperation;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery.PayloadType;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Creates a payload matcher object based on a payload type and an operation. PayloadTypes of
 * INT,FLOAT, or STRING are supported. Inequality operations are supported.
 */
public class PayloadMatcherFactory {

  private static final EnumMap<PayloadType, EnumMap<MatchOperation, PayloadMatcher>>
      payloadCheckerOpTypeMap;

  static {
    payloadCheckerOpTypeMap = new EnumMap<>(PayloadType.class);
    // ints
    EnumMap<MatchOperation, PayloadMatcher> intCheckers = new EnumMap<>(MatchOperation.class);
    intCheckers.put(MatchOperation.LT, new LTIntPayloadMatcher());
    intCheckers.put(MatchOperation.LTE, new LTEIntPayloadMatcher());
    intCheckers.put(MatchOperation.GT, new GTIntPayloadMatcher());
    intCheckers.put(MatchOperation.GTE, new GTEIntPayloadMatcher());
    EnumMap<MatchOperation, PayloadMatcher> floatCheckers = new EnumMap<>(MatchOperation.class);
    floatCheckers.put(MatchOperation.LT, new LTFloatPayloadMatcher());
    floatCheckers.put(MatchOperation.LTE, new LTEFloatPayloadMatcher());
    floatCheckers.put(MatchOperation.GT, new GTFloatPayloadMatcher());
    floatCheckers.put(MatchOperation.GTE, new GTEFloatPayloadMatcher());
    // strings
    EnumMap<MatchOperation, PayloadMatcher> stringCheckers = new EnumMap<>(MatchOperation.class);
    stringCheckers.put(MatchOperation.LT, new LTStringPayloadMatcher());
    stringCheckers.put(MatchOperation.LTE, new LTEStringPayloadMatcher());
    stringCheckers.put(MatchOperation.GT, new GTStringPayloadMatcher());
    stringCheckers.put(MatchOperation.GTE, new GTEStringPayloadMatcher());
    // load the matcher maps per payload type
    payloadCheckerOpTypeMap.put(PayloadType.INT, intCheckers);
    payloadCheckerOpTypeMap.put(PayloadType.FLOAT, floatCheckers);
    payloadCheckerOpTypeMap.put(PayloadType.STRING, stringCheckers);
  }

  /**
   * Return a payload matcher for use in the SpanPayloadCheckQuery that will decode the ByteRef from
   * a payload based on the payload type, and apply a matching inequality operations
   * (eq,lt,lte,gt,and gte)
   *
   * @param payloadType the type of the payload to decode, STRING, INT, FLOAT
   * @param op and inequalit operation as the test (example: eq for equals, gt for greater than)
   * @return a payload matcher that decodes the payload and applies the operation inequality test.
   */
  public static PayloadMatcher createMatcherForOpAndType(
      PayloadType payloadType, MatchOperation op) {

    // special optimization, binary/byte comparison
    if (op == null || MatchOperation.EQ.equals(op)) {
      return new EQPayloadMatcher();
    }
    // otherwise, we need to pay attention to the payload type and operation
    EnumMap<MatchOperation, PayloadMatcher> opMap = payloadCheckerOpTypeMap.get(payloadType);
    if (opMap != null) {
      return opMap.get(op);
    } else {
      // Unknown op and payload type gets you an equals operator.
      return new EQPayloadMatcher();
    }
  }

  // Equality is the same for all payload types
  private static class EQPayloadMatcher implements PayloadMatcher {
    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return source.bytesEquals(payload);
    }
  }

  private abstract static class StringPayloadMatcher implements PayloadMatcher {

    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return stringCompare(
          decodeString(payload.bytes, payload.offset, payload.length),
          decodeString(source.bytes, source.offset, source.length));
    }

    private String decodeString(byte[] bytes, int offset, int length) {
      return new String(
          ArrayUtil.copyOfSubArray(bytes, offset, offset + length), StandardCharsets.UTF_8);
    }

    protected abstract boolean stringCompare(String val, String threshold);
  }

  private static class LTStringPayloadMatcher extends StringPayloadMatcher {

    @Override
    protected boolean stringCompare(String val, String thresh) {
      int res = val.compareTo(thresh);
      return (res < 0) ? true : false;
    }
  }

  private static class LTEStringPayloadMatcher extends StringPayloadMatcher {

    @Override
    protected boolean stringCompare(String val, String thresh) {
      int res = val.compareTo(thresh);
      return (res < 1) ? true : false;
    }
  }

  private static class GTStringPayloadMatcher extends StringPayloadMatcher {

    @Override
    protected boolean stringCompare(String val, String thresh) {
      int res = val.compareTo(thresh);
      return (res > 0) ? true : false;
    }
  }

  private static class GTEStringPayloadMatcher extends StringPayloadMatcher {

    @Override
    protected boolean stringCompare(String val, String thresh) {
      int res = val.compareTo(thresh);
      return (res > -1) ? true : false;
    }
  }

  private abstract static class IntPayloadMatcher implements PayloadMatcher {

    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return intCompare(
          decodeInt(payload.bytes, payload.offset), decodeInt(source.bytes, source.offset));
    }

    private int decodeInt(byte[] bytes, int offset) {
      return ((bytes[offset] & 0xFF) << 24)
          | ((bytes[offset + 1] & 0xFF) << 16)
          | ((bytes[offset + 2] & 0xFF) << 8)
          | (bytes[offset + 3] & 0xFF);
    }

    protected abstract boolean intCompare(int val, int threshold);
  }

  private static class LTIntPayloadMatcher extends IntPayloadMatcher {

    @Override
    public boolean intCompare(int val, int thresh) {
      return (val < thresh);
    }
  }

  private static class LTEIntPayloadMatcher extends IntPayloadMatcher {

    @Override
    public boolean intCompare(int val, int thresh) {
      return (val <= thresh);
    }
  }

  private static class GTIntPayloadMatcher extends IntPayloadMatcher {

    @Override
    public boolean intCompare(int val, int thresh) {
      return (val > thresh);
    }
  }

  private static class GTEIntPayloadMatcher extends IntPayloadMatcher {

    @Override
    protected boolean intCompare(int val, int thresh) {
      return (val >= thresh);
    }
  }

  private abstract static class FloatPayloadMatcher implements PayloadMatcher {

    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return floatCompare(
          decodeFloat(payload.bytes, payload.offset), decodeFloat(source.bytes, source.offset));
    }

    private float decodeFloat(byte[] bytes, int offset) {
      return Float.intBitsToFloat(
          ((bytes[offset] & 0xFF) << 24)
              | ((bytes[offset + 1] & 0xFF) << 16)
              | ((bytes[offset + 2] & 0xFF) << 8)
              | (bytes[offset + 3] & 0xFF));
    }

    protected abstract boolean floatCompare(float val, float threshold);
  }

  private static class LTFloatPayloadMatcher extends FloatPayloadMatcher {

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val < thresh);
    }
  }

  private static class LTEFloatPayloadMatcher extends FloatPayloadMatcher {

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val <= thresh);
    }
  }

  private static class GTFloatPayloadMatcher extends FloatPayloadMatcher {

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val > thresh);
    }
  }

  private static class GTEFloatPayloadMatcher extends FloatPayloadMatcher {

    @Override
    protected boolean floatCompare(float val, float thresh) {
      return (val >= thresh);
    }
  }
}