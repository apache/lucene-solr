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
import java.util.Arrays;
import java.util.HashMap;

import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery.PayloadType;
import org.apache.lucene.util.BytesRef;

/**
 * Creates a payload matcher object based on a payload type and an operation.
 * PayloadTypes of INT,FLOAT, or STRING are supported.  Inequality operations are supported.
 */
public class PayloadMatcherFactory {

  private static final HashMap<PayloadType, HashMap<String, PayloadMatcher>> payloadCheckerOpTypeMap;
  static {
    payloadCheckerOpTypeMap= new HashMap<PayloadType, HashMap<String, PayloadMatcher>>();
    // ints
    HashMap<String, PayloadMatcher> intCheckers = new HashMap<String, PayloadMatcher>();
    intCheckers.put("lt", new LTIntPayloadMatcher());
    intCheckers.put("lte", new LTEIntPayloadMatcher());
    intCheckers.put("gt", new GTIntPayloadMatcher());
    intCheckers.put("gte", new GTEIntPayloadMatcher());
    HashMap<String, PayloadMatcher> floatCheckers = new HashMap<String, PayloadMatcher>();
    floatCheckers.put("lt", new LTFloatPayloadMatcher());
    floatCheckers.put("lte", new LTEFloatPayloadMatcher());
    floatCheckers.put("gt", new GTFloatPayloadMatcher());
    floatCheckers.put("gte", new GTEFloatPayloadMatcher());
    // strings
    HashMap<String, PayloadMatcher> stringCheckers = new HashMap<String, PayloadMatcher>();
    stringCheckers.put("lt", new LTStringPayloadMatcher());
    stringCheckers.put("lte", new LTEStringPayloadMatcher());
    stringCheckers.put("gt", new GTStringPayloadMatcher());
    stringCheckers.put("gte", new GTEStringPayloadMatcher());
    // load the matcher maps per payload type
    payloadCheckerOpTypeMap.put(PayloadType.INT, intCheckers);
    payloadCheckerOpTypeMap.put(PayloadType.FLOAT, floatCheckers);
    payloadCheckerOpTypeMap.put(PayloadType.STRING, stringCheckers);
  }
  
  /**
   * Return a payload matcher for use in the SpanPayloadCheckQuery that will decode the ByteRef from 
   * a payload based on the payload type, and apply a matching inequality operations (eq,lt,lte,gt,and gte)
   * 
   * @param payloadType the type of the payload to decode, STRING, INT, FLOAT
   * @param op and inequalit operation as the test (example: eq for equals, gt for greater than)
   * @return a payload matcher that decodes the payload and applies the operation inequality test.
   * 
   */
  public static PayloadMatcher createMatcherForOpAndType(PayloadType payloadType, String op) {
    // special optimization, binary/byte comparison
    if (op == null || "eq".contentEquals(op)) {
      return new EQPayloadMatcher();
    }
    // otherwise, we need to pay attention to the payload type and operation
    HashMap<String, PayloadMatcher> opMap = payloadCheckerOpTypeMap.get(payloadType);
    if (opMap != null) {
      return opMap.get(op);
    } else {
      // Unknown op and payload type gets you an equals operator.
      return new EQPayloadMatcher();
    }
  }
  
  private static class EQPayloadMatcher implements PayloadMatcher {
    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return source.bytesEquals(payload);
    }
  }
  
  private static abstract class StringPayloadMatcher implements PayloadMatcher {
    
    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return stringCompare(decodeString(payload.bytes, payload.offset, payload.length), decodeString(source.bytes, source.offset, source.length));
    }
    
    private String decodeString(byte[] bytes, int offset, int length) {
      // TODO: consider just the raw byte array instead of a decoded String
      // TODO: Encoding?  Is this correct?  What about null pointers? Do we need to deal with the offsets here?
      // TODO: I think we need the byte array from the offset and beyond only?
      // System.err.println("OFFSET : " + offset + " " + new String(bytes, StandardCharsets.UTF_8));
      // System.err.println("Decoded : " + decoded);
      return new String(Arrays.copyOfRange(bytes, offset, offset+length), StandardCharsets.UTF_8);    
    }
    
    protected abstract boolean stringCompare(String val, String threshold);
    
  }
  
  private static class LTStringPayloadMatcher extends StringPayloadMatcher {

    @Override
    protected boolean stringCompare(String val, String thresh) {
      // TODO: null check?  also is this logic backwards?
      // System.err.println("Compare: " + val + " to " + thresh);
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

  private static abstract class IntPayloadMatcher implements PayloadMatcher {

    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return intCompare(decodeInt(payload.bytes, payload.offset), decodeInt(source.bytes, source.offset));
    }
    
    private int decodeInt(byte[] bytes, int offset) {
      return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) |
             ((bytes[offset + 2] & 0xFF) <<  8) | (bytes[offset + 3] & 0xFF);
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
  
  private static abstract class FloatPayloadMatcher implements PayloadMatcher {

    @Override
    public boolean comparePayload(BytesRef source, BytesRef payload) {
      return floatCompare(decodeFloat(payload.bytes, payload.offset), decodeFloat(source.bytes, source.offset));
    }
    
    private float decodeFloat(byte[] bytes, int offset) {
      return Float.intBitsToFloat(((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | 
                                  ((bytes[offset + 2] & 0xFF) <<  8) | (bytes[offset + 3] & 0xFF));
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
