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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;

public class DelimitedPayloadTokenFilterTest extends BaseTokenStreamTestCase {

  public void testPayloads() throws Exception {
    String test = "The quick|JJ red|JJ fox|NN jumped|VB over the lazy|JJ brown|JJ dogs|NN";
    DelimitedPayloadTokenFilter filter = new DelimitedPayloadTokenFilter
      (whitespaceMockTokenizer(test), 
       DelimitedPayloadTokenFilter.DEFAULT_DELIMITER, new IdentityEncoder());
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    PayloadAttribute payAtt = filter.getAttribute(PayloadAttribute.class);
    filter.reset();
    assertTermEquals("The", filter, termAtt, payAtt, null);
    assertTermEquals("quick", filter, termAtt, payAtt, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("red", filter, termAtt, payAtt, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("fox", filter, termAtt, payAtt, "NN".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("jumped", filter, termAtt, payAtt, "VB".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("over", filter, termAtt, payAtt, null);
    assertTermEquals("the", filter, termAtt, payAtt, null);
    assertTermEquals("lazy", filter, termAtt, payAtt, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("brown", filter, termAtt, payAtt, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("dogs", filter, termAtt, payAtt, "NN".getBytes(StandardCharsets.UTF_8));
    assertFalse(filter.incrementToken());
    filter.end();
    filter.close();
  }

  public void testNext() throws Exception {

    String test = "The quick|JJ red|JJ fox|NN jumped|VB over the lazy|JJ brown|JJ dogs|NN";
    DelimitedPayloadTokenFilter filter = new DelimitedPayloadTokenFilter
      (whitespaceMockTokenizer(test), 
       DelimitedPayloadTokenFilter.DEFAULT_DELIMITER, new IdentityEncoder());
    filter.reset();
    assertTermEquals("The", filter, null);
    assertTermEquals("quick", filter, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("red", filter, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("fox", filter, "NN".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("jumped", filter, "VB".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("over", filter, null);
    assertTermEquals("the", filter, null);
    assertTermEquals("lazy", filter, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("brown", filter, "JJ".getBytes(StandardCharsets.UTF_8));
    assertTermEquals("dogs", filter, "NN".getBytes(StandardCharsets.UTF_8));
    assertFalse(filter.incrementToken());
    filter.end();
    filter.close();
  }


  public void testFloatEncoding() throws Exception {
    String test = "The quick|1.0 red|2.0 fox|3.5 jumped|0.5 over the lazy|5 brown|99.3 dogs|83.7";
    DelimitedPayloadTokenFilter filter = new DelimitedPayloadTokenFilter(whitespaceMockTokenizer(test), '|', new FloatEncoder());
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    PayloadAttribute payAtt = filter.getAttribute(PayloadAttribute.class);
    filter.reset();
    assertTermEquals("The", filter, termAtt, payAtt, null);
    assertTermEquals("quick", filter, termAtt, payAtt, PayloadHelper.encodeFloat(1.0f));
    assertTermEquals("red", filter, termAtt, payAtt, PayloadHelper.encodeFloat(2.0f));
    assertTermEquals("fox", filter, termAtt, payAtt, PayloadHelper.encodeFloat(3.5f));
    assertTermEquals("jumped", filter, termAtt, payAtt, PayloadHelper.encodeFloat(0.5f));
    assertTermEquals("over", filter, termAtt, payAtt, null);
    assertTermEquals("the", filter, termAtt, payAtt, null);
    assertTermEquals("lazy", filter, termAtt, payAtt, PayloadHelper.encodeFloat(5.0f));
    assertTermEquals("brown", filter, termAtt, payAtt, PayloadHelper.encodeFloat(99.3f));
    assertTermEquals("dogs", filter, termAtt, payAtt, PayloadHelper.encodeFloat(83.7f));
    assertFalse(filter.incrementToken());
    filter.end();
    filter.close();
  }

  public void testIntEncoding() throws Exception {
    String test = "The quick|1 red|2 fox|3 jumped over the lazy|5 brown|99 dogs|83";
    DelimitedPayloadTokenFilter filter = new DelimitedPayloadTokenFilter(whitespaceMockTokenizer(test), '|', new IntegerEncoder());
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    PayloadAttribute payAtt = filter.getAttribute(PayloadAttribute.class);
    filter.reset();
    assertTermEquals("The", filter, termAtt, payAtt, null);
    assertTermEquals("quick", filter, termAtt, payAtt, PayloadHelper.encodeInt(1));
    assertTermEquals("red", filter, termAtt, payAtt, PayloadHelper.encodeInt(2));
    assertTermEquals("fox", filter, termAtt, payAtt, PayloadHelper.encodeInt(3));
    assertTermEquals("jumped", filter, termAtt, payAtt, null);
    assertTermEquals("over", filter, termAtt, payAtt, null);
    assertTermEquals("the", filter, termAtt, payAtt, null);
    assertTermEquals("lazy", filter, termAtt, payAtt, PayloadHelper.encodeInt(5));
    assertTermEquals("brown", filter, termAtt, payAtt, PayloadHelper.encodeInt(99));
    assertTermEquals("dogs", filter, termAtt, payAtt, PayloadHelper.encodeInt(83));
    assertFalse(filter.incrementToken());
    filter.end();
    filter.close();
  }

  void assertTermEquals(String expected, TokenStream stream, byte[] expectPay) throws Exception {
    CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    PayloadAttribute payloadAtt = stream.getAttribute(PayloadAttribute.class);
    assertTrue(stream.incrementToken());
    assertEquals(expected, termAtt.toString());
    BytesRef payload = payloadAtt.getPayload();
    if (payload != null) {
      assertTrue(payload.length + " does not equal: " + expectPay.length, payload.length == expectPay.length);
      for (int i = 0; i < expectPay.length; i++) {
        assertTrue(expectPay[i] + " does not equal: " + payload.bytes[i + payload.offset], expectPay[i] == payload.bytes[i + payload.offset]);

      }
    } else {
      assertTrue("expectPay is not null and it should be", expectPay == null);
    }
  }


  void assertTermEquals(String expected, TokenStream stream, CharTermAttribute termAtt, PayloadAttribute payAtt, byte[] expectPay) throws Exception {
    assertTrue(stream.incrementToken());
    assertEquals(expected, termAtt.toString());
    BytesRef payload = payAtt.getPayload();
    if (payload != null) {
      assertTrue(payload.length + " does not equal: " + expectPay.length, payload.length == expectPay.length);
      for (int i = 0; i < expectPay.length; i++) {
        assertTrue(expectPay[i] + " does not equal: " + payload.bytes[i + payload.offset], expectPay[i] == payload.bytes[i + payload.offset]);

      }
    } else {
      assertTrue("expectPay is not null and it should be", expectPay == null);
    }
  }
}
