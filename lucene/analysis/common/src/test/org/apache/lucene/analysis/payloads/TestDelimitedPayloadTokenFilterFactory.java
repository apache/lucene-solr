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


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

public class TestDelimitedPayloadTokenFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testEncoder() throws Exception {
    Reader reader = new StringReader("the|0.1 quick|0.1 red|0.1");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("DelimitedPayload", "encoder", "float").create(stream);

    stream.reset();
    while (stream.incrementToken()) {
      PayloadAttribute payAttr = stream.getAttribute(PayloadAttribute.class);
      assertNotNull(payAttr);
      byte[] payData = payAttr.getPayload().bytes;
      assertNotNull(payData);
      float payFloat = PayloadHelper.decodeFloat(payData);
      assertEquals(0.1f, payFloat, 0.0f);
    }
    stream.end();
    stream.close();
  }

  public void testDelim() throws Exception {
    Reader reader = new StringReader("the*0.1 quick*0.1 red*0.1");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("DelimitedPayload",
        "encoder", "float",
        "delimiter", "*").create(stream);
    stream.reset();
    while (stream.incrementToken()) {
      PayloadAttribute payAttr = stream.getAttribute(PayloadAttribute.class);
      assertNotNull(payAttr);
      byte[] payData = payAttr.getPayload().bytes;
      assertNotNull(payData);
      float payFloat = PayloadHelper.decodeFloat(payData);
      assertEquals(0.1f, payFloat, 0.0f);
    }
    stream.end();
    stream.close();
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("DelimitedPayload", 
          "encoder", "float",
          "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}

