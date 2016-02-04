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
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.IOException;

public class TypeAsPayloadTokenFilterTest extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    String test = "The quick red fox jumped over the lazy brown dogs";

    TypeAsPayloadTokenFilter nptf = new TypeAsPayloadTokenFilter(new WordTokenFilter(whitespaceMockTokenizer(test)));
    int count = 0;
    CharTermAttribute termAtt = nptf.getAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = nptf.getAttribute(TypeAttribute.class);
    PayloadAttribute payloadAtt = nptf.getAttribute(PayloadAttribute.class);
    nptf.reset();
    while (nptf.incrementToken()) {
      assertTrue(typeAtt.type() + " is not null and it should be", typeAtt.type().equals(String.valueOf(Character.toUpperCase(termAtt.buffer()[0]))));
      assertTrue("nextToken.getPayload() is null and it shouldn't be", payloadAtt.getPayload() != null);
      String type = payloadAtt.getPayload().utf8ToString();
      assertTrue(type + " is not equal to " + typeAtt.type(), type.equals(typeAtt.type()));
      count++;
    }

    assertTrue(count + " does not equal: " + 10, count == 10);
  }

  private final class WordTokenFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    
    private WordTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        typeAtt.setType(String.valueOf(Character.toUpperCase(termAtt.buffer()[0])));
        return true;
      } else {
        return false;
      }
    }
  }

}