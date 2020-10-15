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
package org.apache.lucene.analysis.boost;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.BoostAttribute;

public class DelimitedBoostTokenFilterTest extends BaseTokenStreamTestCase {

  public void testBoosts() throws Exception {
    String test = "The quick|0.4 red|0.5 fox|0.2 jumped|0.1 over the lazy|0.8 brown|0.9 dogs|0.9";
    DelimitedBoostTokenFilter filter = new DelimitedBoostTokenFilter
            (whitespaceMockTokenizer(test),
                    DelimitedBoostTokenFilterFactory.DEFAULT_DELIMITER);
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    BoostAttribute boostAtt = filter.addAttribute(BoostAttribute.class);
    filter.reset();
    assertTermEquals("The", filter, termAtt, boostAtt, 1.0f);
    assertTermEquals("quick", filter, termAtt, boostAtt, 0.4f);
    assertTermEquals("red", filter, termAtt, boostAtt, 0.5f);
    assertTermEquals("fox", filter, termAtt, boostAtt, 0.2f);
    assertTermEquals("jumped", filter, termAtt, boostAtt, 0.1f);
    assertTermEquals("over", filter, termAtt, boostAtt, 1.0f);
    assertTermEquals("the", filter, termAtt, boostAtt, 1.0f);
    assertTermEquals("lazy", filter, termAtt, boostAtt, 0.8f);
    assertTermEquals("brown", filter, termAtt, boostAtt, 0.9f);
    assertTermEquals("dogs", filter, termAtt, boostAtt, 0.9f);
    assertFalse(filter.incrementToken());
    filter.end();
    filter.close();
  }

  public void testNext() throws Exception {
    String test = "The quick|0.1 red|0.2 fox|0.3 jumped|0.4 over the lazy|0.5 brown|0.6 dogs|0.6";
    DelimitedBoostTokenFilter filter = new DelimitedBoostTokenFilter
      (whitespaceMockTokenizer(test), 
       DelimitedBoostTokenFilterFactory.DEFAULT_DELIMITER);
    filter.reset();
    assertTermEquals("The", filter, 1.0f);
    assertTermEquals("quick", filter, 0.1f);
    assertTermEquals("red", filter, 0.2f);
    assertTermEquals("fox", filter, 0.3f);
    assertTermEquals("jumped", filter, 0.4f);
    assertTermEquals("over", filter, 1.0f);
    assertTermEquals("the", filter, 1.0f);
    assertTermEquals("lazy", filter, 0.5f);
    assertTermEquals("brown", filter, 0.6f);
    assertTermEquals("dogs", filter, 0.6f);
    assertFalse(filter.incrementToken());
    filter.end();
    filter.close();
  }

  void assertTermEquals(String expected, TokenStream stream, float expectedBoost) throws Exception {
    CharTermAttribute termAtt = stream.getAttribute(CharTermAttribute.class);
    BoostAttribute boostAtt = stream.addAttribute(BoostAttribute.class);
    assertTrue(stream.incrementToken());
    assertEquals(expected, termAtt.toString());
    float actualBoost = boostAtt.getBoost();
    assertTrue(actualBoost + " does not equal: " + expectedBoost, actualBoost == expectedBoost);
  }
  
  void assertTermEquals(String expected, TokenStream stream, CharTermAttribute termAtt, BoostAttribute boostAtt, float expectedBoost) throws Exception {
    assertTrue(stream.incrementToken());
    assertEquals(expected, termAtt.toString());
    float actualBoost = boostAtt.getBoost();
    assertTrue(actualBoost + " does not equal: " + expectedBoost, actualBoost == expectedBoost);
  }
}
