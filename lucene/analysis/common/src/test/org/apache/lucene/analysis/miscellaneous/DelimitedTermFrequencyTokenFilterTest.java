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

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;

public class DelimitedTermFrequencyTokenFilterTest extends BaseTokenStreamTestCase {

  public void testTermFrequency() throws Exception {
    String test = "The quick|40 red|4 fox|06 jumped|1 over the lazy|2 brown|123 dogs|1024";
    DelimitedTermFrequencyTokenFilter filter =
        new DelimitedTermFrequencyTokenFilter(whitespaceMockTokenizer(test));
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    TermFrequencyAttribute tfAtt = filter.getAttribute(TermFrequencyAttribute.class);
    filter.reset();
    assertTermEquals("The", filter, termAtt, tfAtt, 1);
    assertTermEquals("quick", filter, termAtt, tfAtt, 40);
    assertTermEquals("red", filter, termAtt, tfAtt, 4);
    assertTermEquals("fox", filter, termAtt, tfAtt, 6);
    assertTermEquals("jumped", filter, termAtt, tfAtt, 1);
    assertTermEquals("over", filter, termAtt, tfAtt, 1);
    assertTermEquals("the", filter, termAtt, tfAtt, 1);
    assertTermEquals("lazy", filter, termAtt, tfAtt, 2);
    assertTermEquals("brown", filter, termAtt, tfAtt, 123);
    assertTermEquals("dogs", filter, termAtt, tfAtt, 1024);
    assertFalse(filter.incrementToken());
    filter.end();
    filter.close();
  }

  public void testInvalidNegativeTf() throws Exception {
    String test = "foo bar|-20";
    DelimitedTermFrequencyTokenFilter filter =
        new DelimitedTermFrequencyTokenFilter(whitespaceMockTokenizer(test));
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    TermFrequencyAttribute tfAtt = filter.getAttribute(TermFrequencyAttribute.class);
    filter.reset();
    assertTermEquals("foo", filter, termAtt, tfAtt, 1);
    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, filter::incrementToken);
    assertEquals("Term frequency must be 1 or greater; got -20", iae.getMessage());
  }

  public void testInvalidFloatTf() throws Exception {
    String test = "foo bar|1.2";
    DelimitedTermFrequencyTokenFilter filter =
        new DelimitedTermFrequencyTokenFilter(whitespaceMockTokenizer(test));
    CharTermAttribute termAtt = filter.getAttribute(CharTermAttribute.class);
    TermFrequencyAttribute tfAtt = filter.getAttribute(TermFrequencyAttribute.class);
    filter.reset();
    assertTermEquals("foo", filter, termAtt, tfAtt, 1);
    expectThrows(NumberFormatException.class, filter::incrementToken);
  }

  void assertTermEquals(String expected, TokenStream stream, CharTermAttribute termAtt, TermFrequencyAttribute tfAtt, int expectedTf) throws Exception {
    assertTrue(stream.incrementToken());
    assertEquals(expected, termAtt.toString());
    assertEquals(expectedTf, tfAtt.getTermFrequency());
  }
}
