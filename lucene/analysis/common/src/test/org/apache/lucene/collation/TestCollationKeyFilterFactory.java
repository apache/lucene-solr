package org.apache.lucene.collation;

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

import java.io.IOException;
import java.io.StringReader;
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.analysis.util.TokenFilterFactory;

public class TestCollationKeyFilterFactory extends BaseTokenStreamFactoryTestCase {

  /*
   * Turkish has some funny casing.
   * This test shows how you can solve this kind of thing easily with collation.
   * Instead of using LowerCaseFilter, use a turkish collator with primary strength.
   * Then things will sort and match correctly.
   */
  public void testBasicUsage() throws Exception {
    String turkishUpperCase = "I WİLL USE TURKİSH CASING";
    String turkishLowerCase = "ı will use turkish casıng";
    TokenFilterFactory factory = tokenFilterFactory("CollationKey",
        "language", "tr",
        "strength", "primary");
    TokenStream tsUpper = factory.create(
        new MockTokenizer(new StringReader(turkishUpperCase), MockTokenizer.KEYWORD, false));
    TokenStream tsLower = factory.create(
        new MockTokenizer(new StringReader(turkishLowerCase), MockTokenizer.KEYWORD, false));
    assertCollatesToSame(tsUpper, tsLower);
  }
  
  /*
   * Test usage of the decomposition option for unicode normalization.
   */
  public void testNormalization() throws Exception {
    String turkishUpperCase = "I W\u0049\u0307LL USE TURKİSH CASING";
    String turkishLowerCase = "ı will use turkish casıng";
    TokenFilterFactory factory = tokenFilterFactory("CollationKey",
        "language", "tr",
        "strength", "primary",
        "decomposition", "canonical");
    TokenStream tsUpper = factory.create(
        new MockTokenizer(new StringReader(turkishUpperCase), MockTokenizer.KEYWORD, false));
    TokenStream tsLower = factory.create(
        new MockTokenizer(new StringReader(turkishLowerCase), MockTokenizer.KEYWORD, false));
    assertCollatesToSame(tsUpper, tsLower);
  }
  
  /*
   * Test usage of the K decomposition option for unicode normalization.
   * This works even with identical strength.
   */
  public void testFullDecomposition() throws Exception {
    String fullWidth = "Ｔｅｓｔｉｎｇ";
    String halfWidth = "Testing";
    TokenFilterFactory factory = tokenFilterFactory("CollationKey",
        "language", "zh",
        "strength", "identical",
        "decomposition", "full");
    TokenStream tsFull = factory.create(
        new MockTokenizer(new StringReader(fullWidth), MockTokenizer.KEYWORD, false));
    TokenStream tsHalf = factory.create(
        new MockTokenizer(new StringReader(halfWidth), MockTokenizer.KEYWORD, false));
    assertCollatesToSame(tsFull, tsHalf);
  }
  
  /*
   * Test secondary strength, for english case is not significant.
   */
  public void testSecondaryStrength() throws Exception {
    String upperCase = "TESTING";
    String lowerCase = "testing";
    TokenFilterFactory factory = tokenFilterFactory("CollationKey",
        "language", "en",
        "strength", "secondary",
        "decomposition", "no");
    TokenStream tsUpper = factory.create(
        new MockTokenizer(new StringReader(upperCase), MockTokenizer.KEYWORD, false));
    TokenStream tsLower = factory.create(
        new MockTokenizer(new StringReader(lowerCase), MockTokenizer.KEYWORD, false));
    assertCollatesToSame(tsUpper, tsLower);
  }

  /*
   * For german, you might want oe to sort and match with o umlaut.
   * This is not the default, but you can make a customized ruleset to do this.
   *
   * The default is DIN 5007-1, this shows how to tailor a collator to get DIN 5007-2 behavior.
   *  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4423383
   */
  public void testCustomRules() throws Exception {
    RuleBasedCollator baseCollator = (RuleBasedCollator) Collator.getInstance(new Locale("de", "DE"));

    String DIN5007_2_tailorings =
      "& ae , a\u0308 & AE , A\u0308"+
      "& oe , o\u0308 & OE , O\u0308"+
      "& ue , u\u0308 & UE , u\u0308";

    RuleBasedCollator tailoredCollator = new RuleBasedCollator(baseCollator.getRules() + DIN5007_2_tailorings);
    String tailoredRules = tailoredCollator.getRules();
    //
    // at this point, you would save these tailoredRules to a file, 
    // and use the custom parameter.
    //
    String germanUmlaut = "Töne";
    String germanOE = "Toene";
    Map<String,String> args = new HashMap<String,String>();
    args.put("custom", "rules.txt");
    args.put("strength", "primary");
    CollationKeyFilterFactory factory = new CollationKeyFilterFactory(args);
    factory.inform(new StringMockResourceLoader(tailoredRules));
    TokenStream tsUmlaut = factory.create(
        new MockTokenizer(new StringReader(germanUmlaut), MockTokenizer.KEYWORD, false));
    TokenStream tsOE = factory.create(
        new MockTokenizer(new StringReader(germanOE), MockTokenizer.KEYWORD, false));

    assertCollatesToSame(tsUmlaut, tsOE);
  }
  
  private void assertCollatesToSame(TokenStream stream1, TokenStream stream2)
      throws IOException {
    stream1.reset();
    stream2.reset();
    CharTermAttribute term1 = stream1
        .addAttribute(CharTermAttribute.class);
    CharTermAttribute term2 = stream2
        .addAttribute(CharTermAttribute.class);
    assertTrue(stream1.incrementToken());
    assertTrue(stream2.incrementToken());
    assertEquals(term1.toString(), term2.toString());
    assertFalse(stream1.incrementToken());
    assertFalse(stream2.incrementToken());
    stream1.end();
    stream2.end();
    stream1.close();
    stream2.close();
  }
}
