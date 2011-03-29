package org.apache.solr.analysis;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.common.ResourceLoader;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

@Deprecated
public class TestICUCollationKeyFilterFactory extends BaseTokenTestCase {

  /*
   * Turkish has some funny casing.
   * This test shows how you can solve this kind of thing easily with collation.
   * Instead of using LowerCaseFilter, use a turkish collator with primary strength.
   * Then things will sort and match correctly.
   */
  public void testBasicUsage() throws IOException {
    String turkishUpperCase = "I WİLL USE TURKİSH CASING";
    String turkishLowerCase = "ı will use turkish casıng";
    ICUCollationKeyFilterFactory factory = new ICUCollationKeyFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("locale", "tr");
    args.put("strength", "primary");
    factory.init(args);
    factory.inform(new StringMockSolrResourceLoader(""));
    TokenStream tsUpper = factory.create(
        new KeywordTokenizer(new StringReader(turkishUpperCase)));
    TokenStream tsLower = factory.create(
        new KeywordTokenizer(new StringReader(turkishLowerCase)));
    assertCollatesToSame(tsUpper, tsLower);
  }
  
  /*
   * Test usage of the decomposition option for unicode normalization.
   */
  public void testNormalization() throws IOException {
    String turkishUpperCase = "I W\u0049\u0307LL USE TURKİSH CASING";
    String turkishLowerCase = "ı will use turkish casıng";
    ICUCollationKeyFilterFactory factory = new ICUCollationKeyFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("locale", "tr");
    args.put("strength", "primary");
    args.put("decomposition", "canonical");
    factory.init(args);
    factory.inform(new StringMockSolrResourceLoader(""));
    TokenStream tsUpper = factory.create(
        new KeywordTokenizer(new StringReader(turkishUpperCase)));
    TokenStream tsLower = factory.create(
        new KeywordTokenizer(new StringReader(turkishLowerCase)));
    assertCollatesToSame(tsUpper, tsLower);
  }
  
  /*
   * Test secondary strength, for english case is not significant.
   */
  public void testSecondaryStrength() throws IOException {
    String upperCase = "TESTING";
    String lowerCase = "testing";
    ICUCollationKeyFilterFactory factory = new ICUCollationKeyFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("locale", "en");
    args.put("strength", "secondary");
    args.put("decomposition", "no");
    factory.init(args);
    factory.inform(new StringMockSolrResourceLoader(""));
    TokenStream tsUpper = factory.create(
        new KeywordTokenizer(new StringReader(upperCase)));
    TokenStream tsLower = factory.create(
        new KeywordTokenizer(new StringReader(lowerCase)));
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
    RuleBasedCollator baseCollator = (RuleBasedCollator) Collator.getInstance(new ULocale("de_DE"));

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
    ICUCollationKeyFilterFactory factory = new ICUCollationKeyFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("custom", "rules.txt");
    args.put("strength", "primary");
    factory.init(args);
    factory.inform(new StringMockSolrResourceLoader(tailoredRules));
    TokenStream tsUmlaut = factory.create(
        new KeywordTokenizer(new StringReader(germanUmlaut)));
    TokenStream tsOE = factory.create(
        new KeywordTokenizer(new StringReader(germanOE)));

    assertCollatesToSame(tsUmlaut, tsOE);
  }
  
  private class StringMockSolrResourceLoader implements ResourceLoader {
    String text;

    StringMockSolrResourceLoader(String text) {
      this.text = text;
    }

    public List<String> getLines(String resource) throws IOException {
      return null;
    }

    public Object newInstance(String cname, String... subpackages) {
      return null;
    }

    public InputStream openResource(String resource) throws IOException {
      return new ByteArrayInputStream(text.getBytes("UTF-8"));
    }
  }
  
  private void assertCollatesToSame(TokenStream stream1, TokenStream stream2)
      throws IOException {
    CharTermAttribute term1 = stream1
        .addAttribute(CharTermAttribute.class);
    CharTermAttribute term2 = stream2
        .addAttribute(CharTermAttribute.class);
    assertTrue(stream1.incrementToken());
    assertTrue(stream2.incrementToken());
    assertEquals(term1.toString(), term2.toString());
    assertFalse(stream1.incrementToken());
    assertFalse(stream2.incrementToken());
  }
}
