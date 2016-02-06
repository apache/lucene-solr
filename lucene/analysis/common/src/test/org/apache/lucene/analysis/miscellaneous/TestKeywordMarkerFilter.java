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

import java.io.IOException;
import java.io.StringReader;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.junit.Test;


/**
 * Testcase for {@link KeywordMarkerFilter}
 */
public class TestKeywordMarkerFilter extends BaseTokenStreamTestCase {

  @Test
  public void testSetFilterIncrementToken() throws IOException {
    CharArraySet set = new CharArraySet( 5, true);
    set.add("lucenefox");
    String[] output = new String[] { "the", "quick", "brown", "LuceneFox",
        "jumps" };
    assertTokenStreamContents(new LowerCaseFilterMock(
        new SetKeywordMarkerFilter(whitespaceMockTokenizer("The quIck browN LuceneFox Jumps"), set)), output);
    CharArraySet mixedCaseSet = new CharArraySet( asSet("LuceneFox"), false);
    assertTokenStreamContents(new LowerCaseFilterMock(
        new SetKeywordMarkerFilter(whitespaceMockTokenizer("The quIck browN LuceneFox Jumps"), mixedCaseSet)), output);
    CharArraySet set2 = set;
    assertTokenStreamContents(new LowerCaseFilterMock(
        new SetKeywordMarkerFilter(whitespaceMockTokenizer("The quIck browN LuceneFox Jumps"), set2)), output);
  }
  
  @Test
  public void testPatternFilterIncrementToken() throws IOException {
    String[] output = new String[] { "the", "quick", "brown", "LuceneFox",
        "jumps" };
    assertTokenStreamContents(new LowerCaseFilterMock(
        new PatternKeywordMarkerFilter(whitespaceMockTokenizer("The quIck browN LuceneFox Jumps"), Pattern.compile("[a-zA-Z]+[fF]ox"))), output);
    
    output = new String[] { "the", "quick", "brown", "lucenefox",
    "jumps" };
    
    assertTokenStreamContents(new LowerCaseFilterMock(
        new PatternKeywordMarkerFilter(whitespaceMockTokenizer("The quIck browN LuceneFox Jumps"), Pattern.compile("[a-zA-Z]+[f]ox"))), output);
  }

  // LUCENE-2901
  public void testComposition() throws Exception {   
    TokenStream ts = new LowerCaseFilterMock(
                     new SetKeywordMarkerFilter(
                     new SetKeywordMarkerFilter(
                     whitespaceMockTokenizer("Dogs Trees Birds Houses"),
                     new CharArraySet( asSet("Birds", "Houses"), false)), 
                     new CharArraySet( asSet("Dogs", "Trees"), false)));
    
    assertTokenStreamContents(ts, new String[] { "Dogs", "Trees", "Birds", "Houses" });
    
    ts = new LowerCaseFilterMock(
        new PatternKeywordMarkerFilter(
        new PatternKeywordMarkerFilter(
        whitespaceMockTokenizer("Dogs Trees Birds Houses"),
        Pattern.compile("Birds|Houses")), 
        Pattern.compile("Dogs|Trees")));

    assertTokenStreamContents(ts, new String[] { "Dogs", "Trees", "Birds", "Houses" });
    
    ts = new LowerCaseFilterMock(
        new SetKeywordMarkerFilter(
        new PatternKeywordMarkerFilter(
        whitespaceMockTokenizer("Dogs Trees Birds Houses"),
        Pattern.compile("Birds|Houses")), 
        new CharArraySet( asSet("Dogs", "Trees"), false)));

    assertTokenStreamContents(ts, new String[] { "Dogs", "Trees", "Birds", "Houses" });
  }
  
  public static final class LowerCaseFilterMock extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

    public LowerCaseFilterMock(TokenStream in) {
      super(in);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (!keywordAttr.isKeyword()) {
          final String term = termAtt.toString().toLowerCase(Locale.ROOT);
          termAtt.setEmpty().append(term);
        }
        return true;
      }
      return false;
    }

  }
}
