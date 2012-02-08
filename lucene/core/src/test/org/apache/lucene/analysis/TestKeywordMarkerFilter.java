package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Test;

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

/**
 * Testcase for {@link KeywordMarkerFilter}
 */
public class TestKeywordMarkerFilter extends BaseTokenStreamTestCase {

  @Test
  public void testIncrementToken() throws IOException {
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 5, true);
    set.add("lucenefox");
    String[] output = new String[] { "the", "quick", "brown", "LuceneFox",
        "jumps" };
    assertTokenStreamContents(new LowerCaseFilterMock(
        new KeywordMarkerFilter(new MockTokenizer(new StringReader(
            "The quIck browN LuceneFox Jumps"), MockTokenizer.WHITESPACE, false), set)), output);
    Set<String> jdkSet = new HashSet<String>();
    jdkSet.add("LuceneFox");
    assertTokenStreamContents(new LowerCaseFilterMock(
        new KeywordMarkerFilter(new MockTokenizer(new StringReader(
            "The quIck browN LuceneFox Jumps"), MockTokenizer.WHITESPACE, false), jdkSet)), output);
    Set<?> set2 = set;
    assertTokenStreamContents(new LowerCaseFilterMock(
        new KeywordMarkerFilter(new MockTokenizer(new StringReader(
            "The quIck browN LuceneFox Jumps"), MockTokenizer.WHITESPACE, false), set2)), output);
  }

  // LUCENE-2901
  public void testComposition() throws Exception {   
    TokenStream ts = new LowerCaseFilterMock(
                     new KeywordMarkerFilter(
                     new KeywordMarkerFilter(
                     new MockTokenizer(new StringReader("Dogs Trees Birds Houses"), MockTokenizer.WHITESPACE, false),
                     new HashSet<String>(Arrays.asList(new String[] { "Birds", "Houses" }))), 
                     new HashSet<String>(Arrays.asList(new String[] { "Dogs", "Trees" }))));
    
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
          final String term = termAtt.toString().toLowerCase(Locale.ENGLISH);
          termAtt.setEmpty().append(term);
        }
        return true;
      }
      return false;
    }

  }
}
