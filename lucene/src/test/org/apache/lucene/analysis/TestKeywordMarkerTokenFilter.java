package org.apache.lucene.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
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
 * Testcase for {@link KeywordMarkerTokenFilter}
 */
public class TestKeywordMarkerTokenFilter extends BaseTokenStreamTestCase {

  @Test
  public void testIncrementToken() throws IOException {
    CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 5, true);
    set.add("lucenefox");
    String[] output = new String[] { "the", "quick", "brown", "LuceneFox",
        "jumps" };
    assertTokenStreamContents(new LowerCaseFilterMock(
        new KeywordMarkerTokenFilter(new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(
            "The quIck browN LuceneFox Jumps")), set)), output);
    Set<String> jdkSet = new HashSet<String>();
    jdkSet.add("LuceneFox");
    assertTokenStreamContents(new LowerCaseFilterMock(
        new KeywordMarkerTokenFilter(new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(
            "The quIck browN LuceneFox Jumps")), jdkSet)), output);
    Set<?> set2 = set;
    assertTokenStreamContents(new LowerCaseFilterMock(
        new KeywordMarkerTokenFilter(new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(
            "The quIck browN LuceneFox Jumps")), set2)), output);
  }

  public static class LowerCaseFilterMock extends TokenFilter {

    private TermAttribute termAtt;
    private KeywordAttribute keywordAttr;

    public LowerCaseFilterMock(TokenStream in) {
      super(in);
      termAtt = addAttribute(TermAttribute.class);
      keywordAttr = addAttribute(KeywordAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (!keywordAttr.isKeyword())
          termAtt.setTermBuffer(termAtt.term().toLowerCase());
        return true;
      }
      return false;
    }

  }
}
