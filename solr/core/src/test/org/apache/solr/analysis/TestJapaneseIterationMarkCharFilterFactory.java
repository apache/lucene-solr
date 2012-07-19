package org.apache.solr.analysis;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.core.SolrResourceLoader;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple tests for {@link org.apache.solr.analysis.JapaneseIterationMarkCharFilterFactory}
 */
public class TestJapaneseIterationMarkCharFilterFactory extends BaseTokenStreamTestCase {

  public void testIterationMarksWithKeywordTokenizer() throws IOException {
    final String text = "時々馬鹿々々しいところゞゝゝミスヾ";
    JapaneseIterationMarkCharFilterFactory filterFactory = new JapaneseIterationMarkCharFilterFactory();
    CharFilter filter = filterFactory.create(new StringReader(text));
    TokenStream tokenStream = new MockTokenizer(filter, MockTokenizer.KEYWORD, false);
    assertTokenStreamContents(tokenStream, new String[]{"時時馬鹿馬鹿しいところどころミスズ"});
  }

  public void testIterationMarksWithJapaneseTokenizer() throws IOException {
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory();
    Map<String, String> tokenizerArgs = Collections.emptyMap();
    tokenizerFactory.init(tokenizerArgs);
    tokenizerFactory.inform(new SolrResourceLoader(null, null));

    JapaneseIterationMarkCharFilterFactory filterFactory = new JapaneseIterationMarkCharFilterFactory();
    Map<String, String> filterArgs = Collections.emptyMap();
    filterFactory.init(filterArgs);

    CharFilter filter = filterFactory.create(
        new StringReader("時々馬鹿々々しいところゞゝゝミスヾ")
    );
    TokenStream tokenStream = tokenizerFactory.create(filter);
    assertTokenStreamContents(tokenStream, new String[]{"時時", "馬鹿馬鹿しい", "ところどころ", "ミ", "スズ"});
  }

  public void testKanjiOnlyIterationMarksWithJapaneseTokenizer() throws IOException {
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory();
    Map<String, String> tokenizerArgs = Collections.emptyMap();
    tokenizerFactory.init(tokenizerArgs);
    tokenizerFactory.inform(new SolrResourceLoader(null, null));

    JapaneseIterationMarkCharFilterFactory filterFactory = new JapaneseIterationMarkCharFilterFactory();
    Map<String, String> filterArgs = new HashMap<String, String>();
    filterArgs.put("normalizeKanji", "true");
    filterArgs.put("normalizeKana", "false");
    filterFactory.init(filterArgs);
    
    CharFilter filter = filterFactory.create(
        new StringReader("時々馬鹿々々しいところゞゝゝミスヾ")
    );
    TokenStream tokenStream = tokenizerFactory.create(filter);
    assertTokenStreamContents(tokenStream, new String[]{"時時", "馬鹿馬鹿しい", "ところ", "ゞ", "ゝ", "ゝ", "ミス", "ヾ"});
  }

  public void testKanaOnlyIterationMarksWithJapaneseTokenizer() throws IOException {
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory();
    Map<String, String> tokenizerArgs = Collections.emptyMap();
    tokenizerFactory.init(tokenizerArgs);
    tokenizerFactory.inform(new SolrResourceLoader(null, null));

    JapaneseIterationMarkCharFilterFactory filterFactory = new JapaneseIterationMarkCharFilterFactory();
    Map<String, String> filterArgs = new HashMap<String, String>();
    filterArgs.put("normalizeKanji", "false");
    filterArgs.put("normalizeKana", "true");
    filterFactory.init(filterArgs);

    CharFilter filter = filterFactory.create(
        new StringReader("時々馬鹿々々しいところゞゝゝミスヾ")
    );
    TokenStream tokenStream = tokenizerFactory.create(filter);
    assertTokenStreamContents(tokenStream, new String[]{"時々", "馬鹿", "々", "々", "しい", "ところどころ", "ミ", "スズ"});
  }
}
