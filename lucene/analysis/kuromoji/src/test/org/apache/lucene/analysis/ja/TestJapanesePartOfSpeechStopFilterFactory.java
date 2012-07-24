package org.apache.lucene.analysis.ja;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;

/**
 * Simple tests for {@link JapanesePartOfSpeechStopFilterFactory}
 */
public class TestJapanesePartOfSpeechStopFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws IOException {
    String tags = 
        "#  verb-main:\n" +
        "動詞-自立\n";
    
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory();
    tokenizerFactory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> tokenizerArgs = Collections.emptyMap();
    tokenizerFactory.init(tokenizerArgs);
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream ts = tokenizerFactory.create(new StringReader("私は制限スピードを超える。"));
    JapanesePartOfSpeechStopFilterFactory factory = new JapanesePartOfSpeechStopFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("tags", "stoptags.txt");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(new StringMockResourceLoader(tags));
    ts = factory.create(ts);
    assertTokenStreamContents(ts,
        new String[] { "私", "は", "制限", "スピード", "を" }
    );
  }
}
