package org.apache.solr.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.core.SolrResourceLoader;

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
 * Simple tests for {@link KuromojiPartOfSpeechStopFilter}
 */
public class TestKuromojiPartOfSpeechStopFilterFactory extends BaseTokenTestCase {
  public void testBasics() throws IOException {
    String tags = 
        "#  verb-main:\n" +
        "動詞-自立\n";
    
    KuromojiTokenizerFactory tokenizerFactory = new KuromojiTokenizerFactory();
    tokenizerFactory.init(DEFAULT_VERSION_PARAM);
    tokenizerFactory.inform(new SolrResourceLoader(null, null));
    TokenStream ts = tokenizerFactory.create(new StringReader("私は制限スピードを超える。"));
    KuromojiPartOfSpeechStopFilterFactory factory = new KuromojiPartOfSpeechStopFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("luceneMatchVersion", TEST_VERSION_CURRENT.toString());
    args.put("tags", "stoptags.txt");
    factory.init(args);
    factory.inform(new StringMockSolrResourceLoader(tags));
    ts = factory.create(ts);
    assertTokenStreamContents(ts,
        new String[] { "私", "は", "制限", "スピード", "を" }
    );
  }
}
