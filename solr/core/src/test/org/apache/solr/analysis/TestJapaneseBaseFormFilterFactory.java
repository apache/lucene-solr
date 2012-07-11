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

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Simple tests for {@link JapaneseBaseFormFilterFactory}
 */
public class TestJapaneseBaseFormFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws IOException {
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory();
    tokenizerFactory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    tokenizerFactory.init(args);
    tokenizerFactory.inform(new SolrResourceLoader(null, null));
    TokenStream ts = tokenizerFactory.create(new StringReader("それはまだ実験段階にあります"));
    JapaneseBaseFormFilterFactory factory = new JapaneseBaseFormFilterFactory();
    ts = factory.create(ts);
    assertTokenStreamContents(ts,
        new String[] { "それ", "は", "まだ", "実験", "段階", "に", "ある", "ます"  }
    );
  }
}
