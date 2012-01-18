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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.core.SolrResourceLoader;

/**
 * Simple tests for {@link KuromojiTokenizerFactory}
 */
public class TestKuromojiTokenizerFactory extends BaseTokenTestCase {
  public void testSimple() throws IOException {
    KuromojiTokenizerFactory factory = new KuromojiTokenizerFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    factory.inform(new SolrResourceLoader(null, null));
    TokenStream ts = factory.create(new StringReader("これは本ではない"));
    assertTokenStreamContents(ts,
        new String[] { "これ", "は", "本", "で", "は", "ない" },
        new int[] { 0, 2, 3, 4, 5, 6 },
        new int[] { 2, 3, 4, 5, 6, 8 }
    );
  }
  
  public void testUserDict() throws IOException {
    String userDict = 
        "# Custom segmentation for long entries\n" +
        "日本経済新聞,日本 経済 新聞,ニホン ケイザイ シンブン,カスタム名詞\n" +
        "関西国際空港,関西 国際 空港,カンサイ コクサイ クウコウ,テスト名詞\n" +
        "# Custom reading for sumo wrestler\n" +
        "朝青龍,朝青龍,アサショウリュウ,カスタム人名\n";
    KuromojiTokenizerFactory factory = new KuromojiTokenizerFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("user-dictionary", "userdict.txt");
    factory.init(args);
    factory.inform(new StringMockSolrResourceLoader(userDict));
    TokenStream ts = factory.create(new StringReader("関西国際空港に行った"));
    assertTokenStreamContents(ts,
        new String[] { "関西", "国際", "空港", "に",  "行っ",  "た" }
    );
  }
}
