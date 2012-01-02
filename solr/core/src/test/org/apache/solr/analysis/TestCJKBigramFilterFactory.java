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

import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Simple tests to ensure the CJK bigram factory is working.
 * @deprecated
 */
public class TestCJKBigramFilterFactory extends BaseTokenTestCase {
  public void testDefaults() throws Exception {
    Reader reader = new StringReader("多くの学生が試験に落ちた。");
    CJKBigramFilterFactory factory = new CJKBigramFilterFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    TokenStream stream = factory.create(new StandardTokenizer(TEST_VERSION_CURRENT, reader));
    assertTokenStreamContents(stream,
        new String[] { "多く", "くの", "の学", "学生", "生が", "が試", "試験", "験に", "に落", "落ち", "ちた" });
  }
  
  public void testHanOnly() throws Exception {
    Reader reader = new StringReader("多くの学生が試験に落ちた。");
    CJKBigramFilterFactory factory = new CJKBigramFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("hiragana", "false");
    factory.init(args);
    TokenStream stream = factory.create(new StandardTokenizer(TEST_VERSION_CURRENT, reader));
    assertTokenStreamContents(stream,
        new String[] { "多", "く", "の",  "学生", "が",  "試験", "に",  "落", "ち", "た" });
  }
}
