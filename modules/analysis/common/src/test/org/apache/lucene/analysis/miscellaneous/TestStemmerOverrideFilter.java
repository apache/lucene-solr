package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

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

public class TestStemmerOverrideFilter extends BaseTokenStreamTestCase {
  public void testOverride() throws IOException {
    // lets make booked stem to books
    // the override filter will convert "booked" to "books",
    // but also mark it with KeywordAttribute so Porter will not change it.
    Map<String,String> dictionary = new HashMap<String,String>();
    dictionary.put("booked", "books");
    Tokenizer tokenizer = new KeywordTokenizer(new StringReader("booked"));
    TokenStream stream = new PorterStemFilter(
        new StemmerOverrideFilter(TEST_VERSION_CURRENT, tokenizer, dictionary));
    assertTokenStreamContents(stream, new String[] { "books" });
  }
}
