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

package org.apache.lucene.analysis.pattern;

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

/**
 * This test just ensures the factory works
 */
public class TestPatternTypingFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testFactory() throws Exception {
    Token tokenA1 = new Token("One", 0, 2);
    Token tokenA3 = new Token("forty-two", 11, 13);
    Token tokenB1 = new Token("4-2", 15, 19);

    TokenStream ts = new CannedTokenStream(tokenA1, tokenA3, tokenB1);

    TokenFilterFactory tokenFilterFactory = tokenFilterFactory("patternTyping", Version.LATEST, new StringMockResourceLoader(
        "6 \\b(\\d+)-(\\d+) ::: $1_hnum_$2\n" +
        "2 \\b(\\w+)-(\\w+) ::: $1_hword_$2"
    ), "patternFile", "patterns.txt");

    ts = tokenFilterFactory.create(ts);
    assertTokenStreamContents(ts, new String[]{
            "One", "forty-two", "4-2"}, null, null,
        new String[]{"word", "forty_hword_two", "4_hnum_2"},
        null, null, null, null, null, false, null,
        new int[]{0, 2, 6});
  }

}
