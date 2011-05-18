package org.apache.lucene.analysis.ngram;

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

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

import java.io.StringReader;

/**
 * Tests {@link NGramTokenFilter} for correctness.
 */
public class NGramTokenFilterTest extends BaseTokenStreamTestCase {
    private TokenStream input;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        input = new MockTokenizer(new StringReader("abcde"), MockTokenizer.WHITESPACE, false);
    }

    public void testInvalidInput() throws Exception {
        boolean gotException = false;
        try {        
            new NGramTokenFilter(input, 2, 1);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue(gotException);
    }

    public void testInvalidInput2() throws Exception {
        boolean gotException = false;
        try {        
            new NGramTokenFilter(input, 0, 1);
        } catch (IllegalArgumentException e) {
            gotException = true;
        }
        assertTrue(gotException);
    }

    public void testUnigrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 1, 1);
      assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5});
    }

    public void testBigrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 2, 2);
      assertTokenStreamContents(filter, new String[]{"ab","bc","cd","de"}, new int[]{0,1,2,3}, new int[]{2,3,4,5});
    }

    public void testNgrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 1, 3);
      assertTokenStreamContents(filter,
        new String[]{"a","b","c","d","e", "ab","bc","cd","de", "abc","bcd","cde"}, 
        new int[]{0,1,2,3,4, 0,1,2,3, 0,1,2},
        new int[]{1,2,3,4,5, 2,3,4,5, 3,4,5}
      );
    }

    public void testOversizedNgrams() throws Exception {
      NGramTokenFilter filter = new NGramTokenFilter(input, 6, 7);
      assertTokenStreamContents(filter, new String[0], new int[0], new int[0]);
    }
    
    public void testSmallTokenInStream() throws Exception {
      input = new MockTokenizer(new StringReader("abc de fgh"), MockTokenizer.WHITESPACE, false);
      NGramTokenFilter filter = new NGramTokenFilter(input, 3, 3);
      assertTokenStreamContents(filter, new String[]{"abc","fgh"}, new int[]{0,7}, new int[]{3,10});
    }
    
    public void testReset() throws Exception {
      WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader("abcde"));
      NGramTokenFilter filter = new NGramTokenFilter(tokenizer, 1, 1);
      assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5});
      tokenizer.reset(new StringReader("abcde"));
      assertTokenStreamContents(filter, new String[]{"a","b","c","d","e"}, new int[]{0,1,2,3,4}, new int[]{1,2,3,4,5});
    }
}
