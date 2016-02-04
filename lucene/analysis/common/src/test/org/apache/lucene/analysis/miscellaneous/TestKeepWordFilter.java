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
package org.apache.lucene.analysis.miscellaneous;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

/** Test {@link KeepWordFilter} */
public class TestKeepWordFilter extends BaseTokenStreamTestCase {
  
  public void testStopAndGo() throws Exception 
  {  
    Set<String> words = new HashSet<>();
    words.add( "aaa" );
    words.add( "bbb" );
    
    String input = "xxx yyy aaa zzz BBB ccc ddd EEE";
    
    // Test Stopwords
    TokenStream stream = whitespaceMockTokenizer(input);
    stream = new KeepWordFilter(stream, new CharArraySet( words, true));
    assertTokenStreamContents(stream, new String[] { "aaa", "BBB" }, new int[] { 3, 2 });
       
    // Now force case
    stream = whitespaceMockTokenizer(input);
    stream = new KeepWordFilter(stream, new CharArraySet(words, false));
    assertTokenStreamContents(stream, new String[] { "aaa" }, new int[] { 3 });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    final Set<String> words = new HashSet<>();
    words.add( "a" );
    words.add( "b" );
    
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TokenStream stream = new KeepWordFilter(tokenizer, new CharArraySet( words, true));
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
    
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }
}
