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
package org.apache.lucene.search.suggest.analyzing;

import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

public class TestSuggestStopFilter extends BaseTokenStreamTestCase {

  public void testEndNotStopWord() throws Exception {
    CharArraySet stopWords = StopFilter.makeStopSet("to");
    Tokenizer stream = new MockTokenizer();
    stream.setReader(new StringReader("go to"));
    TokenStream filter = new SuggestStopFilter(stream, stopWords);
    assertTokenStreamContents(filter,
                              new String[] {"go", "to"},
                              new int[] {0, 3},
                              new int[] {2, 5},
                              null,
                              new int[] {1, 1},
                              null,
                              5,
                              new boolean[] {false, true},
                              true);
  }

  public void testEndIsStopWord() throws Exception {
                              
    CharArraySet stopWords = StopFilter.makeStopSet("to");
    Tokenizer stream = new MockTokenizer();
    stream.setReader(new StringReader("go to "));
    TokenStream filter = new SuggestStopFilter(stream, stopWords);
    filter = new SuggestStopFilter(stream, stopWords);
    assertTokenStreamContents(filter,
                              new String[] {"go"},
                              new int[] {0},
                              new int[] {2},
                              null,
                              new int[] {1},
                              null,
                              6,
                              new boolean[] {false},
                              true);
  }

  public void testMidStopWord() throws Exception {
                              
    CharArraySet stopWords = StopFilter.makeStopSet("to");
    Tokenizer stream = new MockTokenizer();
    stream.setReader(new StringReader("go to school"));
    TokenStream filter = new SuggestStopFilter(stream, stopWords);

    filter = new SuggestStopFilter(stream, stopWords);
    assertTokenStreamContents(filter,
                              new String[] {"go", "school"},
                              new int[] {0, 6},
                              new int[] {2, 12},
                              null,
                              new int[] {1, 2},
                              null,
                              12,
                              new boolean[] {false, false},
                              true);
  }

  public void testMultipleStopWords() throws Exception {
                              
    CharArraySet stopWords = StopFilter.makeStopSet("to", "the", "a");
    Tokenizer stream = new MockTokenizer();
    stream.setReader(new StringReader("go to a the school"));
    TokenStream filter = new SuggestStopFilter(stream, stopWords);

    filter = new SuggestStopFilter(stream, stopWords);
    assertTokenStreamContents(filter,
                              new String[] { "go", "school" },
                              new int[] {0, 12},
                              new int[] {2, 18},
                              null,
                              new int[] {1, 4},
                              null,
                              18,
                              new boolean[] {false, false},
                              true);
  }

  public void testMultipleStopWordsEnd() throws Exception {
                              
    CharArraySet stopWords = StopFilter.makeStopSet("to", "the", "a");
    Tokenizer stream = new MockTokenizer();
    stream.setReader(new StringReader("go to a the"));
    TokenStream filter = new SuggestStopFilter(stream, stopWords);

    filter = new SuggestStopFilter(stream, stopWords);
    assertTokenStreamContents(filter,
                              new String[] { "go", "the"},
                              new int[] {0, 8},
                              new int[] {2, 11},
                              null,
                              new int[] {1, 3},
                              null,
                              11,
                              new boolean[] {false, true},
                              true);
  }

  public void testMultipleStopWordsEnd2() throws Exception {
                              
    CharArraySet stopWords = StopFilter.makeStopSet("to", "the", "a");
    Tokenizer stream = new MockTokenizer();
    stream.setReader(new StringReader("go to a the "));
    TokenStream filter = new SuggestStopFilter(stream, stopWords);

    filter = new SuggestStopFilter(stream, stopWords);
    assertTokenStreamContents(filter,
                              new String[] { "go"},
                              new int[] {0},
                              new int[] {2},
                              null,
                              new int[] {1},
                              null,
                              12,
                              new boolean[] {false},
                              true);
  }
}
