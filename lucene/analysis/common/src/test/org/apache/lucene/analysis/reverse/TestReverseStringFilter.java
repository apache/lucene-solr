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
package org.apache.lucene.analysis.reverse;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

public class TestReverseStringFilter extends BaseTokenStreamTestCase {
  public void testFilter() throws Exception {
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);     // 1-4 length string
    ((Tokenizer)stream).setReader(new StringReader("Do have a nice day"));
    ReverseStringFilter filter = new ReverseStringFilter(stream);
    assertTokenStreamContents(filter, new String[] { "oD", "evah", "a", "ecin", "yad" });
  }
  
  public void testFilterWithMark() throws Exception {
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false); // 1-4 length string
    ((Tokenizer)stream).setReader(new StringReader("Do have a nice day"));
    ReverseStringFilter filter = new ReverseStringFilter(stream, '\u0001');
    assertTokenStreamContents(filter, 
        new String[] { "\u0001oD", "\u0001evah", "\u0001a", "\u0001ecin", "\u0001yad" });
  }

  public void testReverseString() throws Exception {
    assertEquals( "A", ReverseStringFilter.reverse( "A" ) );
    assertEquals( "BA", ReverseStringFilter.reverse( "AB" ) );
    assertEquals( "CBA", ReverseStringFilter.reverse( "ABC" ) );
  }
  
  public void testReverseChar() throws Exception {
    char[] buffer = { 'A', 'B', 'C', 'D', 'E', 'F' };
    ReverseStringFilter.reverse( buffer, 2, 3 );
    assertEquals( "ABEDCF", new String( buffer ) );
  }
  
  public void testReverseSupplementary() throws Exception {
    // supplementary at end
    assertEquals("𩬅艱鍟䇹愯瀛", ReverseStringFilter.reverse("瀛愯䇹鍟艱𩬅"));
    // supplementary at end - 1
    assertEquals("a𩬅艱鍟䇹愯瀛", ReverseStringFilter.reverse("瀛愯䇹鍟艱𩬅a"));
    // supplementary at start
    assertEquals("fedcba𩬅", ReverseStringFilter.reverse("𩬅abcdef"));
    // supplementary at start + 1
    assertEquals("fedcba𩬅z", ReverseStringFilter.reverse("z𩬅abcdef"));
    // supplementary medial
    assertEquals("gfe𩬅dcba", ReverseStringFilter.reverse("abcd𩬅efg"));
  }

  public void testReverseSupplementaryChar() throws Exception {
    // supplementary at end
    char[] buffer = "abc瀛愯䇹鍟艱𩬅".toCharArray();
    ReverseStringFilter.reverse(buffer, 3, 7);
    assertEquals("abc𩬅艱鍟䇹愯瀛", new String(buffer));
    // supplementary at end - 1
    buffer = "abc瀛愯䇹鍟艱𩬅d".toCharArray();
    ReverseStringFilter.reverse(buffer, 3, 8);
    assertEquals("abcd𩬅艱鍟䇹愯瀛", new String(buffer));
    // supplementary at start
    buffer = "abc𩬅瀛愯䇹鍟艱".toCharArray();
    ReverseStringFilter.reverse(buffer, 3, 7);
    assertEquals("abc艱鍟䇹愯瀛𩬅", new String(buffer));
    // supplementary at start + 1
    buffer = "abcd𩬅瀛愯䇹鍟艱".toCharArray();
    ReverseStringFilter.reverse(buffer, 3, 8);
    assertEquals("abc艱鍟䇹愯瀛𩬅d", new String(buffer));
    // supplementary medial
    buffer = "abc瀛愯𩬅def".toCharArray();
    ReverseStringFilter.reverse(buffer, 3, 7);
    assertEquals("abcfed𩬅愯瀛", new String(buffer));
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new ReverseStringFilter(tokenizer));
      }
    };
    checkRandomData(random(), a, 200 * RANDOM_MULTIPLIER);
    a.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new ReverseStringFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }
}
