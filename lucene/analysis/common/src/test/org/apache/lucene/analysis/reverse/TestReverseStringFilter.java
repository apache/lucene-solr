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
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.util.Version;

public class TestReverseStringFilter extends BaseTokenStreamTestCase {
  public void testFilter() throws Exception {
    TokenStream stream = new MockTokenizer(new StringReader("Do have a nice day"),
        MockTokenizer.WHITESPACE, false);     // 1-4 length string
    ReverseStringFilter filter = new ReverseStringFilter(TEST_VERSION_CURRENT, stream);
    assertTokenStreamContents(filter, new String[] { "oD", "evah", "a", "ecin", "yad" });
  }
  
  public void testFilterWithMark() throws Exception {
    TokenStream stream = new MockTokenizer(new StringReader("Do have a nice day"),
        MockTokenizer.WHITESPACE, false); // 1-4 length string
    ReverseStringFilter filter = new ReverseStringFilter(TEST_VERSION_CURRENT, stream, '\u0001');
    assertTokenStreamContents(filter, 
        new String[] { "\u0001oD", "\u0001evah", "\u0001a", "\u0001ecin", "\u0001yad" });
  }

  public void testReverseString() throws Exception {
    assertEquals( "A", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "A" ) );
    assertEquals( "BA", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "AB" ) );
    assertEquals( "CBA", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "ABC" ) );
  }
  
  public void testReverseChar() throws Exception {
    char[] buffer = { 'A', 'B', 'C', 'D', 'E', 'F' };
    ReverseStringFilter.reverse(TEST_VERSION_CURRENT, buffer, 2, 3 );
    assertEquals( "ABEDCF", new String( buffer ) );
  }
  
  /**
   * Test the broken 3.0 behavior, for back compat
   * @deprecated (3.1) Remove in Lucene 5.0
   */
  @Deprecated
  public void testBackCompat() throws Exception {
    assertEquals("\uDF05\uD866\uDF05\uD866", ReverseStringFilter.reverse(Version.LUCENE_30, "𩬅𩬅"));
  }
  
  public void testReverseSupplementary() throws Exception {
    // supplementary at end
    assertEquals("𩬅艱鍟䇹愯瀛", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "瀛愯䇹鍟艱𩬅"));
    // supplementary at end - 1
    assertEquals("a𩬅艱鍟䇹愯瀛", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "瀛愯䇹鍟艱𩬅a"));
    // supplementary at start
    assertEquals("fedcba𩬅", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "𩬅abcdef"));
    // supplementary at start + 1
    assertEquals("fedcba𩬅z", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "z𩬅abcdef"));
    // supplementary medial
    assertEquals("gfe𩬅dcba", ReverseStringFilter.reverse(TEST_VERSION_CURRENT, "abcd𩬅efg"));
  }

  public void testReverseSupplementaryChar() throws Exception {
    // supplementary at end
    char[] buffer = "abc瀛愯䇹鍟艱𩬅".toCharArray();
    ReverseStringFilter.reverse(TEST_VERSION_CURRENT, buffer, 3, 7);
    assertEquals("abc𩬅艱鍟䇹愯瀛", new String(buffer));
    // supplementary at end - 1
    buffer = "abc瀛愯䇹鍟艱𩬅d".toCharArray();
    ReverseStringFilter.reverse(TEST_VERSION_CURRENT, buffer, 3, 8);
    assertEquals("abcd𩬅艱鍟䇹愯瀛", new String(buffer));
    // supplementary at start
    buffer = "abc𩬅瀛愯䇹鍟艱".toCharArray();
    ReverseStringFilter.reverse(TEST_VERSION_CURRENT, buffer, 3, 7);
    assertEquals("abc艱鍟䇹愯瀛𩬅", new String(buffer));
    // supplementary at start + 1
    buffer = "abcd𩬅瀛愯䇹鍟艱".toCharArray();
    ReverseStringFilter.reverse(TEST_VERSION_CURRENT, buffer, 3, 8);
    assertEquals("abc艱鍟䇹愯瀛𩬅d", new String(buffer));
    // supplementary medial
    buffer = "abc瀛愯𩬅def".toCharArray();
    ReverseStringFilter.reverse(TEST_VERSION_CURRENT, buffer, 3, 7);
    assertEquals("abcfed𩬅愯瀛", new String(buffer));
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new ReverseStringFilter(TEST_VERSION_CURRENT, tokenizer));
      }
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tokenizer = new KeywordTokenizer(reader);
        return new TokenStreamComponents(tokenizer, new ReverseStringFilter(TEST_VERSION_CURRENT, tokenizer));
      }
    };
    checkOneTermReuse(a, "", "");
  }
}
