package org.apache.lucene.analysis.synonym;

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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.util.Version;

public class TestSynonymFilterFactory extends BaseTokenStreamFactoryTestCase {
  /** test that we can parse and use the solr syn file */
  public void testSynonyms() throws Exception {
    Reader reader = new StringReader("GB");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Synonym", "synonyms", "synonyms.txt").create(stream);
    assertTrue(stream instanceof SynonymFilter);
    assertTokenStreamContents(stream, 
        new String[] { "GB", "gib", "gigabyte", "gigabytes" },
        new int[] { 1, 0, 0, 0 });
  }
  
  /** test that we can parse and use the solr syn file, with the old impl
   * @deprecated Remove this test in Lucene 5.0 */
  @Deprecated
  public void testSynonymsOld() throws Exception {
    Reader reader = new StringReader("GB");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Synonym", Version.LUCENE_33, new ClasspathResourceLoader(getClass()),
        "synonyms", "synonyms.txt").create(stream);
    assertTrue(stream instanceof SlowSynonymFilter);
    assertTokenStreamContents(stream, 
        new String[] { "GB", "gib", "gigabyte", "gigabytes" },
        new int[] { 1, 0, 0, 0 });
  }
  
  /** test multiword offsets with the old impl
   * @deprecated Remove this test in Lucene 5.0 */
  @Deprecated
  public void testMultiwordOffsetsOld() throws Exception {
    Reader reader = new StringReader("national hockey league");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Synonym", Version.LUCENE_33, new StringMockResourceLoader("national hockey league, nhl"),
        "synonyms", "synonyms.txt").create(stream);
    // WTF?
    assertTokenStreamContents(stream, 
        new String[] { "national", "nhl", "hockey", "league" },
        new int[] { 0, 0, 0, 0 },
        new int[] { 22, 22, 22, 22 },
        new int[] { 1, 0, 1, 1 });
  }
  
  /** if the synonyms are completely empty, test that we still analyze correctly */
  public void testEmptySynonyms() throws Exception {
    Reader reader = new StringReader("GB");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Synonym", TEST_VERSION_CURRENT, 
        new StringMockResourceLoader(""), // empty file!
        "synonyms", "synonyms.txt").create(stream);
    assertTokenStreamContents(stream, new String[] { "GB" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Synonym", 
          "synonyms", "synonyms.txt", 
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
