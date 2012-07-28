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

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.util.Version;

public class TestSynonymFilterFactory extends BaseTokenStreamTestCase {
  /** test that we can parse and use the solr syn file */
  public void testSynonyms() throws Exception {
    SynonymFilterFactory factory = new SynonymFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("synonyms", "synonyms.txt");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(new ClasspathResourceLoader(getClass()));
    TokenStream ts = factory.create(new MockTokenizer(new StringReader("GB"), MockTokenizer.WHITESPACE, false));
    assertTrue(ts instanceof SynonymFilter);
    assertTokenStreamContents(ts, 
        new String[] { "GB", "gib", "gigabyte", "gigabytes" },
        new int[] { 1, 0, 0, 0 });
  }
  
  /** test that we can parse and use the solr syn file, with the old impl
   * @deprecated Remove this test in Lucene 5.0 */
  @Deprecated
  public void testSynonymsOld() throws Exception {
    SynonymFilterFactory factory = new SynonymFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("synonyms", "synonyms.txt");
    factory.setLuceneMatchVersion(Version.LUCENE_33);
    factory.init(args);
    factory.inform(new ClasspathResourceLoader(getClass()));
    TokenStream ts = factory.create(new MockTokenizer(new StringReader("GB"), MockTokenizer.WHITESPACE, false));
    assertTrue(ts instanceof SlowSynonymFilter);
    assertTokenStreamContents(ts, 
        new String[] { "GB", "gib", "gigabyte", "gigabytes" },
        new int[] { 1, 0, 0, 0 });
  }
  
  /** test multiword offsets with the old impl
   * @deprecated Remove this test in Lucene 5.0 */
  @Deprecated
  public void testMultiwordOffsetsOld() throws Exception {
    SynonymFilterFactory factory = new SynonymFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("synonyms", "synonyms.txt");
    factory.setLuceneMatchVersion(Version.LUCENE_33);
    factory.init(args);
    factory.inform(new StringMockResourceLoader("national hockey league, nhl"));
    TokenStream ts = factory.create(new MockTokenizer(new StringReader("national hockey league"), MockTokenizer.WHITESPACE, false));
    // WTF?
    assertTokenStreamContents(ts, 
        new String[] { "national", "nhl", "hockey", "league" },
        new int[] { 0, 0, 0, 0 },
        new int[] { 22, 22, 22, 22 },
        new int[] { 1, 0, 1, 1 });
  }
  
  /** if the synonyms are completely empty, test that we still analyze correctly */
  public void testEmptySynonyms() throws Exception {
    SynonymFilterFactory factory = new SynonymFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("synonyms", "synonyms.txt");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(new StringMockResourceLoader("")); // empty file!
    TokenStream ts = factory.create(new MockTokenizer(new StringReader("GB"), MockTokenizer.WHITESPACE, false));
    assertTokenStreamContents(ts, new String[] { "GB" });
  }
}
