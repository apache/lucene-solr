package org.apache.lucene.analysis.hi;

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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.in.IndicNormalizationFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;

/**
 * Simple tests to ensure the Hindi filter Factories are working.
 */
public class TestHindiFilters extends BaseTokenStreamTestCase {
  /**
   * Test IndicNormalizationFilterFactory
   */
  public void testIndicNormalizer() throws Exception {
    Reader reader = new StringReader("ত্‍ अाैर");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    IndicNormalizationFilterFactory filterFactory = new IndicNormalizationFilterFactory();
    filterFactory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    filterFactory.init(args);
    Tokenizer tokenizer = factory.create(reader);
    TokenStream stream = filterFactory.create(tokenizer);
    assertTokenStreamContents(stream, new String[] { "ৎ", "और" });
  }
  
  /**
   * Test HindiNormalizationFilterFactory
   */
  public void testHindiNormalizer() throws Exception {
    Reader reader = new StringReader("क़िताब");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    IndicNormalizationFilterFactory indicFilterFactory = new IndicNormalizationFilterFactory();
    HindiNormalizationFilterFactory hindiFilterFactory = new HindiNormalizationFilterFactory();
    hindiFilterFactory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    hindiFilterFactory.init(args);
    Tokenizer tokenizer = factory.create(reader);
    TokenStream stream = indicFilterFactory.create(tokenizer);
    stream = hindiFilterFactory.create(stream);
    assertTokenStreamContents(stream, new String[] {"किताब"});
  }
  
  /**
   * Test HindiStemFilterFactory
   */
  public void testStemmer() throws Exception {
    Reader reader = new StringReader("किताबें");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    IndicNormalizationFilterFactory indicFilterFactory = new IndicNormalizationFilterFactory();
    HindiNormalizationFilterFactory hindiFilterFactory = new HindiNormalizationFilterFactory();
    HindiStemFilterFactory stemFactory = new HindiStemFilterFactory();
    stemFactory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    Map<String, String> args = Collections.emptyMap();
    factory.init(args);
    stemFactory.init(args);
    Tokenizer tokenizer = factory.create(reader);
    TokenStream stream = indicFilterFactory.create(tokenizer);
    stream = hindiFilterFactory.create(stream);
    stream = stemFactory.create(stream);
    assertTokenStreamContents(stream, new String[] {"किताब"});
  }
}
