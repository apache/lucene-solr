package org.apache.solr.analysis;

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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Simple tests to ensure the Hindi filter Factories are working.
 */
public class TestHindiFilters extends BaseTokenTestCase {
  /**
   * Test IndicNormalizationFilterFactory
   */
  public void testIndicNormalizer() throws Exception {
    Reader reader = new StringReader("ত্‍ अाैर");
    StandardTokenizerFactory factory = new StandardTokenizerFactory();
    factory.setLuceneMatchVersion(DEFAULT_VERSION);
    IndicNormalizationFilterFactory filterFactory = new IndicNormalizationFilterFactory();
    filterFactory.setLuceneMatchVersion(DEFAULT_VERSION);
    factory.init(EMPTY_PARAMS);
    filterFactory.init(EMPTY_PARAMS);
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
    factory.setLuceneMatchVersion(DEFAULT_VERSION);
    IndicNormalizationFilterFactory indicFilterFactory = new IndicNormalizationFilterFactory();
    HindiNormalizationFilterFactory hindiFilterFactory = new HindiNormalizationFilterFactory();
    hindiFilterFactory.setLuceneMatchVersion(DEFAULT_VERSION);
    factory.init(EMPTY_PARAMS);
    hindiFilterFactory.init(EMPTY_PARAMS);
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
    factory.setLuceneMatchVersion(DEFAULT_VERSION);
    IndicNormalizationFilterFactory indicFilterFactory = new IndicNormalizationFilterFactory();
    HindiNormalizationFilterFactory hindiFilterFactory = new HindiNormalizationFilterFactory();
    HindiStemFilterFactory stemFactory = new HindiStemFilterFactory();
    stemFactory.setLuceneMatchVersion(DEFAULT_VERSION);
    factory.init(EMPTY_PARAMS);
    stemFactory.init(EMPTY_PARAMS);
    Tokenizer tokenizer = factory.create(reader);
    TokenStream stream = indicFilterFactory.create(tokenizer);
    stream = hindiFilterFactory.create(stream);
    stream = stemFactory.create(stream);
    assertTokenStreamContents(stream, new String[] {"किताब"});
  }
}
