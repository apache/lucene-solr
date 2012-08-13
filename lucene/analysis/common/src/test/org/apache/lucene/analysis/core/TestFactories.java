package org.apache.lucene.analysis.core;

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

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.MultiTermAwareComponent;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;

/**
 * Sanity check some things about all factories,
 * we do our best to see if we can sanely initialize it with
 * no parameters and smoke test it, etc.
 */
// TODO: move this, TestRandomChains, and TestAllAnalyzersHaveFactories
// to an integration test module that sucks in all analysis modules.
// currently the only way to do this is via eclipse etc (LUCENE-3974)
public class TestFactories extends BaseTokenStreamTestCase {
  public void test() throws IOException {
    for (String tokenizer : TokenizerFactory.availableTokenizers()) {
      doTestTokenizer(tokenizer);
    }
    
    for (String tokenFilter : TokenFilterFactory.availableTokenFilters()) {
      doTestTokenFilter(tokenFilter);
    }
    
    for (String charFilter : CharFilterFactory.availableCharFilters()) {
      doTestCharFilter(charFilter);
    }
  }
  
  private void doTestTokenizer(String tokenizer) throws IOException {
    TokenizerFactory factory = TokenizerFactory.forName(tokenizer);
    if (initialize(factory)) {
      // we managed to fully create an instance. check a few more things:
      
      // if it implements MultiTermAware, sanity check its impl
      if (factory instanceof MultiTermAwareComponent) {
        AbstractAnalysisFactory mtc = ((MultiTermAwareComponent) factory).getMultiTermComponent();
        assertNotNull(mtc);
        // its not ok to return e.g. a charfilter here: but a tokenizer could wrap a filter around it
        assertFalse(mtc instanceof CharFilterFactory);
      }
      
      // beast it just a little, it shouldnt throw exceptions:
      // (it should have thrown them in initialize)
      checkRandomData(random(), new FactoryAnalyzer(factory, null, null), 100, 20, false, false);
    }
  }
  
  private void doTestTokenFilter(String tokenfilter) throws IOException {
    TokenFilterFactory factory = TokenFilterFactory.forName(tokenfilter);
    if (initialize(factory)) {
      // we managed to fully create an instance. check a few more things:
      
      // if it implements MultiTermAware, sanity check its impl
      if (factory instanceof MultiTermAwareComponent) {
        AbstractAnalysisFactory mtc = ((MultiTermAwareComponent) factory).getMultiTermComponent();
        assertNotNull(mtc);
        // its not ok to return a charfilter or tokenizer here, this makes no sense
        assertTrue(mtc instanceof TokenFilterFactory);
      }
      
      // beast it just a little, it shouldnt throw exceptions:
      // (it should have thrown them in initialize)
      checkRandomData(random(), new FactoryAnalyzer(assertingTokenizer, factory, null), 100, 20, false, false);
    }
  }
  
  private void doTestCharFilter(String charfilter) throws IOException {
    CharFilterFactory factory = CharFilterFactory.forName(charfilter);
    if (initialize(factory)) {
      // we managed to fully create an instance. check a few more things:
      
      // if it implements MultiTermAware, sanity check its impl
      if (factory instanceof MultiTermAwareComponent) {
        AbstractAnalysisFactory mtc = ((MultiTermAwareComponent) factory).getMultiTermComponent();
        assertNotNull(mtc);
        // its not ok to return a tokenizer or tokenfilter here, this makes no sense
        assertTrue(mtc instanceof CharFilterFactory);
      }
      
      // beast it just a little, it shouldnt throw exceptions:
      // (it should have thrown them in initialize)
      checkRandomData(random(), new FactoryAnalyzer(assertingTokenizer, null, factory), 100, 20, false, false);
    }
  }
  
  /** tries to initialize a factory with no arguments */
  private boolean initialize(AbstractAnalysisFactory factory) {
    boolean success = false;
    try {
      factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
      factory.init(Collections.<String,String>emptyMap());
      success = true;
    } catch (IllegalArgumentException ignored) {
      // its ok if we dont provide the right parameters to throw this
    }
    
    if (factory instanceof ResourceLoaderAware) {
      success = false;
      try {
        ((ResourceLoaderAware) factory).inform(new StringMockResourceLoader(""));
        success = true;
      } catch (IOException ignored) {
        // its ok if the right files arent available or whatever to throw this
      } catch (IllegalArgumentException ignored) {
        // is this ok? I guess so
      }
    }
    return success;
  }
  
  // some silly classes just so we can use checkRandomData
  private TokenizerFactory assertingTokenizer = new TokenizerFactory() {
    @Override
    public Tokenizer create(Reader input) {
      return new MockTokenizer(input);
    }
  };
  
  private static class FactoryAnalyzer extends Analyzer {
    final TokenizerFactory tokenizer;
    final CharFilterFactory charFilter;
    final TokenFilterFactory tokenfilter;
    
    FactoryAnalyzer(TokenizerFactory tokenizer, TokenFilterFactory tokenfilter, CharFilterFactory charFilter) {
      assert tokenizer != null;
      this.tokenizer = tokenizer;
      this.charFilter = charFilter;
      this.tokenfilter = tokenfilter;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tf = tokenizer.create(reader);
      if (tokenfilter != null) {
        return new TokenStreamComponents(tf, tokenfilter.create(tf));
      } else {
        return new TokenStreamComponents(tf);
      }
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
      if (charFilter != null) {
        return charFilter.create(reader);
      } else {
        return reader;
      }
    }
  }
}
