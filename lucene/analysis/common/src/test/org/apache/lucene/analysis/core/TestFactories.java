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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.lucene.util.AttributeSource.AttributeFactory;

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
    Class<? extends TokenizerFactory> factoryClazz = TokenizerFactory.lookupClass(tokenizer);
    TokenizerFactory factory = (TokenizerFactory) initialize(factoryClazz);
    if (factory != null) {
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
    Class<? extends TokenFilterFactory> factoryClazz = TokenFilterFactory.lookupClass(tokenfilter);
    TokenFilterFactory factory = (TokenFilterFactory) initialize(factoryClazz);
    if (factory != null) {
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
    Class<? extends CharFilterFactory> factoryClazz = CharFilterFactory.lookupClass(charfilter);
    CharFilterFactory factory = (CharFilterFactory) initialize(factoryClazz);
    if (factory != null) {
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
  private AbstractAnalysisFactory initialize(Class<? extends AbstractAnalysisFactory> factoryClazz) throws IOException {
    Map<String,String> args = new HashMap<String,String>();
    args.put("luceneMatchVersion", TEST_VERSION_CURRENT.toString());
    Constructor<? extends AbstractAnalysisFactory> ctor;
    try {
      ctor = factoryClazz.getConstructor(Map.class);
    } catch (Exception e) {
      throw new RuntimeException("factory '" + factoryClazz + "' does not have a proper ctor!");
    }
    
    AbstractAnalysisFactory factory = null;
    try {
      factory = ctor.newInstance(args);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof IllegalArgumentException) {
        // its ok if we dont provide the right parameters to throw this
        return null;
      }
    }
    
    if (factory instanceof ResourceLoaderAware) {
      try {
        ((ResourceLoaderAware) factory).inform(new StringMockResourceLoader(""));
      } catch (IOException ignored) {
        // its ok if the right files arent available or whatever to throw this
      } catch (IllegalArgumentException ignored) {
        // is this ok? I guess so
      }
    }
    return factory;
  }
  
  // some silly classes just so we can use checkRandomData
  private TokenizerFactory assertingTokenizer = new TokenizerFactory(new HashMap<String,String>()) {
    @Override
    public MockTokenizer create(AttributeFactory factory, Reader input) {
      return new MockTokenizer(factory, input);
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
