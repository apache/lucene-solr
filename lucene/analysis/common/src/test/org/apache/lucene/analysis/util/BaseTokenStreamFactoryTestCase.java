package org.apache.lucene.analysis.util;

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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.util.Version;

/** Base class for testing tokenstream factories */
// TODO: this has to be here, since the abstract factories are not in lucene-core,
// so test-framework doesnt know about them...
// this also means we currently cannot use this in other analysis modules :(
// TODO: maybe after we improve the abstract factory/SPI apis, they can sit in core and resolve this.
public abstract class BaseTokenStreamFactoryTestCase extends BaseTokenStreamTestCase {
  
  private AbstractAnalysisFactory analysisFactory(Class<? extends AbstractAnalysisFactory> clazz, Version matchVersion, ResourceLoader loader, String... keysAndValues) throws Exception {
    if (keysAndValues.length % 2 == 1) {
      throw new IllegalArgumentException("invalid keysAndValues map");
    }
    Map<String,String> args = new HashMap<String,String>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      String previous = args.put(keysAndValues[i], keysAndValues[i+1]);
      assertNull("duplicate values for key: " + keysAndValues[i], previous);
    }
    if (matchVersion != null) {
      String previous = args.put("luceneMatchVersion", matchVersion.toString());
      assertNull("duplicate values for key: luceneMatchVersion", previous);
    }
    AbstractAnalysisFactory factory = null;
    try {
      factory = clazz.getConstructor(Map.class).newInstance(args);
    } catch (InvocationTargetException e) {
      // to simplify tests that check for illegal parameters
      if (e.getCause() instanceof IllegalArgumentException) {
        throw (IllegalArgumentException) e.getCause();
      } else {
        throw e;
      }
    }
    if (factory instanceof ResourceLoaderAware) {
      ((ResourceLoaderAware) factory).inform(loader);
    }
    return factory;
  }
  
  protected TokenizerFactory tokenizerFactory(String name, String... keysAndValues) throws Exception {
    return tokenizerFactory(name, TEST_VERSION_CURRENT, new ClasspathResourceLoader(getClass()), keysAndValues);
  }
  
  protected TokenizerFactory tokenizerFactory(String name, Version matchVersion, ResourceLoader loader, String... keysAndValues) throws Exception {
    return (TokenizerFactory) analysisFactory(TokenizerFactory.lookupClass(name), matchVersion, loader, keysAndValues);
  }
  
  protected TokenFilterFactory tokenFilterFactory(String name, String... keysAndValues) throws Exception {
    return tokenFilterFactory(name, TEST_VERSION_CURRENT, new ClasspathResourceLoader(getClass()), keysAndValues);
  }
  
  protected TokenFilterFactory tokenFilterFactory(String name, Version matchVersion, ResourceLoader loader, String... keysAndValues) throws Exception {
    return (TokenFilterFactory) analysisFactory(TokenFilterFactory.lookupClass(name), matchVersion, loader, keysAndValues);
  }
  
  protected CharFilterFactory charFilterFactory(String name, String... keysAndValues) throws Exception {
    return charFilterFactory(name, TEST_VERSION_CURRENT, new ClasspathResourceLoader(getClass()), keysAndValues);
  }
  
  protected CharFilterFactory charFilterFactory(String name, Version matchVersion, ResourceLoader loader, String... keysAndValues) throws Exception {
    return (CharFilterFactory) analysisFactory(CharFilterFactory.lookupClass(name), matchVersion, loader, keysAndValues);
  }
}
