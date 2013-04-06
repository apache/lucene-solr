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

import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.MockCharFilter;
import org.apache.lucene.analysis.MockFixedLengthPayloadFilter;
import org.apache.lucene.analysis.MockGraphTokenFilter;
import org.apache.lucene.analysis.MockHoleInjectingTokenFilter;
import org.apache.lucene.analysis.MockRandomLookaheadTokenFilter;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.MockVariableLengthPayloadFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ValidatingTokenFilter;
import org.apache.lucene.analysis.miscellaneous.PatternKeywordMarkerFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.fr.FrenchStemFilter;
import org.apache.lucene.analysis.in.IndicTokenizer;
import org.apache.lucene.analysis.nl.DutchStemFilter;
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer;
import org.apache.lucene.analysis.sinks.TeeSinkTokenFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests that any newly added Tokenizers/TokenFilters/CharFilters have a
 * corresponding factory (and that the SPI configuration is correct)
 */
public class TestAllAnalyzersHaveFactories extends LuceneTestCase {

  // these are test-only components (e.g. test-framework)
  private static final Set<Class<?>> testComponents = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
  static {
    Collections.<Class<?>>addAll(testComponents,
      MockTokenizer.class,
      MockCharFilter.class,
      MockFixedLengthPayloadFilter.class,
      MockGraphTokenFilter.class,
      MockHoleInjectingTokenFilter.class,
      MockRandomLookaheadTokenFilter.class,
      MockTokenFilter.class,
      MockVariableLengthPayloadFilter.class,
      ValidatingTokenFilter.class
    );
  }
  
  // these are 'crazy' components like cachingtokenfilter. does it make sense to add factories for these?
  private static final Set<Class<?>> crazyComponents = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
  static {
    Collections.<Class<?>>addAll(crazyComponents,
      CachingTokenFilter.class,
      TeeSinkTokenFilter.class
    );
  }
  
  // these are deprecated components that are just exact dups of other functionality: they dont need factories
  // (they never had them)
  private static final Set<Class<?>> deprecatedDuplicatedComponents = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
  static {
    Collections.<Class<?>>addAll(deprecatedDuplicatedComponents,
      DutchStemFilter.class,
      FrenchStemFilter.class,
      IndicTokenizer.class
    );
  }
  
  // these are oddly-named (either the actual analyzer, or its factory)
  // they do actually have factories.
  // TODO: clean this up!
  private static final Set<Class<?>> oddlyNamedComponents = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
  static {
    Collections.<Class<?>>addAll(oddlyNamedComponents,
      ReversePathHierarchyTokenizer.class, // this is supported via an option to PathHierarchyTokenizer's factory
      SnowballFilter.class, // this is called SnowballPorterFilterFactory
      PatternKeywordMarkerFilter.class,
      SetKeywordMarkerFilter.class
    );
  }
  
  private static final ResourceLoader loader = new StringMockResourceLoader("");
  
  public void test() throws Exception {
    List<Class<?>> analysisClasses = new ArrayList<Class<?>>();
    analysisClasses.addAll(TestRandomChains.getClassesForPackage("org.apache.lucene.analysis"));
    analysisClasses.addAll(TestRandomChains.getClassesForPackage("org.apache.lucene.collation"));
    
    for (final Class<?> c : analysisClasses) {
      final int modifiers = c.getModifiers();
      if (
        // don't waste time with abstract classes
        Modifier.isAbstract(modifiers) || !Modifier.isPublic(modifiers)
        || c.isSynthetic() || c.isAnonymousClass() || c.isMemberClass() || c.isInterface()
        || testComponents.contains(c)
        || crazyComponents.contains(c)
        || oddlyNamedComponents.contains(c)
        || deprecatedDuplicatedComponents.contains(c)
        || !(Tokenizer.class.isAssignableFrom(c) || TokenFilter.class.isAssignableFrom(c) || CharFilter.class.isAssignableFrom(c))
      ) {
        continue;
      }
      
      Map<String,String> args = new HashMap<String,String>();
      args.put("luceneMatchVersion", TEST_VERSION_CURRENT.toString());
      
      if (Tokenizer.class.isAssignableFrom(c)) {
        String clazzName = c.getSimpleName();
        assertTrue(clazzName.endsWith("Tokenizer"));
        String simpleName = clazzName.substring(0, clazzName.length() - 9);
        TokenizerFactory instance = null;
        try {
          instance = TokenizerFactory.forName(simpleName, args);
          assertNotNull(instance);
          if (instance instanceof ResourceLoaderAware) {
            ((ResourceLoaderAware) instance).inform(loader);
          }
          assertSame(c, instance.create(new StringReader("")).getClass());
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().contains("SPI")) {
            throw e;
          }
          // TODO: For now pass because some factories have not yet a default config that always works
        }
      } else if (TokenFilter.class.isAssignableFrom(c)) {
        String clazzName = c.getSimpleName();
        assertTrue(clazzName.endsWith("Filter"));
        String simpleName = clazzName.substring(0, clazzName.length() - (clazzName.endsWith("TokenFilter") ? 11 : 6));
        TokenFilterFactory instance = null; 
        try {
          instance = TokenFilterFactory.forName(simpleName, args);
          assertNotNull(instance);
          if (instance instanceof ResourceLoaderAware) {
            ((ResourceLoaderAware) instance).inform(loader);
          }
          Class<? extends TokenStream> createdClazz = instance.create(new KeywordTokenizer(new StringReader(""))).getClass();
          // only check instance if factory have wrapped at all!
          if (KeywordTokenizer.class != createdClazz) {
            assertSame(c, createdClazz);
          }
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().contains("SPI")) {
            throw e;
          }
          // TODO: For now pass because some factories have not yet a default config that always works
        }
      } else if (CharFilter.class.isAssignableFrom(c)) {
        String clazzName = c.getSimpleName();
        assertTrue(clazzName.endsWith("CharFilter"));
        String simpleName = clazzName.substring(0, clazzName.length() - 10);
        CharFilterFactory instance = null;
        try {
          instance = CharFilterFactory.forName(simpleName, args);
          assertNotNull(instance);
          if (instance instanceof ResourceLoaderAware) {
            ((ResourceLoaderAware) instance).inform(loader);
          }
          Class<? extends Reader> createdClazz = instance.create(new StringReader("")).getClass();
          // only check instance if factory have wrapped at all!
          if (StringReader.class != createdClazz) {
            assertSame(c, createdClazz);
          }
        } catch (IllegalArgumentException e) {
          if (!e.getMessage().contains("SPI")) {
            throw e;
          }
          // TODO: For now pass because some factories have not yet a default config that always works
        }
      }
    }
  }
}
