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
package org.apache.lucene.analysis.core;


import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.CrankyTokenFilter;
import org.apache.lucene.analysis.MockCharFilter;
import org.apache.lucene.analysis.MockFixedLengthPayloadFilter;
import org.apache.lucene.analysis.MockGraphTokenFilter;
import org.apache.lucene.analysis.MockHoleInjectingTokenFilter;
import org.apache.lucene.analysis.MockRandomLookaheadTokenFilter;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.MockVariableLengthPayloadFilter;
import org.apache.lucene.analysis.SimplePayloadFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ValidatingTokenFilter;
import org.apache.lucene.analysis.miscellaneous.PatternKeywordMarkerFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.path.ReversePathHierarchyTokenizer;
import org.apache.lucene.analysis.sinks.TeeSinkTokenFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.sr.SerbianNormalizationRegularFilter;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

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
      ValidatingTokenFilter.class,
      CrankyTokenFilter.class,
      SimplePayloadFilter.class
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
  
  // these are oddly-named (either the actual analyzer, or its factory)
  // they do actually have factories.
  // TODO: clean this up!
  private static final Set<Class<?>> oddlyNamedComponents = Collections.newSetFromMap(new IdentityHashMap<Class<?>,Boolean>());
  static {
    Collections.<Class<?>>addAll(oddlyNamedComponents,
      ReversePathHierarchyTokenizer.class, // this is supported via an option to PathHierarchyTokenizer's factory
      SnowballFilter.class, // this is called SnowballPorterFilterFactory
      PatternKeywordMarkerFilter.class,
      SetKeywordMarkerFilter.class,
      UnicodeWhitespaceTokenizer.class // a supported option via WhitespaceTokenizerFactory
    );
  }

  // The following token filters are excused from having their factory.
  private static final Set<Class<?>> tokenFiltersWithoutFactory = new HashSet<>();
  static {
    tokenFiltersWithoutFactory.add(SerbianNormalizationRegularFilter.class);
  }

  private static final ResourceLoader loader = new StringMockResourceLoader("");
  
  public void test() throws Exception {
    List<Class<?>> analysisClasses = TestRandomChains.getClassesForPackage("org.apache.lucene.analysis");
    
    for (final Class<?> c : analysisClasses) {
      final int modifiers = c.getModifiers();
      if (
        // don't waste time with abstract classes
        Modifier.isAbstract(modifiers) || !Modifier.isPublic(modifiers)
        || c.isSynthetic() || c.isAnonymousClass() || c.isMemberClass() || c.isInterface()
        || testComponents.contains(c)
        || crazyComponents.contains(c)
        || oddlyNamedComponents.contains(c)
        || tokenFiltersWithoutFactory.contains(c)
        || c.isAnnotationPresent(Deprecated.class) // deprecated ones are typically back compat hacks
        || !(Tokenizer.class.isAssignableFrom(c) || TokenFilter.class.isAssignableFrom(c) || CharFilter.class.isAssignableFrom(c))
      ) {
        continue;
      }

      Map<String,String> args = new HashMap<>();
      args.put("luceneMatchVersion", Version.LATEST.toString());
      
      if (Tokenizer.class.isAssignableFrom(c)) {
        String clazzName = c.getSimpleName();
        assertTrue(clazzName.endsWith("Tokenizer"));
        String simpleName = clazzName.substring(0, clazzName.length() - 9);
        assertNotNull(TokenizerFactory.lookupClass(simpleName));
        TokenizerFactory instance = null;
        try {
          instance = TokenizerFactory.forName(simpleName, args);
          assertNotNull(instance);
          if (instance instanceof ResourceLoaderAware) {
            ((ResourceLoaderAware) instance).inform(loader);
          }
          assertSame(c, instance.create().getClass());
        } catch (IllegalArgumentException e) {
          // TODO: For now pass because some factories have not yet a default config that always works
        }
      } else if (TokenFilter.class.isAssignableFrom(c)) {
        String clazzName = c.getSimpleName();
        assertTrue(clazzName.endsWith("Filter"));
        String simpleName = clazzName.substring(0, clazzName.length() - (clazzName.endsWith("TokenFilter") ? 11 : 6));
        assertNotNull(TokenFilterFactory.lookupClass(simpleName));
        TokenFilterFactory instance = null; 
        try {
          instance = TokenFilterFactory.forName(simpleName, args);
          assertNotNull(instance);
          if (instance instanceof ResourceLoaderAware) {
            ((ResourceLoaderAware) instance).inform(loader);
          }
          Class<? extends TokenStream> createdClazz = instance.create(new KeywordTokenizer()).getClass();
          // only check instance if factory have wrapped at all!
          if (KeywordTokenizer.class != createdClazz) {
            assertSame(c, createdClazz);
          }
        } catch (IllegalArgumentException e) {
          // TODO: For now pass because some factories have not yet a default config that always works
        }
      } else if (CharFilter.class.isAssignableFrom(c)) {
        String clazzName = c.getSimpleName();
        assertTrue(clazzName.endsWith("CharFilter"));
        String simpleName = clazzName.substring(0, clazzName.length() - 10);
        assertNotNull(CharFilterFactory.lookupClass(simpleName));
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
          // TODO: For now pass because some factories have not yet a default config that always works
        }
      }
    }
  }
}
