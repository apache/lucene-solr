package org.apache.lucene.analysis.core;

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

import java.io.File;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.EmptyTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** tests random analysis chains */
public class TestRandomChains extends BaseTokenStreamTestCase {
  static List<Class<? extends Tokenizer>> tokenizers;
  static List<Class<? extends TokenFilter>> tokenfilters;
  static List<Class<? extends CharStream>> charfilters;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    List<Class<?>> analysisClasses = new ArrayList<Class<?>>();
    getClassesForPackage("org.apache.lucene.analysis", analysisClasses);
    tokenizers = new ArrayList<Class<? extends Tokenizer>>();
    tokenfilters = new ArrayList<Class<? extends TokenFilter>>();
    charfilters = new ArrayList<Class<? extends CharStream>>();
    for (Class<?> c : analysisClasses) {
      // don't waste time with abstract classes or deprecated known-buggy ones
      final int modifiers = c.getModifiers();
      if (Modifier.isAbstract(modifiers) || !Modifier.isPublic(modifiers)
          || c.getAnnotation(Deprecated.class) != null
          || c.isSynthetic() || c.isAnonymousClass() || c.isMemberClass() || c.isInterface()
          // TODO: fix basetokenstreamtestcase not to trip because this one has no CharTermAtt
          || c.equals(EmptyTokenizer.class)
          // doesn't actual reset itself!
          || c.equals(CachingTokenFilter.class)
          // broken!
          || c.equals(NGramTokenizer.class)
          // broken!
          || c.equals(NGramTokenFilter.class)
          // broken!
          || c.equals(EdgeNGramTokenizer.class)
          // broken!
          || c.equals(EdgeNGramTokenFilter.class)) {
        continue;
      }
      if (Tokenizer.class.isAssignableFrom(c)) {
        tokenizers.add(c.asSubclass(Tokenizer.class));
      } else if (TokenFilter.class.isAssignableFrom(c)) {
        tokenfilters.add(c.asSubclass(TokenFilter.class));
      } else if (CharStream.class.isAssignableFrom(c)) {
        charfilters.add(c.asSubclass(CharStream.class));
      }
    }
    final Comparator<Class<?>> classComp = new Comparator<Class<?>>() {
      @Override
      public int compare(Class<?> arg0, Class<?> arg1) {
        return arg0.getName().compareTo(arg1.getName());
      }
    };
    Collections.sort(tokenizers, classComp);
    Collections.sort(tokenfilters, classComp);
    Collections.sort(charfilters, classComp);
    if (VERBOSE) {
      System.out.println("tokenizers = " + tokenizers);
      System.out.println("tokenfilters = " + tokenfilters);
      System.out.println("charfilters = " + charfilters);
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    tokenizers = null;
    tokenfilters = null;
    charfilters = null;
  }
  
  static class MockRandomAnalyzer extends Analyzer {
    final long seed;
    
    MockRandomAnalyzer(long seed) {
      this.seed = seed;
    }
    
    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Random random = new Random(seed);
      TokenizerSpec tokenizerspec = newTokenizer(random, reader);
      TokenFilterSpec filterspec = newFilterChain(random, tokenizerspec.tokenizer);
      return new TokenStreamComponents(tokenizerspec.tokenizer, filterspec.stream);
    }

    @Override
    protected Reader initReader(Reader reader) {
      Random random = new Random(seed);
      CharFilterSpec charfilterspec = newCharFilterChain(random, reader);
      return charfilterspec.reader;
    }

    @Override
    public String toString() {
      Random random = new Random(seed);
      StringBuilder sb = new StringBuilder();
      CharFilterSpec charfilterSpec = newCharFilterChain(random, new StringReader(""));
      sb.append("\ncharfilters=");
      sb.append(charfilterSpec.toString);
      // intentional: initReader gets its own separate random
      random = new Random(seed);
      TokenizerSpec tokenizerSpec = newTokenizer(random, charfilterSpec.reader);
      sb.append("\n");
      sb.append("tokenizer=");
      sb.append(tokenizerSpec.toString);
      TokenFilterSpec tokenfilterSpec = newFilterChain(random, tokenizerSpec.tokenizer);
      sb.append("\n");
      sb.append("filters=");
      sb.append(tokenfilterSpec.toString);
      return sb.toString();
    }
    
    // create a new random tokenizer from classpath
    private TokenizerSpec newTokenizer(Random random, Reader reader) {
      TokenizerSpec spec = new TokenizerSpec();
      boolean success = false;
      while (!success) {
        try {
          // TODO: check Reader+Version,Version+Reader too
          // also look for other variants and handle them special
          int idx = random.nextInt(tokenizers.size());
          try {
            Constructor<? extends Tokenizer> c = tokenizers.get(idx).getConstructor(Version.class, Reader.class);
            spec.tokenizer = c.newInstance(TEST_VERSION_CURRENT, reader);
          } catch (NoSuchMethodException e) {
            Constructor<? extends Tokenizer> c = tokenizers.get(idx).getConstructor(Reader.class);
            spec.tokenizer = c.newInstance(reader);
          }
          spec.toString = tokenizers.get(idx).toString();
          success = true;
        } catch (Exception e) {
          // ignore
        }
      }
      return spec;
    }
    
    private CharFilterSpec newCharFilterChain(Random random, Reader reader) {
      CharFilterSpec spec = new CharFilterSpec();
      spec.reader = reader;
      StringBuilder descr = new StringBuilder();
      int numFilters = random.nextInt(3);
      for (int i = 0; i < numFilters; i++) {
        boolean success = false;
        while (!success) {
          try {
            // TODO: also look for other variants and handle them special
            int idx = random.nextInt(charfilters.size());
            try {
              Constructor<? extends CharStream> c = charfilters.get(idx).getConstructor(Reader.class);
              spec.reader = c.newInstance(spec.reader);
            } catch (NoSuchMethodException e) {
              Constructor<? extends CharStream> c = charfilters.get(idx).getConstructor(CharStream.class);
              spec.reader = c.newInstance(CharReader.get(spec.reader));
            }

            if (descr.length() > 0) {
              descr.append(",");
            }
            descr.append(charfilters.get(idx).toString());
            success = true;
          } catch (Exception e) {
            // ignore
          }
        }
      }
      spec.toString = descr.toString();
      return spec;
    }
    
    private TokenFilterSpec newFilterChain(Random random, Tokenizer tokenizer) {
      TokenFilterSpec spec = new TokenFilterSpec();
      spec.stream = tokenizer;
      StringBuilder descr = new StringBuilder();
      int numFilters = random.nextInt(5);
      for (int i = 0; i < numFilters; i++) {
        boolean success = false;
        while (!success) {
          try {
            // TODO: also look for other variants and handle them special
            int idx = random.nextInt(tokenfilters.size());
            try {
              Constructor<? extends TokenFilter> c = tokenfilters.get(idx).getConstructor(Version.class, TokenStream.class);
              spec.stream = c.newInstance(TEST_VERSION_CURRENT, spec.stream);
            } catch (NoSuchMethodException e) {
              Constructor<? extends TokenFilter> c = tokenfilters.get(idx).getConstructor(TokenStream.class);
              spec.stream = c.newInstance(spec.stream);
            }
            if (descr.length() > 0) {
              descr.append(",");
            }
            descr.append(tokenfilters.get(idx).toString());
            success = true;
          } catch (Exception e) {
            // ignore
          }
        }
      }
      spec.toString = descr.toString();
      return spec;
    }
  }
  
  static class TokenizerSpec {
    Tokenizer tokenizer;
    String toString;
  }
  
  static class TokenFilterSpec {
    TokenStream stream;
    String toString;
  }
  
  static class CharFilterSpec {
    Reader reader;
    String toString;
  }
  
  public void testRandomChains() throws Throwable {
    int numIterations = atLeast(20);
    for (int i = 0; i < numIterations; i++) {
      MockRandomAnalyzer a = new MockRandomAnalyzer(random.nextLong());
      if (VERBOSE) {
        System.out.println("Creating random analyzer:" + a);
      }
      try {
        checkRandomData(random, a, 1000);
      } catch (Throwable e) {
        System.err.println("Exception from random analyzer: " + a);
        throw e;
      }
    }
  }
  
  private static void getClassesForPackage(String pckgname, List<Class<?>> classes) throws Exception {
    final ClassLoader cld = TestRandomChains.class.getClassLoader();
    final String path = pckgname.replace('.', '/');
    final Enumeration<URL> resources = cld.getResources(path);
    while (resources.hasMoreElements()) {
      final File directory = new File(resources.nextElement().toURI());
      if (directory.exists()) {
        String[] files = directory.list();
        for (String file : files) {
          if (new File(directory, file).isDirectory()) {
            // recurse
            String subPackage = pckgname + "." + file;
            getClassesForPackage(subPackage, classes);
          }
          if (file.endsWith(".class")) {
            String clazzName = file.substring(0, file.length() - 6);
            // exclude Test classes that happen to be in these packages.
            // class.ForName'ing some of them can cause trouble.
            if (!clazzName.endsWith("Test") && !clazzName.startsWith("Test")) {
              // Don't run static initializers, as we won't use most of them.
              // Java will do that automatically once accessed/instantiated.
              classes.add(Class.forName(pckgname + '.' + clazzName, false, cld));
            }
          }
        }
      }
    }
  }
}
