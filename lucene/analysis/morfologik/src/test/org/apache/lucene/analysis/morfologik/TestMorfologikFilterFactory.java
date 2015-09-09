package org.apache.lucene.analysis.morfologik;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoader;

/**
 * Test for {@link MorfologikFilterFactory}.
 */
public class TestMorfologikFilterFactory extends BaseTokenStreamTestCase {
  final ResourceLoader loader = new ClasspathResourceLoader(getClass());

  public void testDefaultDictionary() throws Exception {
    StringReader reader = new StringReader("rowery bilety");
    MorfologikFilterFactory factory = new MorfologikFilterFactory(Collections.<String,String>emptyMap());
    factory.inform(loader);
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"rower", "bilet"});
  }
  
  public void testResourceDictionary() throws Exception {
    StringReader reader = new StringReader("rowery bilety");
    Map<String,String> params = new HashMap<>();
    params.put(MorfologikFilterFactory.DICTIONARY_RESOURCE_ATTRIBUTE, MorfologikFilterFactory.DEFAULT_DICTIONARY_RESOURCE);
    MorfologikFilterFactory factory = new MorfologikFilterFactory(params);
    factory.inform(loader);
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"rower", "bilet"});
  }
  
  public void testResourceLoaderDictionary1() throws Exception {
    StringReader reader = new StringReader("rowery bilety");
    Map<String,String> params = new HashMap<>();
    params.put(MorfologikFilterFactory.DICTIONARY_FSA_FILE_ATTRIBUTE, "/morfologik/dictionaries/pl.dict");
    MorfologikFilterFactory factory = new MorfologikFilterFactory(params);
    factory.inform(loader);
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"rower", "bilet"});
  }
  
  public void testResourceLoaderDictionary2() throws Exception {
    StringReader reader = new StringReader("rowery bilety");
    Map<String,String> params = new HashMap<>();
    params.put(MorfologikFilterFactory.DICTIONARY_FSA_FILE_ATTRIBUTE, "/morfologik/dictionaries/pl.dict");
    params.put(MorfologikFilterFactory.DICTIONARY_FEATURES_FILE_ATTRIBUTE, "/morfologik/dictionaries/pl.info");
    MorfologikFilterFactory factory = new MorfologikFilterFactory(params);
    factory.inform(loader);
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"rower", "bilet"});
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put("bogusArg", "bogusValue");
      new MorfologikFilterFactory(params);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
  
  public void testIncompatibleArgs1() throws Exception {
    try {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(MorfologikFilterFactory.DICTIONARY_RESOURCE_ATTRIBUTE, MorfologikFilterFactory.DEFAULT_DICTIONARY_RESOURCE);
      params.put(MorfologikFilterFactory.DICTIONARY_FSA_FILE_ATTRIBUTE, "/morfologik/dictionaries/pl.dict");
      new MorfologikFilterFactory(params);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("at the same time"));
    }
  }
  
  public void testIncompatibleArgs2() throws Exception {
    try {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(MorfologikFilterFactory.DICTIONARY_RESOURCE_ATTRIBUTE, MorfologikFilterFactory.DEFAULT_DICTIONARY_RESOURCE);
      params.put(MorfologikFilterFactory.DICTIONARY_FSA_FILE_ATTRIBUTE, "/morfologik/dictionaries/pl.dict");
      params.put(MorfologikFilterFactory.DICTIONARY_FEATURES_FILE_ATTRIBUTE, "/morfologik/dictionaries/pl.info");
      new MorfologikFilterFactory(params);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("at the same time"));
    }
  }
  
  public void testMissingArgs1() throws Exception {
    try {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put(MorfologikFilterFactory.DICTIONARY_FEATURES_FILE_ATTRIBUTE, "/morfologik/dictionaries/pl.info");
      new MorfologikFilterFactory(params);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Missing"));
    }
  }
}
