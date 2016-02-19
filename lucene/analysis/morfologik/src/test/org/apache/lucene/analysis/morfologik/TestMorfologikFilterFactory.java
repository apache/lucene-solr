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
package org.apache.lucene.analysis.morfologik;


import java.io.IOException;
import java.io.InputStream;
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
  private static class ForbidResourcesLoader implements ResourceLoader {
    @Override
    public InputStream openResource(String resource) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T newInstance(String cname, Class<T> expectedType) {
      throw new UnsupportedOperationException();
    }
  }

  public void testDefaultDictionary() throws Exception {
    StringReader reader = new StringReader("rowery bilety");
    MorfologikFilterFactory factory = new MorfologikFilterFactory(Collections.<String,String>emptyMap());
    factory.inform(new ForbidResourcesLoader());
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"rower", "bilet"});
  }

  public void testExplicitDictionary() throws Exception {
    final ResourceLoader loader = new ClasspathResourceLoader(TestMorfologikFilterFactory.class);

    StringReader reader = new StringReader("inflected1 inflected2");
    Map<String,String> params = new HashMap<>();
    params.put(MorfologikFilterFactory.DICTIONARY_ATTRIBUTE, "custom-dictionary.dict");
    MorfologikFilterFactory factory = new MorfologikFilterFactory(params);
    factory.inform(loader);
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"lemma1", "lemma2"});
  }

  public void testMissingDictionary() throws Exception {
    final ResourceLoader loader = new ClasspathResourceLoader(TestMorfologikFilterFactory.class);

    IOException expected = expectThrows(IOException.class, () -> {
      Map<String,String> params = new HashMap<>();
      params.put(MorfologikFilterFactory.DICTIONARY_ATTRIBUTE, "missing-dictionary-resource.dict");
      MorfologikFilterFactory factory = new MorfologikFilterFactory(params);
      factory.inform(loader);
    });
    assertTrue(expected.getMessage().contains("Resource not found"));
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      HashMap<String,String> params = new HashMap<String,String>();
      params.put("bogusArg", "bogusValue");
      new MorfologikFilterFactory(params);
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
