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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;

/**
 * Test for {@link MorfologikFilterFactory}.
 */
public class TestMorfologikFilterFactory extends BaseTokenStreamTestCase {
  public void testCreateDictionary() throws Exception {
    StringReader reader = new StringReader("rowery bilety");
    Map<String,String> initParams = new HashMap<String,String>();
    initParams.put(MorfologikFilterFactory.DICTIONARY_SCHEMA_ATTRIBUTE, "morfologik");
    initParams.put("luceneMatchVersion", TEST_VERSION_CURRENT.toString());
    MorfologikFilterFactory factory = new MorfologikFilterFactory(initParams);
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"rower", "bilet"});
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new MorfologikFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
