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
package org.apache.lucene.analysis.phonetic;


import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

public class TestDaitchMokotoffSoundexFilterFactory extends BaseTokenStreamTestCase {

  public void testDefaults() throws Exception {
    DaitchMokotoffSoundexFilterFactory factory = new DaitchMokotoffSoundexFilterFactory(new HashMap<String, String>());
    Tokenizer inputStream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    inputStream.setReader(new StringReader("international"));

    TokenStream filteredStream = factory.create(inputStream);
    assertEquals(DaitchMokotoffSoundexFilter.class, filteredStream.getClass());
    assertTokenStreamContents(filteredStream, new String[] { "international", "063963" });
  }

  public void testSettingInject() throws Exception {
    Map<String,String> parameters = new HashMap<>();
    parameters.put("inject", "false");
    DaitchMokotoffSoundexFilterFactory factory = new DaitchMokotoffSoundexFilterFactory(parameters);

    Tokenizer inputStream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    inputStream.setReader(new StringReader("international"));

    TokenStream filteredStream = factory.create(inputStream);
    assertEquals(DaitchMokotoffSoundexFilter.class, filteredStream.getClass());
    assertTokenStreamContents(filteredStream, new String[] { "063963" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new DaitchMokotoffSoundexFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
