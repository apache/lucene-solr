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
package org.apache.lucene.analysis.icu;


import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;

/** basic tests for {@link ICUNormalizer2FilterFactory} */
public class TestICUNormalizer2FilterFactory extends BaseTokenStreamTestCase {
  
  /** Test nfkc_cf defaults */
  public void testDefaults() throws Exception {
    Reader reader = new StringReader("This is a Ｔｅｓｔ");
    ICUNormalizer2FilterFactory factory = new ICUNormalizer2FilterFactory(new HashMap<String,String>());
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] { "this", "is", "a", "test" });
  }

  /** Test nfkc form */
  public void testFormArgument() throws Exception {
    Reader reader = new StringReader("This is a Ｔｅｓｔ");
    Map<String, String> args = new HashMap<>();
    args.put("form", "nfkc");
    ICUNormalizer2FilterFactory factory = new ICUNormalizer2FilterFactory(args);
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] { "This", "is", "a", "Test" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new ICUNormalizer2FilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
  
  // TODO: add tests for different forms
}
