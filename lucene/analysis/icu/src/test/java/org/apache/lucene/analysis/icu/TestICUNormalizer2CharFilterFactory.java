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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;

/** basic tests for {@link ICUNormalizer2CharFilterFactory} */
public class TestICUNormalizer2CharFilterFactory extends BaseTokenStreamTestCase {
  
  /** Test nfkc_cf defaults */
  public void testDefaults() throws Exception {
    Reader reader = new StringReader("This is a Ｔｅｓｔ");
    ICUNormalizer2CharFilterFactory factory = new ICUNormalizer2CharFilterFactory(new HashMap<String,String>());
    reader = factory.create(reader);
    TokenStream stream = whitespaceMockTokenizer(reader);
    assertTokenStreamContents(stream, new String[] { "this", "is", "a", "test" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new ICUNormalizer2CharFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
  
  // TODO: add tests for different forms
}
