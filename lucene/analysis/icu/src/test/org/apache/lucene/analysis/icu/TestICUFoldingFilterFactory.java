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

/** basic tests for {@link ICUFoldingFilterFactory} */
public class TestICUFoldingFilterFactory extends BaseTokenStreamTestCase {

  /** basic tests to ensure the folding is working */
  public void test() throws Exception {
    Reader reader = new StringReader("Résumé");
    ICUFoldingFilterFactory factory = new ICUFoldingFilterFactory(new HashMap<String, String>());
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"resume"});
  }

  /** test to ensure the filter parameter is working */
  public void testFilter() throws Exception {
    HashMap<String, String> args = new HashMap<String, String>();
    args.put("filter", "[^ö]");
    ICUFoldingFilterFactory factory = new ICUFoldingFilterFactory(args);

    Reader reader = new StringReader("Résumé");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"resume"});

    reader = new StringReader("Fönster");
    stream = whitespaceMockTokenizer(reader);
    stream = factory.create(stream);
    assertTokenStreamContents(stream, new String[] {"fönster"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new ICUFoldingFilterFactory(
                  new HashMap<String, String>() {
                    {
                      put("bogusArg", "bogusValue");
                    }
                  });
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
