package org.apache.lucene.analysis.miscellaneous;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;

public class TestLimitTokenCountFilterFactory extends BaseTokenStreamTestCase {

  public void test() throws IOException {
    LimitTokenCountFilterFactory factory = new LimitTokenCountFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put(LimitTokenCountFilterFactory.MAX_TOKEN_COUNT_KEY, "3");
    factory.init(args);
    String test = "A1 B2 C3 D4 E5 F6";
    MockTokenizer tok = new MockTokenizer(new StringReader(test), MockTokenizer.WHITESPACE, false);
    // LimitTokenCountFilter doesn't consume the entire stream that it wraps
    tok.setEnableChecks(false); 
    TokenStream stream = factory.create(tok);
    assertTokenStreamContents(stream, new String[] { "A1", "B2", "C3" });

    // param is required
    factory = new LimitTokenCountFilterFactory();
    args = new HashMap<String, String>();
    IllegalArgumentException iae = null;
    try {
      factory.init(args);
    } catch (IllegalArgumentException e) {
      assertTrue("exception doesn't mention param: " + e.getMessage(),
                 0 < e.getMessage().indexOf(LimitTokenCountFilterFactory.MAX_TOKEN_COUNT_KEY));
      iae = e;
    }
    assertNotNull("no exception thrown", iae);
  }
}
