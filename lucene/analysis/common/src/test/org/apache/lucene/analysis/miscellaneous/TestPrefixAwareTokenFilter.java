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
package org.apache.lucene.analysis.miscellaneous;


import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;

import java.io.IOException;
import java.io.StringReader;

public class TestPrefixAwareTokenFilter extends BaseTokenStreamTestCase {

  public void test() throws IOException {

    PrefixAwareTokenFilter ts;

    ts = new PrefixAwareTokenFilter(
        new CannedTokenStream(createToken("a", 0, 1)),
        new CannedTokenStream(createToken("b", 0, 1)));
    assertTokenStreamContents(ts, 
        new String[] { "a", "b" },
        new int[] { 0, 1 },
        new int[] { 1, 2 });

    // prefix and suffix using 2x prefix

    final MockTokenizer suffix = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    suffix.setReader(new StringReader("hello world"));
    ts = new PrefixAwareTokenFilter(new CannedTokenStream(createToken("^", 0, 0)),
        suffix);
    ts = new PrefixAwareTokenFilter(ts, new CannedTokenStream(createToken("$", 0, 0)));

    assertTokenStreamContents(ts,
        new String[] { "^", "hello", "world", "$" },
        new int[] { 0, 0, 6, 11 },
        new int[] { 0, 5, 11, 11 });
  }

  private static Token createToken(String term, int start, int offset)
  {
    return new Token(term, start, offset);
  }
}
