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

import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

public class TestTypeAsSynonymFilterFactory extends BaseTokenStreamFactoryTestCase {

  private static final Token[] TOKENS =  { token("Visit", "<ALPHANUM>"), token("example.com", "<URL>") };

  public void testBasic() throws Exception {
    TokenStream stream = new CannedTokenStream(TOKENS);
    stream = tokenFilterFactory("TypeAsSynonym").create(stream);
    assertTokenStreamContents(stream, new String[] { "Visit", "<ALPHANUM>", "example.com", "<URL>" },
        null, null, new String[] { "<ALPHANUM>", "<ALPHANUM>", "<URL>", "<URL>" }, new int[] { 1, 0, 1, 0 });
  }

  public void testPrefix() throws Exception {
    TokenStream stream = new CannedTokenStream(TOKENS);
    stream = tokenFilterFactory("TypeAsSynonym", "prefix", "_type_").create(stream);
    assertTokenStreamContents(stream, new String[] { "Visit", "_type_<ALPHANUM>", "example.com", "_type_<URL>" },
        null, null, new String[] { "<ALPHANUM>", "<ALPHANUM>", "<URL>", "<URL>" }, new int[] { 1, 0, 1, 0 });
  }

  public void testIgnore() throws Exception {
    TokenStream stream = new CannedTokenStream(TOKENS);
    stream = tokenFilterFactory("typeAsSynonym", "prefix", "_type_","ignore", "<ALPHANUM>").create(stream);
    assertTokenStreamContents(stream, new String[] { "Visit", "example.com", "_type_<URL>" },
        null, null, new String[] { "<ALPHANUM>", "<URL>", "<URL>" }, new int[] { 1,  1, 0 });
  }

  private static Token token(String term, String type) {
    Token token = new Token();
    token.setEmpty();
    token.append(term);
    token.setType(type);
    return token;
  }
}
