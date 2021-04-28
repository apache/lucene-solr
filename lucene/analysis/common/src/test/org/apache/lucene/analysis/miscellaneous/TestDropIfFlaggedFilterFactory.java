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
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * This test just ensures the factory works, detailed tests in {@link TestDropIfFlaggedFilter}
 */
public class TestDropIfFlaggedFilterFactory extends BaseTokenStreamFactoryTestCase {

  private static final Token[] TOKENS =  { token("foo",1,0,2), token("bar",3, 4,6) };

  public void testFactory() throws Exception {
    TokenStream stream = new CannedTokenStream(TOKENS);
    TokenFilterFactory tokenFilterFactory = tokenFilterFactory("dropIfFlagged", "flags", "2");
    stream = tokenFilterFactory.create(stream);
    assertTokenStreamContents(stream, new String[] { "foo" }, null, null, new String[] { "word",}, new int[] { 1 });
  }

  private static Token token(String term, int flags, int soff, int eoff) {
    Token token = new Token();
    token.setEmpty();
    token.append(term);
    token.setFlags(flags);
    token.setOffset(soff,eoff);
    return token;
  }
}
