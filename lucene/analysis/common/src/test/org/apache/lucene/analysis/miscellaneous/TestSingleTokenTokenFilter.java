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

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class TestSingleTokenTokenFilter extends LuceneTestCase {

  public void test() throws IOException {
    Token token = new Token();
    SingleTokenTokenStream ts = new SingleTokenTokenStream(token);
    AttributeImpl tokenAtt = (AttributeImpl) ts.addAttribute(CharTermAttribute.class);
    assertTrue(tokenAtt instanceof Token);
    ts.reset();

    assertTrue(ts.incrementToken());
    assertEquals(token, tokenAtt);
    assertFalse(ts.incrementToken());
    
    token = new Token("hallo", 10, 20);
    token.setType("someType");
    ts.setToken(token);
    ts.reset();

    assertTrue(ts.incrementToken());
    assertEquals(token, tokenAtt);
    assertFalse(ts.incrementToken());
  }
}
