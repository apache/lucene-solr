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

/** Simple tests to ensure this factory is working */
public class TestRemoveDuplicatesTokenFilterFactory extends BaseTokenStreamFactoryTestCase {

  public static Token tok(int pos, String t, int start, int end) {
    Token tok = new Token(t,start,end);
    tok.setPositionIncrement(pos);
    return tok;
  }

  public void testDups(final String expected, final Token... tokens) throws Exception {
    TokenStream stream = new CannedTokenStream(tokens);
    stream = tokenFilterFactory("RemoveDuplicates").create(stream);
    assertTokenStreamContents(stream, expected.split("\\s"));   
  }
 
  public void testSimpleDups() throws Exception {
    testDups("A B C D E"
             ,tok(1,"A", 0,  4)
             ,tok(1,"B", 5, 10)
             ,tok(0,"B",11, 15)
             ,tok(1,"C",16, 20)
             ,tok(0,"D",16, 20)
             ,tok(1,"E",21, 25)
             ); 
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("RemoveDuplicates", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
