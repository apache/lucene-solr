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
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Test that this filter removes tokens that match a particular set of flags.
 */
public class TestDropIfFlaggedFilter extends BaseTokenStreamTestCase {

  /**
   * Test the straight forward cases. When all flags match the token should be dropped
   */
  public void testDropped() throws Exception {

    Token token = new Token("foo", 0, 2);
    Token token2 = new Token("bar", 4, 6);
    Token token3 = new Token("baz", 8, 10);
    Token token4 = new Token("bam", 12, 14);

    token.setFlags(0); // 000 no flags match
    token2.setFlags(1);// 001 one flag matches
    token3.setFlags(2);// 010 no flags match
    token4.setFlags(7);// 111 both flags match (drop)

    TokenStream ts = new CannedTokenStream(token, token2, token3, token4);
    ts = new DropIfFlaggedFilter(ts, 5); // 101

    assertTokenStreamContents(ts, new String[]{
        "foo", "bar", "baz"}, new int[]{0, 4, 8}, new int[]{2, 6, 10}, new int[]{1, 1, 1});
  }

  /**
   * Test the difficult case where the first token is dropped. See comments in {@link DropIfFlaggedFilter} for details
   * of why this case is problematic.
   */
  public void testDroppedFirst() throws Exception {

    Token token = new Token("foo", 0, 2);
    Token token2 = new Token("bar", 4, 6);
    Token token3 = new Token("baz", 8, 10);
    Token token4 = new Token("bam", 12, 14);

    token.setFlags(4); // 100 flag matches (drop)
    token2.setFlags(1);// 001 no flags match
    token3.setFlags(2);// 010 no flags match
    token4.setFlags(7);// 111 flag matches (drop)

    TokenStream ts = new CannedTokenStream(token, token2, token3, token4);
    ts = new DropIfFlaggedFilter(ts, 4) ;

    assertTokenStreamContents(ts, new String[]{
         "bar", "baz"}, new int[]{ 4, 8}, new int[]{6, 10}, new int[]{2, 1});
  }

  /**
   * In this case lucene won't complain about the offset value (unless there was some other problem
   * that set the value to less than the prior token), so no worries
   */
  public void testFirstNotPos0() throws Exception {

    Token token = new Token("foo", 1, 3);
    Token token2 = new Token("bar", 5, 7);
    Token token3 = new Token("baz", 9, 11);
    Token token4 = new Token("bam", 13, 15);

    token.setFlags(4); // 100 flag matches (drop)
    token2.setFlags(1);// 001 no flags match
    token3.setFlags(2);// 010 no flags match
    token4.setFlags(7);// 111 flag matches (drop)


    TokenStream ts = new CannedTokenStream(token, token2, token3, token4);
    ts = new DropIfFlaggedFilter(ts, 4) ;

    assertTokenStreamContents(ts, new String[]{ "bar", "baz"},
        new int[]{ 5, 9}, new int[]{7, 11}, new int[]{2, 1});
  }
}
