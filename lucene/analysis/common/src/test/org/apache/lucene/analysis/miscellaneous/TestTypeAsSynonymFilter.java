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

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Test that this filter moves the value in type to a synonym token with the same offsets. This is rarely
 * useful by itself, but in combination with another filter that updates the type value with an appropriate
 * synonym can be used to identify synonyms before tokens are modified by further analysis, and then
 * add them at the end, ensuring that the synonym value has not ben subjected to the intervening analysis.
 * This typically applies when the analysis would remove characters that should remain in the synonym.
 */
public class TestTypeAsSynonymFilter extends BaseTokenStreamTestCase {

  /**
   * Test the straight forward case with the simplest constructor. Simply converts every
   * type to a synonym. Typically one wants to also set an ignore list containing "word" unless
   * that default value is removed by prior analysis.
   */
  public void testSimple() throws Exception {

    Token token = new Token("foo", 0, 2);
    token.setType("bar");
    Token token2 = new Token("foo", 4, 6);
    TokenStream ts = new CannedTokenStream(token, token2);
    ts = new TypeAsSynonymFilter(ts);

    // "word" is the default type!
    assertTokenStreamContents(ts, new String[] {
        "foo", "bar","foo","word"},new int[] {0,0,4,4}, new int[]{2,2,6,6}, new int[] {1,0,1,0});
  }

  /**
   * Tests that we can add a prefix to the synonym (for example, to keep it from ever matching user input directly),
   * and test that we can ignore a list of type values we don't wish to turn into synonyms.
   */
  public void testWithPrefixAndIgnore() throws Exception {
    Token[] tokens = new Token[] {
        new Token("foo", 1, 3),
        new Token("foo", 5, 7),
        new Token("foo", 9, 11),
    } ;
    tokens[0].setType("bar");
    tokens[2].setType("ignoreme");
    TokenStream ts = new CannedTokenStream(tokens);
    ts = new TypeAsSynonymFilter(ts,"pfx_", Stream.of("word","ignoreme").collect(Collectors.toSet()), 0);

    assertTokenStreamContents(ts, new String[] {
        "foo", "pfx_bar","foo","foo"},new int[] {1,1,5,9}, new int[]{3,3,7,11}, new int[] {1,0,1,1});

  }

  /**
   * Analysis chains that make use of flags may or may not want flags transferred to the synonym to be
   * created. This tests the mask that can be used to control which flag bits are transferred.
   */
  public void testFlagMask() throws Exception {

    Token token = new Token("foo", 0, 2);
    token.setType("bar");
    token.setFlags(7);
    Token token2 = new Token("foo", 4, 6);
    TokenStream ts = new CannedTokenStream(token, token2);

    ts = new TypeAsSynonymFilter(ts,"", Collections.emptySet(), 5) ;

    // "word" is the default type!
    assertTokenStreamContents(ts, new String[] {
        "foo", "bar","foo","word"},new int[] {0,0,4,4}, new int[]{2,2,6,6},
        null, // not testing types
        null,null, //positions tested above
        // final values, keywords, graph, payloads not tested here
        null,null, null, false,null,
        new int[] {7,5,0,0}
        );
  }
}
