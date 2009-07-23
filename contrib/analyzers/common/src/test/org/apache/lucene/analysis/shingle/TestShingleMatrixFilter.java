package org.apache.lucene.analysis.shingle;

/**
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

import junit.framework.TestCase;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.PrefixAndSuffixAwareTokenFilter;
import org.apache.lucene.analysis.miscellaneous.SingleTokenTokenStream;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix.Column;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class TestShingleMatrixFilter extends TestCase {


  public void testBehavingAsShingleFilter() throws IOException {

    ShingleMatrixFilter.defaultSettingsCodec = null;

    TokenStream ts;

    ts = new ShingleMatrixFilter(new EmptyTokenStream(), 1, 2, new Character(' '), false, new ShingleMatrixFilter.OneDimensionalNonWeightedTokenSettingsCodec());
    assertNull(ts.next(new Token()));

    TokenListStream tls;
    LinkedList tokens;

    // test a plain old token stream with synonyms translated to rows.

    tokens = new LinkedList();
    tokens.add(createToken("please", 0, 6));
    tokens.add(createToken("divide", 7, 13));
    tokens.add(createToken("this", 14, 18));
    tokens.add(createToken("sentence", 19, 27));
    tokens.add(createToken("into", 28, 32));
    tokens.add(createToken("shingles", 33, 39));

    tls = new TokenListStream(tokens);

    // bi-grams

    ts = new ShingleMatrixFilter(tls, 1, 2, new Character(' '), false, new ShingleMatrixFilter.OneDimensionalNonWeightedTokenSettingsCodec());

    Token reusableToken = new Token();

    assertNext(ts, reusableToken, "please", 0, 6);
    assertNext(ts, reusableToken, "please divide", 0, 13);
    assertNext(ts, reusableToken, "divide", 7, 13);
    assertNext(ts, reusableToken, "divide this", 7, 18);
    assertNext(ts, reusableToken, "this", 14, 18);
    assertNext(ts, reusableToken, "this sentence", 14, 27);
    assertNext(ts, reusableToken, "sentence", 19, 27);
    assertNext(ts, reusableToken, "sentence into", 19, 32);
    assertNext(ts, reusableToken, "into", 28, 32);
    assertNext(ts, reusableToken, "into shingles", 28, 39);
    assertNext(ts, reusableToken, "shingles", 33, 39);


    assertNull(ts.next(reusableToken));

  }

  /**
   * Extracts a matrix from a token stream.
   * @throws IOException
   */
  public void testTokenStream() throws IOException {

    ShingleMatrixFilter.defaultSettingsCodec = null;//new ShingleMatrixFilter.SimpleThreeDimensionalTokenSettingsCodec();

    TokenStream ts;
    TokenListStream tls;
    LinkedList tokens;

    // test a plain old token stream with synonyms tranlated to rows.

    tokens = new LinkedList();
    tokens.add(tokenFactory("hello", 1, 0, 4));
    tokens.add(tokenFactory("greetings", 0, 0, 4));
    tokens.add(tokenFactory("world", 1, 5, 10));
    tokens.add(tokenFactory("earth", 0, 5, 10));
    tokens.add(tokenFactory("tellus", 0, 5, 10));

    tls = new TokenListStream(tokens);

    // bi-grams

    ts = new ShingleMatrixFilter(tls, 2, 2, new Character('_'), false, new ShingleMatrixFilter.TwoDimensionalNonWeightedSynonymTokenSettingsCodec());

    final Token reusableToken = new Token();
    assertNext(ts, reusableToken, "hello_world");
    assertNext(ts, reusableToken, "greetings_world");
    assertNext(ts, reusableToken, "hello_earth");
    assertNext(ts, reusableToken, "greetings_earth");
    assertNext(ts, reusableToken, "hello_tellus");
    assertNext(ts, reusableToken, "greetings_tellus");
    assertNull(ts.next(reusableToken));

    // bi-grams with no spacer character, start offset, end offset

    tls.reset();
    ts = new ShingleMatrixFilter(tls, 2, 2, null, false, new ShingleMatrixFilter.TwoDimensionalNonWeightedSynonymTokenSettingsCodec());
    assertNext(ts, reusableToken, "helloworld", 0, 10);
    assertNext(ts, reusableToken, "greetingsworld", 0, 10);
    assertNext(ts, reusableToken, "helloearth", 0, 10);
    assertNext(ts, reusableToken, "greetingsearth", 0, 10);
    assertNext(ts, reusableToken, "hellotellus", 0, 10);
    assertNext(ts, reusableToken, "greetingstellus", 0, 10);
    assertNull(ts.next(reusableToken));


    // add ^_prefix_and_suffix_$
    //
    // using 3d codec as it supports weights

    ShingleMatrixFilter.defaultSettingsCodec = new ShingleMatrixFilter.SimpleThreeDimensionalTokenSettingsCodec();

    tokens = new LinkedList();
    tokens.add(tokenFactory("hello", 1, 1f, 0, 4, ShingleMatrixFilter.TokenPositioner.newColumn));
    tokens.add(tokenFactory("greetings", 0, 1f, 0, 4, ShingleMatrixFilter.TokenPositioner.newRow));
    tokens.add(tokenFactory("world", 1, 1f, 5, 10, ShingleMatrixFilter.TokenPositioner.newColumn));
    tokens.add(tokenFactory("earth", 0, 1f, 5, 10, ShingleMatrixFilter.TokenPositioner.newRow));
    tokens.add(tokenFactory("tellus", 0, 1f, 5, 10, ShingleMatrixFilter.TokenPositioner.newRow));

    tls = new TokenListStream(tokens);

    ts = new PrefixAndSuffixAwareTokenFilter(new SingleTokenTokenStream(tokenFactory("^", 1, 100f, 0, 0)), tls, new SingleTokenTokenStream(tokenFactory("$", 1, 50f, 0, 0)));
    tls = new TokenListStream(ts);

    // bi-grams, position incrememnt, weight, start offset, end offset

    ts = new ShingleMatrixFilter(tls, 2, 2, new Character('_'), false);
//
//    for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    assertNext(ts, reusableToken, "^_hello", 1, 10.049875f, 0, 4);
    assertNext(ts, reusableToken, "^_greetings", 1, 10.049875f, 0, 4);
    assertNext(ts, reusableToken, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "world_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "earth_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "tellus_$", 1, 7.1414285f, 5, 10);
    assertNull(ts.next(reusableToken));

    // test unlimited size and allow single boundary token as shingle
    tls.reset();
    ts = new ShingleMatrixFilter(tls, 1, Integer.MAX_VALUE, new Character('_'), false);

//
//  for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    assertNext(ts, reusableToken, "^", 1, 10.0f, 0, 0);
    assertNext(ts, reusableToken, "^_hello", 1, 10.049875f, 0, 4);
    assertNext(ts, reusableToken, "^_hello_world", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "hello", 1, 1.0f, 0, 4);
    assertNext(ts, reusableToken, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "world", 1, 1.0f, 5, 10);
    assertNext(ts, reusableToken, "world_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "$", 1, 7.071068f, 10, 10);
    assertNext(ts, reusableToken, "^_greetings", 1, 10.049875f, 0, 4);
    assertNext(ts, reusableToken, "^_greetings_world", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_greetings_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "greetings", 1, 1.0f, 0, 4);
    assertNext(ts, reusableToken, "greetings_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "earth", 1, 1.0f, 5, 10);
    assertNext(ts, reusableToken, "earth_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "^_greetings_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_greetings_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "greetings_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_tellus_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "tellus", 1, 1.0f, 5, 10);
    assertNext(ts, reusableToken, "tellus_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "^_greetings_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_greetings_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "greetings_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_tellus_$", 1, 7.2111025f, 0, 10);

    assertNull(ts.next(reusableToken));

    // test unlimited size but don't allow single boundary token as shingle

    tls.reset();
    ts = new ShingleMatrixFilter(tls, 1, Integer.MAX_VALUE, new Character('_'), true);
//  for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    assertNext(ts, reusableToken, "^_hello", 1, 10.049875f, 0, 4);
    assertNext(ts, reusableToken, "^_hello_world", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "hello", 1, 1.0f, 0, 4);
    assertNext(ts, reusableToken, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "world", 1, 1.0f, 5, 10);
    assertNext(ts, reusableToken, "world_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "^_greetings", 1, 10.049875f, 0, 4);
    assertNext(ts, reusableToken, "^_greetings_world", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_greetings_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "greetings", 1, 1.0f, 0, 4);
    assertNext(ts, reusableToken, "greetings_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "earth", 1, 1.0f, 5, 10);
    assertNext(ts, reusableToken, "earth_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "^_greetings_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_greetings_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "greetings_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_hello_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_tellus_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, reusableToken, "tellus", 1, 1.0f, 5, 10);
    assertNext(ts, reusableToken, "tellus_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, reusableToken, "^_greetings_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, reusableToken, "^_greetings_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, reusableToken, "greetings_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_tellus_$", 1, 7.2111025f, 0, 10);


    assertNull(ts.next(reusableToken));

    System.currentTimeMillis();

    // multi-token synonyms
    //
    // Token[][][] {
    //    {{hello}, {greetings, and, salutations},
    //    {{world}, {earth}, {tellus}}
    // }
    //


    tokens = new LinkedList();
    tokens.add(tokenFactory("hello", 1, 1f, 0, 4, ShingleMatrixFilter.TokenPositioner.newColumn));
    tokens.add(tokenFactory("greetings", 1, 1f, 0, 4, ShingleMatrixFilter.TokenPositioner.newRow));
    tokens.add(tokenFactory("and", 1, 1f, 0, 4, ShingleMatrixFilter.TokenPositioner.sameRow));
    tokens.add(tokenFactory("salutations", 1, 1f, 0, 4, ShingleMatrixFilter.TokenPositioner.sameRow));
    tokens.add(tokenFactory("world", 1, 1f, 5, 10, ShingleMatrixFilter.TokenPositioner.newColumn));
    tokens.add(tokenFactory("earth", 1, 1f, 5, 10, ShingleMatrixFilter.TokenPositioner.newRow));
    tokens.add(tokenFactory("tellus", 1, 1f, 5, 10, ShingleMatrixFilter.TokenPositioner.newRow));

    tls = new TokenListStream(tokens);

    // 2-3 grams

    ts = new ShingleMatrixFilter(tls, 2, 3, new Character('_'), false);

//  for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    // shingle, position increment, weight, start offset, end offset

    assertNext(ts, reusableToken, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "greetings_and", 1, 1.4142135f, 0, 4);
    assertNext(ts, reusableToken, "greetings_and_salutations", 1, 1.7320508f, 0, 4);
    assertNext(ts, reusableToken, "and_salutations", 1, 1.4142135f, 0, 4);
    assertNext(ts, reusableToken, "and_salutations_world", 1, 1.7320508f, 0, 10);
    assertNext(ts, reusableToken, "salutations_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "and_salutations_earth", 1, 1.7320508f, 0, 10);
    assertNext(ts, reusableToken, "salutations_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, reusableToken, "and_salutations_tellus", 1, 1.7320508f, 0, 10);
    assertNext(ts, reusableToken, "salutations_tellus", 1, 1.4142135f, 0, 10);

    assertNull(ts.next(reusableToken));

    System.currentTimeMillis();


  }

  /**
   * Tests creat shingles from a pre-assembled matrix
   *
   * Tests the row token z-axis, multi token synonyms.
   *
   * @throws IOException
   */
  public void testMatrix() throws IOException {

    Matrix matrix = new Matrix();

    matrix.new Column(tokenFactory("no", 1));
    matrix.new Column(tokenFactory("surprise", 1));
    matrix.new Column(tokenFactory("to", 1));
    matrix.new Column(tokenFactory("see", 1));
    matrix.new Column(tokenFactory("england", 1));
    matrix.new Column(tokenFactory("manager", 1));

    Column col = matrix.new Column();

    // sven göran eriksson is a multi token synonym to svennis
    col.new Row().getTokens().add(tokenFactory("svennis", 1));

    Column.Row row = col.new Row();
    row.getTokens().add(tokenFactory("sven", 1));
    row.getTokens().add(tokenFactory("göran", 1));
    row.getTokens().add(tokenFactory("eriksson", 1));

    matrix.new Column(tokenFactory("in", 1));
    matrix.new Column(tokenFactory("the", 1));
    matrix.new Column(tokenFactory("croud", 1));

    TokenStream ts = new ShingleMatrixFilter(matrix, 2, 4, new Character('_'), true, new ShingleMatrixFilter.SimpleThreeDimensionalTokenSettingsCodec());

//  for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    final Token reusableToken = new Token();
    assertNext(ts, reusableToken, "no_surprise", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "no_surprise_to", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "no_surprise_to_see", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "surprise_to", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "surprise_to_see", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "surprise_to_see_england", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "to_see", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "to_see_england", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "to_see_england_manager", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "see_england", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "see_england_manager", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "see_england_manager_svennis", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "england_manager", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "england_manager_svennis", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "england_manager_svennis_in", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "manager_svennis", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "manager_svennis_in", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "manager_svennis_in_the", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "svennis_in", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "svennis_in_the", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "svennis_in_the_croud", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "in_the", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "in_the_croud", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "the_croud", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "see_england_manager_sven", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "england_manager_sven", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "england_manager_sven_göran", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "manager_sven", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "manager_sven_göran", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "manager_sven_göran_eriksson", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "sven_göran", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "sven_göran_eriksson", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "sven_göran_eriksson_in", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "göran_eriksson", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "göran_eriksson_in", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "göran_eriksson_in_the", 1, 2.0f, 0, 0);
    assertNext(ts, reusableToken, "eriksson_in", 1, 1.4142135f, 0, 0);
    assertNext(ts, reusableToken, "eriksson_in_the", 1, 1.7320508f, 0, 0);
    assertNext(ts, reusableToken, "eriksson_in_the_croud", 1, 2.0f, 0, 0);

    assertNull(ts.next(reusableToken));

  }

  private Token tokenFactory(String text, int startOffset, int endOffset) {
    return tokenFactory(text, 1, 1f, startOffset, endOffset);
  }


  private Token tokenFactory(String text, int posIncr, int startOffset, int endOffset) {
    Token token = new Token(startOffset, endOffset);
    token.setTermBuffer(text);
    token.setPositionIncrement(posIncr);
    return token;
  }


  private Token tokenFactory(String text, int posIncr) {
    return tokenFactory(text, posIncr, 1f, 0, 0);
  }

  private Token tokenFactory(String text, int posIncr, float weight) {
    return tokenFactory(text, posIncr, weight, 0, 0);
  }

  private Token tokenFactory(String text, int posIncr, float weight, int startOffset, int endOffset) {
    Token token = new Token(startOffset, endOffset);
    token.setTermBuffer(text);
    token.setPositionIncrement(posIncr);
    ShingleMatrixFilter.defaultSettingsCodec.setWeight(token, weight);
    return token;
  }

  private Token tokenFactory(String text, int posIncr, float weight, int startOffset, int endOffset, ShingleMatrixFilter.TokenPositioner positioner) {
    Token token = new Token(startOffset, endOffset);
    token.setTermBuffer(text);
    token.setPositionIncrement(posIncr);
    ShingleMatrixFilter.defaultSettingsCodec.setWeight(token, weight);
    ShingleMatrixFilter.defaultSettingsCodec.setTokenPositioner(token, positioner);
    return token;
  }

  // assert-methods start here

  private Token assertNext(TokenStream ts, final Token reusableToken, String text) throws IOException {
    Token nextToken = ts.next(reusableToken);
    assertNotNull(nextToken);
    assertEquals(text, nextToken.term());
    return nextToken;
  }

  private Token assertNext(TokenStream ts, final Token reusableToken, String text, int positionIncrement, float boost) throws IOException {
    Token nextToken = ts.next(reusableToken);
    assertNotNull(nextToken);
    assertEquals(text, nextToken.term());
    assertEquals(positionIncrement, nextToken.getPositionIncrement());
    assertEquals(boost, nextToken.getPayload() == null ? 1f : PayloadHelper.decodeFloat(nextToken.getPayload().getData()), 0);
    return nextToken;
  }

  private Token assertNext(TokenStream ts, final Token reusableToken, String text, int positionIncrement, float boost, int startOffset, int endOffset) throws IOException {
    Token nextToken = ts.next(reusableToken);
    assertNotNull(nextToken);
    assertEquals(text, nextToken.term());
    assertEquals(positionIncrement, nextToken.getPositionIncrement());
    assertEquals(boost, nextToken.getPayload() == null ? 1f : PayloadHelper.decodeFloat(nextToken.getPayload().getData()), 0);
    assertEquals(startOffset, nextToken.startOffset());
    assertEquals(endOffset, nextToken.endOffset());
    return nextToken;
  }

  private Token assertNext(TokenStream ts, final Token reusableToken, String text, int startOffset, int endOffset) throws IOException {
    Token nextToken = ts.next(reusableToken);
    assertNotNull(nextToken);
    assertEquals(text, nextToken.term());
    assertEquals(startOffset, nextToken.startOffset());
    assertEquals(endOffset, nextToken.endOffset());
    return nextToken;
  }

  private static Token createToken(String term, int start, int offset)
  {
    Token token = new Token(start, offset);
    token.setTermBuffer(term);
    return token;
  }


  public static class TokenListStream extends TokenStream {

    private Collection tokens;

    public TokenListStream(TokenStream ts) throws IOException {
      tokens = new ArrayList();
      final Token reusableToken = new Token();
      for (Token nextToken = ts.next(reusableToken); nextToken != null; nextToken = ts.next(reusableToken)) {
        tokens.add((Token) nextToken.clone());
      }
    }

    public TokenListStream(Collection tokens) {
      this.tokens = tokens;
    }

    private Iterator iterator;

    public Token next(final Token reusableToken) throws IOException {
      assert reusableToken != null;
      if (iterator == null) {
        iterator = tokens.iterator();
      }
      if (!iterator.hasNext()) {
        return null;
      }
      Token nextToken = (Token) iterator.next();
      return (Token) nextToken.clone();
    }


    public void reset() throws IOException {
      iterator = null;
    }
  }

}
