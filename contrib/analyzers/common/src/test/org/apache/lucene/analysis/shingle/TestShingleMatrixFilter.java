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

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Arrays;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.miscellaneous.EmptyTokenStream;
import org.apache.lucene.analysis.miscellaneous.PrefixAndSuffixAwareTokenFilter;
import org.apache.lucene.analysis.miscellaneous.SingleTokenTokenStream;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter.Matrix.Column;
import org.apache.lucene.analysis.tokenattributes.*;

public class TestShingleMatrixFilter extends BaseTokenStreamTestCase {

  public TestShingleMatrixFilter(String name) {
    super(name);
  }

  public void testIterator() throws IOException {

    WhitespaceTokenizer wst = new WhitespaceTokenizer(new StringReader("one two three four five"));
    ShingleMatrixFilter smf = new ShingleMatrixFilter(wst, 2, 2, '_', false, new ShingleMatrixFilter.OneDimensionalNonWeightedTokenSettingsCodec());

    int i;
    for(i=0; smf.incrementToken(); i++);
    assertEquals(4, i);

    // call next once more. this should return false again rather than throwing an exception (LUCENE-1939)
    assertFalse(smf.incrementToken());

    System.currentTimeMillis();

  }

  public void testBehavingAsShingleFilter() throws IOException {

    ShingleMatrixFilter.defaultSettingsCodec = null;

    TokenStream ts;

    ts = new ShingleMatrixFilter(new EmptyTokenStream(), 1, 2, new Character(' '), false, new ShingleMatrixFilter.OneDimensionalNonWeightedTokenSettingsCodec());
    assertFalse(ts.incrementToken());

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


    assertNext(ts, "please", 0, 6);
    assertNext(ts, "please divide", 0, 13);
    assertNext(ts, "divide", 7, 13);
    assertNext(ts, "divide this", 7, 18);
    assertNext(ts, "this", 14, 18);
    assertNext(ts, "this sentence", 14, 27);
    assertNext(ts, "sentence", 19, 27);
    assertNext(ts, "sentence into", 19, 32);
    assertNext(ts, "into", 28, 32);
    assertNext(ts, "into shingles", 28, 39);
    assertNext(ts, "shingles", 33, 39);


    assertFalse(ts.incrementToken());

  }

  /**
   * Extracts a matrix from a token stream.
   * @throws IOException
   */
  public void testTokenStream() throws IOException {

    ShingleMatrixFilter.defaultSettingsCodec = null;//new ShingleMatrixFilter.SimpleThreeDimensionalTokenSettingsCodec();

    TokenStream ts;
    TokenStream tls;
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

    assertNext(ts, "hello_world");
    assertNext(ts, "greetings_world");
    assertNext(ts, "hello_earth");
    assertNext(ts, "greetings_earth");
    assertNext(ts, "hello_tellus");
    assertNext(ts, "greetings_tellus");
    assertFalse(ts.incrementToken());

    // bi-grams with no spacer character, start offset, end offset

    tls.reset();
    ts = new ShingleMatrixFilter(tls, 2, 2, null, false, new ShingleMatrixFilter.TwoDimensionalNonWeightedSynonymTokenSettingsCodec());
    assertNext(ts, "helloworld", 0, 10);
    assertNext(ts, "greetingsworld", 0, 10);
    assertNext(ts, "helloearth", 0, 10);
    assertNext(ts, "greetingsearth", 0, 10);
    assertNext(ts, "hellotellus", 0, 10);
    assertNext(ts, "greetingstellus", 0, 10);
    assertFalse(ts.incrementToken());


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
    tls = new CachingTokenFilter(ts);

    // bi-grams, position incrememnt, weight, start offset, end offset

    ts = new ShingleMatrixFilter(tls, 2, 2, new Character('_'), false);
//
//    for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    assertNext(ts, "^_hello", 1, 10.049875f, 0, 4);
    assertNext(ts, "^_greetings", 1, 10.049875f, 0, 4);
    assertNext(ts, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, "world_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "earth_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "tellus_$", 1, 7.1414285f, 5, 10);
    assertFalse(ts.incrementToken());

    // test unlimited size and allow single boundary token as shingle
    tls.reset();
    ts = new ShingleMatrixFilter(tls, 1, Integer.MAX_VALUE, new Character('_'), false);

//
//  for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    assertNext(ts, "^", 1, 10.0f, 0, 0);
    assertNext(ts, "^_hello", 1, 10.049875f, 0, 4);
    assertNext(ts, "^_hello_world", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_hello_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "hello", 1, 1.0f, 0, 4);
    assertNext(ts, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "world", 1, 1.0f, 5, 10);
    assertNext(ts, "world_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "$", 1, 7.071068f, 10, 10);
    assertNext(ts, "^_greetings", 1, 10.049875f, 0, 4);
    assertNext(ts, "^_greetings_world", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_greetings_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "greetings", 1, 1.0f, 0, 4);
    assertNext(ts, "greetings_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "^_hello_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_hello_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "earth", 1, 1.0f, 5, 10);
    assertNext(ts, "earth_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "^_greetings_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_greetings_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "greetings_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "^_hello_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_hello_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_tellus_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "tellus", 1, 1.0f, 5, 10);
    assertNext(ts, "tellus_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "^_greetings_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_greetings_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "greetings_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_tellus_$", 1, 7.2111025f, 0, 10);

    assertFalse(ts.incrementToken());

    // test unlimited size but don't allow single boundary token as shingle

    tls.reset();
    ts = new ShingleMatrixFilter(tls, 1, Integer.MAX_VALUE, new Character('_'), true);
//  for (Token token = ts.next(new Token()); token != null; token = ts.next(token)) {
//      System.out.println("assertNext(ts, \"" + token.term() + "\", " + token.getPositionIncrement() + ", " + (token.getPayload() == null ? "1.0" : PayloadHelper.decodeFloat(token.getPayload().getData())) + "f, " + token.startOffset() + ", " + token.endOffset() + ");");
//      token.clear();
//    }

    assertNext(ts, "^_hello", 1, 10.049875f, 0, 4);
    assertNext(ts, "^_hello_world", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_hello_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "hello", 1, 1.0f, 0, 4);
    assertNext(ts, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "world", 1, 1.0f, 5, 10);
    assertNext(ts, "world_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "^_greetings", 1, 10.049875f, 0, 4);
    assertNext(ts, "^_greetings_world", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_greetings_world_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "greetings", 1, 1.0f, 0, 4);
    assertNext(ts, "greetings_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_world_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "^_hello_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_hello_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "earth", 1, 1.0f, 5, 10);
    assertNext(ts, "earth_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "^_greetings_earth", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_greetings_earth_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "greetings_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_earth_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "^_hello_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_hello_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_tellus_$", 1, 7.2111025f, 0, 10);
    assertNext(ts, "tellus", 1, 1.0f, 5, 10);
    assertNext(ts, "tellus_$", 1, 7.1414285f, 5, 10);
    assertNext(ts, "^_greetings_tellus", 1, 10.099504f, 0, 10);
    assertNext(ts, "^_greetings_tellus_$", 1, 12.328828f, 0, 10);
    assertNext(ts, "greetings_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_tellus_$", 1, 7.2111025f, 0, 10);


    assertFalse(ts.incrementToken());

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

    assertNext(ts, "hello_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "greetings_and", 1, 1.4142135f, 0, 4);
    assertNext(ts, "greetings_and_salutations", 1, 1.7320508f, 0, 4);
    assertNext(ts, "and_salutations", 1, 1.4142135f, 0, 4);
    assertNext(ts, "and_salutations_world", 1, 1.7320508f, 0, 10);
    assertNext(ts, "salutations_world", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "and_salutations_earth", 1, 1.7320508f, 0, 10);
    assertNext(ts, "salutations_earth", 1, 1.4142135f, 0, 10);
    assertNext(ts, "hello_tellus", 1, 1.4142135f, 0, 10);
    assertNext(ts, "and_salutations_tellus", 1, 1.7320508f, 0, 10);
    assertNext(ts, "salutations_tellus", 1, 1.4142135f, 0, 10);

    assertFalse(ts.incrementToken());

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
    // some other tests set this to null.
    // set it here in case tests are run out of the usual order.
    ShingleMatrixFilter.defaultSettingsCodec = new ShingleMatrixFilter.SimpleThreeDimensionalTokenSettingsCodec();
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

    assertNext(ts, "no_surprise", 1, 1.4142135f, 0, 0);
    assertNext(ts, "no_surprise_to", 1, 1.7320508f, 0, 0);
    assertNext(ts, "no_surprise_to_see", 1, 2.0f, 0, 0);
    assertNext(ts, "surprise_to", 1, 1.4142135f, 0, 0);
    assertNext(ts, "surprise_to_see", 1, 1.7320508f, 0, 0);
    assertNext(ts, "surprise_to_see_england", 1, 2.0f, 0, 0);
    assertNext(ts, "to_see", 1, 1.4142135f, 0, 0);
    assertNext(ts, "to_see_england", 1, 1.7320508f, 0, 0);
    assertNext(ts, "to_see_england_manager", 1, 2.0f, 0, 0);
    assertNext(ts, "see_england", 1, 1.4142135f, 0, 0);
    assertNext(ts, "see_england_manager", 1, 1.7320508f, 0, 0);
    assertNext(ts, "see_england_manager_svennis", 1, 2.0f, 0, 0);
    assertNext(ts, "england_manager", 1, 1.4142135f, 0, 0);
    assertNext(ts, "england_manager_svennis", 1, 1.7320508f, 0, 0);
    assertNext(ts, "england_manager_svennis_in", 1, 2.0f, 0, 0);
    assertNext(ts, "manager_svennis", 1, 1.4142135f, 0, 0);
    assertNext(ts, "manager_svennis_in", 1, 1.7320508f, 0, 0);
    assertNext(ts, "manager_svennis_in_the", 1, 2.0f, 0, 0);
    assertNext(ts, "svennis_in", 1, 1.4142135f, 0, 0);
    assertNext(ts, "svennis_in_the", 1, 1.7320508f, 0, 0);
    assertNext(ts, "svennis_in_the_croud", 1, 2.0f, 0, 0);
    assertNext(ts, "in_the", 1, 1.4142135f, 0, 0);
    assertNext(ts, "in_the_croud", 1, 1.7320508f, 0, 0);
    assertNext(ts, "the_croud", 1, 1.4142135f, 0, 0);
    assertNext(ts, "see_england_manager_sven", 1, 2.0f, 0, 0);
    assertNext(ts, "england_manager_sven", 1, 1.7320508f, 0, 0);
    assertNext(ts, "england_manager_sven_göran", 1, 2.0f, 0, 0);
    assertNext(ts, "manager_sven", 1, 1.4142135f, 0, 0);
    assertNext(ts, "manager_sven_göran", 1, 1.7320508f, 0, 0);
    assertNext(ts, "manager_sven_göran_eriksson", 1, 2.0f, 0, 0);
    assertNext(ts, "sven_göran", 1, 1.4142135f, 0, 0);
    assertNext(ts, "sven_göran_eriksson", 1, 1.7320508f, 0, 0);
    assertNext(ts, "sven_göran_eriksson_in", 1, 2.0f, 0, 0);
    assertNext(ts, "göran_eriksson", 1, 1.4142135f, 0, 0);
    assertNext(ts, "göran_eriksson_in", 1, 1.7320508f, 0, 0);
    assertNext(ts, "göran_eriksson_in_the", 1, 2.0f, 0, 0);
    assertNext(ts, "eriksson_in", 1, 1.4142135f, 0, 0);
    assertNext(ts, "eriksson_in_the", 1, 1.7320508f, 0, 0);
    assertNext(ts, "eriksson_in_the_croud", 1, 2.0f, 0, 0);

    assertFalse(ts.incrementToken());

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

  private void assertNext(TokenStream ts, String text) throws IOException {
    TermAttribute termAtt = ts.addAttribute(TermAttribute.class);

    assertTrue(ts.incrementToken());
    assertEquals(text, termAtt.term());
  }

  private void assertNext(TokenStream ts, String text, int positionIncrement, float boost) throws IOException {
    TermAttribute termAtt = ts.addAttribute(TermAttribute.class);
    PositionIncrementAttribute posIncrAtt = ts.addAttribute(PositionIncrementAttribute.class);
    PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);

    assertTrue(ts.incrementToken());
    assertEquals(text, termAtt.term());
    assertEquals(positionIncrement, posIncrAtt.getPositionIncrement());
    assertEquals(boost, payloadAtt.getPayload() == null ? 1f : PayloadHelper.decodeFloat(payloadAtt.getPayload().getData()), 0);
  }

  private void assertNext(TokenStream ts, String text, int positionIncrement, float boost, int startOffset, int endOffset) throws IOException {
    TermAttribute termAtt = ts.addAttribute(TermAttribute.class);
    PositionIncrementAttribute posIncrAtt = ts.addAttribute(PositionIncrementAttribute.class);
    PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);
    OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
    
    assertTrue(ts.incrementToken());
    assertEquals(text, termAtt.term());
    assertEquals(positionIncrement, posIncrAtt.getPositionIncrement());
    assertEquals(boost, payloadAtt.getPayload() == null ? 1f : PayloadHelper.decodeFloat(payloadAtt.getPayload().getData()), 0);
    assertEquals(startOffset, offsetAtt.startOffset());
    assertEquals(endOffset, offsetAtt.endOffset());
  }
  
  private void assertNext(TokenStream ts, String text, int startOffset, int endOffset) throws IOException {
    TermAttribute termAtt = ts.addAttribute(TermAttribute.class);
    OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);

    assertTrue(ts.incrementToken());
    assertEquals(text, termAtt.term());
    assertEquals(startOffset, offsetAtt.startOffset());
    assertEquals(endOffset, offsetAtt.endOffset());
  }

  private static Token createToken(String term, int start, int offset)
  {
    Token token = new Token(start, offset);
    token.setTermBuffer(term);
    return token;
  }


  public static class TokenListStream extends TokenStream {

    private Collection tokens;
    TermAttribute termAtt;
    PositionIncrementAttribute posIncrAtt;
    PayloadAttribute payloadAtt;
    OffsetAttribute offsetAtt;
    TypeAttribute typeAtt;
    FlagsAttribute flagsAtt;
    
    public TokenListStream(Collection tokens) {
      this.tokens = tokens;
      termAtt = addAttribute(TermAttribute.class);
      posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      payloadAtt = addAttribute(PayloadAttribute.class);
      offsetAtt = addAttribute(OffsetAttribute.class);
      typeAtt = addAttribute(TypeAttribute.class);
      flagsAtt = addAttribute(FlagsAttribute.class);
    }

    private Iterator iterator;

    @Override
    public boolean incrementToken() throws IOException {
      if (iterator == null) {
        iterator = tokens.iterator();
      }
      if (!iterator.hasNext()) {
        return false;
      }
      Token prototype = (Token) iterator.next();
      termAtt.setTermBuffer(prototype.termBuffer(), 0, prototype.termLength());
      posIncrAtt.setPositionIncrement(prototype.getPositionIncrement());
      flagsAtt.setFlags(prototype.getFlags());
      offsetAtt.setOffset(prototype.startOffset(), prototype.endOffset());
      typeAtt.setType(prototype.type());
      payloadAtt.setPayload(prototype.getPayload());

      return true;
    }


    @Override
    public void reset() throws IOException {
      iterator = null;
    }
  }

}
