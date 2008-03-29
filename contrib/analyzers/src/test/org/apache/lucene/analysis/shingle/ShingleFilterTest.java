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

import junit.framework.TestCase;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class ShingleFilterTest extends TestCase {

  public class TestTokenStream extends TokenStream {

    protected int index = 0;
    protected Token[] testToken;

    public TestTokenStream(Token[] testToken) {
      super();
      this.testToken = testToken;
    }

    public Token next() throws IOException {
      if (index < testToken.length) {
        return testToken[index++];
      } else {
        return null;
      }
    }
  }

  public static void main(String[] args) {
    junit.textui.TestRunner.run(ShingleFilterTest.class);
  }

  public static final Token[] TEST_TOKEN = new Token[] {
      new Token("please", 0, 6),
      new Token("divide", 7, 13),
      new Token("this", 14, 18),
      new Token("sentence", 19, 27),
      new Token("into", 28, 32),
      new Token("shingles", 33, 39),
  };

  public static Token[] testTokenWithHoles;

  public static final Token[] BI_GRAM_TOKENS = new Token[] {
    new Token("please", 0, 6),
    new Token("please divide", 0, 13),
    new Token("divide", 7, 13),
    new Token("divide this", 7, 18),
    new Token("this", 14, 18),
    new Token("this sentence", 14, 27),
    new Token("sentence", 19, 27),
    new Token("sentence into", 19, 32),
    new Token("into", 28, 32),
    new Token("into shingles", 28, 39),
    new Token("shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
  };

  public static final String[] BI_GRAM_TYPES = new String[] {
    "word", "shingle", "word", "shingle", "word", "shingle", "word",
    "shingle", "word", "shingle", "word"
  };

  public static final Token[] BI_GRAM_TOKENS_WITH_HOLES = new Token[] {
    new Token("please", 0, 6),
    new Token("please divide", 0, 13),
    new Token("divide", 7, 13),
    new Token("divide _", 7, 19),
    new Token("_", 19, 19),
    new Token("_ sentence", 19, 27),
    new Token("sentence", 19, 27),
    new Token("sentence _", 19, 33),
    new Token("_", 33, 33),
    new Token("_ shingles", 33, 39),
    new Token("shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_WITH_HOLES = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
  };

  public static final Token[] TRI_GRAM_TOKENS = new Token[] {
    new Token("please", 0, 6),
    new Token("please divide", 0, 13),
    new Token("please divide this", 0, 18),
    new Token("divide", 7, 13),
    new Token("divide this", 7, 18),
    new Token("divide this sentence", 7, 27),
    new Token("this", 14, 18),
    new Token("this sentence", 14, 27),
    new Token("this sentence into", 14, 32),
    new Token("sentence", 19, 27),
    new Token("sentence into", 19, 32),
    new Token("sentence into shingles", 19, 39),
    new Token("into", 28, 32),
    new Token("into shingles", 28, 39),
    new Token("shingles", 33, 39)
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS = new int[] {
    1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1
  };

  public static final String[] TRI_GRAM_TYPES = new String[] {
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle",
    "word"
  };


  protected void setUp() throws Exception {
    super.setUp();
    testTokenWithHoles = new Token[] {
      new Token("please", 0, 6),
      new Token("divide", 7, 13),
      new Token("sentence", 19, 27),
      new Token("shingles", 33, 39),
    };

    testTokenWithHoles[2].setPositionIncrement(2);
    testTokenWithHoles[3].setPositionIncrement(2);
  }

  /*
   * Class under test for void ShingleFilter(TokenStream, int)
   */
  public void testBiGramFilter() throws IOException {
    this.shingleFilterTest(2, TEST_TOKEN, BI_GRAM_TOKENS,
                           BI_GRAM_POSITION_INCREMENTS, BI_GRAM_TYPES);
  }

  public void testBiGramFilterWithHoles() throws IOException {
    this.shingleFilterTest(2, testTokenWithHoles, BI_GRAM_TOKENS_WITH_HOLES,
                           BI_GRAM_POSITION_INCREMENTS, BI_GRAM_TYPES);
  }

  public void testTriGramFilter() throws IOException {
    this.shingleFilterTest(3, TEST_TOKEN, TRI_GRAM_TOKENS,
                           TRI_GRAM_POSITION_INCREMENTS, TRI_GRAM_TYPES);
  }

  protected void shingleFilterTest(int n, Token[] testToken, Token[] tokens,
                                   int[] positionIncrements, String[] types)
    throws IOException {

    TokenStream filter = new ShingleFilter(new TestTokenStream(testToken), n);
    Token token;
    int i = 0;

    while ((token = filter.next()) != null) {
      String termText = new String(token.termBuffer(), 0, token.termLength());
      String goldText
        = new String(tokens[i].termBuffer(), 0, tokens[i].termLength());
      assertEquals("Wrong termText", goldText, termText);
      assertEquals("Wrong startOffset for token \"" + termText + "\"",
                   tokens[i].startOffset(), token.startOffset());
      assertEquals("Wrong endOffset for token \"" + termText + "\"",
                   tokens[i].endOffset(), token.endOffset());
      assertEquals("Wrong positionIncrement for token \"" + termText + "\"",
                   positionIncrements[i], token.getPositionIncrement());
      assertEquals("Wrong type for token \"" + termText + "\"",
                   types[i], token.type());
      i++;
    }
  }
}
