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

    public Token next(final Token reusableToken) throws IOException {
      assert reusableToken != null;
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
      createToken("please", 0, 6),
      createToken("divide", 7, 13),
      createToken("this", 14, 18),
      createToken("sentence", 19, 27),
      createToken("into", 28, 32),
      createToken("shingles", 33, 39),
  };

  public static Token[] testTokenWithHoles;

  public static final Token[] BI_GRAM_TOKENS = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide", 0, 13),
    createToken("divide", 7, 13),
    createToken("divide this", 7, 18),
    createToken("this", 14, 18),
    createToken("this sentence", 14, 27),
    createToken("sentence", 19, 27),
    createToken("sentence into", 19, 32),
    createToken("into", 28, 32),
    createToken("into shingles", 28, 39),
    createToken("shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
  };

  public static final String[] BI_GRAM_TYPES = new String[] {
    "word", "shingle", "word", "shingle", "word", "shingle", "word",
    "shingle", "word", "shingle", "word"
  };

  public static final Token[] BI_GRAM_TOKENS_WITH_HOLES = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide", 0, 13),
    createToken("divide", 7, 13),
    createToken("divide _", 7, 19),
    createToken("_", 19, 19),
    createToken("_ sentence", 19, 27),
    createToken("sentence", 19, 27),
    createToken("sentence _", 19, 33),
    createToken("_", 33, 33),
    createToken("_ shingles", 33, 39),
    createToken("shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_WITH_HOLES = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
  };

  public static final Token[] TRI_GRAM_TOKENS = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide", 0, 13),
    createToken("please divide this", 0, 18),
    createToken("divide", 7, 13),
    createToken("divide this", 7, 18),
    createToken("divide this sentence", 7, 27),
    createToken("this", 14, 18),
    createToken("this sentence", 14, 27),
    createToken("this sentence into", 14, 32),
    createToken("sentence", 19, 27),
    createToken("sentence into", 19, 32),
    createToken("sentence into shingles", 19, 39),
    createToken("into", 28, 32),
    createToken("into shingles", 28, 39),
    createToken("shingles", 33, 39)
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
      createToken("please", 0, 6),
      createToken("divide", 7, 13),
      createToken("sentence", 19, 27),
      createToken("shingles", 33, 39),
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

  protected void shingleFilterTest(int maxSize, Token[] tokensToShingle, Token[] tokensToCompare,
                                   int[] positionIncrements, String[] types)
    throws IOException {

    TokenStream filter = new ShingleFilter(new TestTokenStream(tokensToShingle), maxSize);
    int i = 0;
    final Token reusableToken = new Token();
    for (Token nextToken = filter.next(reusableToken); nextToken != null; nextToken = filter.next(reusableToken)) {
      String termText = nextToken.term();
      String goldText = tokensToCompare[i].term();
      assertEquals("Wrong termText", goldText, termText);
      assertEquals("Wrong startOffset for token \"" + termText + "\"",
          tokensToCompare[i].startOffset(), nextToken.startOffset());
      assertEquals("Wrong endOffset for token \"" + termText + "\"",
          tokensToCompare[i].endOffset(), nextToken.endOffset());
      assertEquals("Wrong positionIncrement for token \"" + termText + "\"",
          positionIncrements[i], nextToken.getPositionIncrement());
      assertEquals("Wrong type for token \"" + termText + "\"", types[i], nextToken.type());
      i++;
    }
  }

  private static Token createToken(String term, int start, int offset)
  {
    Token token = new Token(start, offset);
    token.setTermBuffer(term);
    return token;
  }
}
