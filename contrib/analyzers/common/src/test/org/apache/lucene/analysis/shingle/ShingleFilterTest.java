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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.*;

public class ShingleFilterTest extends BaseTokenStreamTestCase {

  public class TestTokenStream extends TokenStream {

    protected int index = 0;
    protected Token[] testToken;
    
    private TermAttribute termAtt;
    private OffsetAttribute offsetAtt;
    private PositionIncrementAttribute posIncrAtt;
    private TypeAttribute typeAtt;

    public TestTokenStream(Token[] testToken) {
      super();
      this.testToken = testToken;
      this.termAtt = addAttribute(TermAttribute.class);
      this.offsetAtt = addAttribute(OffsetAttribute.class);
      this.posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      this.typeAtt = addAttribute(TypeAttribute.class);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      clearAttributes();
      if (index < testToken.length) {
        Token t = testToken[index++];
        termAtt.setTermBuffer(t.termBuffer(), 0, t.termLength());
        offsetAtt.setOffset(t.startOffset(), t.endOffset());
        posIncrAtt.setPositionIncrement(t.getPositionIncrement());
        typeAtt.setType(TypeAttributeImpl.DEFAULT_TYPE);
        return true;
      } else {
        return false;
      }
    }
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

  public static final Token[] BI_GRAM_TOKENS_WITHOUT_UNIGRAMS = new Token[] {
    createToken("please divide", 0, 13),
    createToken("divide this", 7, 18),
    createToken("this sentence", 14, 27),
    createToken("sentence into", 19, 32),
    createToken("into shingles", 28, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS = new int[] {
    1, 1, 1, 1, 1
  };

  public static final String[] BI_GRAM_TYPES_WITHOUT_UNIGRAMS = new String[] {
    "shingle", "shingle", "shingle", "shingle", "shingle"
  };

  public static final Token[] BI_GRAM_TOKENS_WITH_HOLES_WITHOUT_UNIGRAMS = new Token[] {
    createToken("please divide", 0, 13),
    createToken("divide _", 7, 19),
    createToken("_ sentence", 19, 27),
    createToken("sentence _", 19, 33),
    createToken("_ shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_WITH_HOLES_WITHOUT_UNIGRAMS = new int[] {
    1, 1, 1, 1, 1, 1
  };


  public static final Token[] TEST_SINGLE_TOKEN = new Token[] {
    createToken("please", 0, 6)
  };

  public static final Token[] SINGLE_TOKEN = new Token[] {
    createToken("please", 0, 6)
  };

  public static final int[] SINGLE_TOKEN_INCREMENTS = new int[] {
    1
  };

  public static final String[] SINGLE_TOKEN_TYPES = new String[] {
    "word"
  };

  public static final Token[] EMPTY_TOKEN_ARRAY = new Token[] {
  };

  public static final int[] EMPTY_TOKEN_INCREMENTS_ARRAY = new int[] {
  };

  public static final String[] EMPTY_TOKEN_TYPES_ARRAY = new String[] {
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


  @Override
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
                           BI_GRAM_POSITION_INCREMENTS, BI_GRAM_TYPES,
                           true);
  }

  public void testBiGramFilterWithHoles() throws IOException {
    this.shingleFilterTest(2, testTokenWithHoles, BI_GRAM_TOKENS_WITH_HOLES,
                           BI_GRAM_POSITION_INCREMENTS, BI_GRAM_TYPES,
                           true);
  }

  public void testBiGramFilterWithoutUnigrams() throws IOException {
    this.shingleFilterTest(2, TEST_TOKEN, BI_GRAM_TOKENS_WITHOUT_UNIGRAMS,
                           BI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS, BI_GRAM_TYPES_WITHOUT_UNIGRAMS,
                           false);
  }

  public void testBiGramFilterWithHolesWithoutUnigrams() throws IOException {
    this.shingleFilterTest(2, testTokenWithHoles, BI_GRAM_TOKENS_WITH_HOLES_WITHOUT_UNIGRAMS,
                           BI_GRAM_POSITION_INCREMENTS_WITH_HOLES_WITHOUT_UNIGRAMS, BI_GRAM_TYPES_WITHOUT_UNIGRAMS,
                           false);
  }

  public void testBiGramFilterWithSingleToken() throws IOException {
    this.shingleFilterTest(2, TEST_SINGLE_TOKEN, SINGLE_TOKEN,
                           SINGLE_TOKEN_INCREMENTS, SINGLE_TOKEN_TYPES,
                           true);
  }

  public void testBiGramFilterWithSingleTokenWithoutUnigrams() throws IOException {
    this.shingleFilterTest(2, TEST_SINGLE_TOKEN, EMPTY_TOKEN_ARRAY,
                           EMPTY_TOKEN_INCREMENTS_ARRAY, EMPTY_TOKEN_TYPES_ARRAY,
                           false);
  }

  public void testBiGramFilterWithEmptyTokenStream() throws IOException {
    this.shingleFilterTest(2, EMPTY_TOKEN_ARRAY, EMPTY_TOKEN_ARRAY,
                           EMPTY_TOKEN_INCREMENTS_ARRAY, EMPTY_TOKEN_TYPES_ARRAY,
                           true);
  }

  public void testBiGramFilterWithEmptyTokenStreamWithoutUnigrams() throws IOException {
    this.shingleFilterTest(2, EMPTY_TOKEN_ARRAY, EMPTY_TOKEN_ARRAY,
                           EMPTY_TOKEN_INCREMENTS_ARRAY, EMPTY_TOKEN_TYPES_ARRAY,
                           false);
  }

  public void testTriGramFilter() throws IOException {
    this.shingleFilterTest(3, TEST_TOKEN, TRI_GRAM_TOKENS,
                           TRI_GRAM_POSITION_INCREMENTS, TRI_GRAM_TYPES,
                           true);
  }


  
  public void testReset() throws Exception {
    Tokenizer wsTokenizer = new WhitespaceTokenizer(new StringReader("please divide this sentence"));
    TokenStream filter = new ShingleFilter(wsTokenizer, 2);
    assertTokenStreamContents(filter,
      new String[]{"please","please divide","divide","divide this","this","this sentence","sentence"},
      new int[]{0,0,7,7,14,14,19}, new int[]{6,13,13,18,18,27,27},
      new String[]{TypeAttributeImpl.DEFAULT_TYPE,"shingle",TypeAttributeImpl.DEFAULT_TYPE,"shingle",TypeAttributeImpl.DEFAULT_TYPE,"shingle",TypeAttributeImpl.DEFAULT_TYPE},
      new int[]{1,0,1,0,1,0,1}
    );
    wsTokenizer.reset(new StringReader("please divide this sentence"));
    assertTokenStreamContents(filter,
      new String[]{"please","please divide","divide","divide this","this","this sentence","sentence"},
      new int[]{0,0,7,7,14,14,19}, new int[]{6,13,13,18,18,27,27},
      new String[]{TypeAttributeImpl.DEFAULT_TYPE,"shingle",TypeAttributeImpl.DEFAULT_TYPE,"shingle",TypeAttributeImpl.DEFAULT_TYPE,"shingle",TypeAttributeImpl.DEFAULT_TYPE},
      new int[]{1,0,1,0,1,0,1}
    );
  }
  
  protected void shingleFilterTest(int maxSize, Token[] tokensToShingle, Token[] tokensToCompare,
                                   int[] positionIncrements, String[] types,
                                   boolean outputUnigrams)
    throws IOException {

    ShingleFilter filter = new ShingleFilter(new TestTokenStream(tokensToShingle), maxSize);
    filter.setOutputUnigrams(outputUnigrams);

    TermAttribute termAtt = filter.addAttribute(TermAttribute.class);
    OffsetAttribute offsetAtt = filter.addAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncrAtt = filter.addAttribute(PositionIncrementAttribute.class);
    TypeAttribute typeAtt = filter.addAttribute(TypeAttribute.class);

    int i = 0;
    while (filter.incrementToken()) {
      assertTrue("ShingleFilter outputted more tokens than expected", i < tokensToCompare.length);
      String termText = termAtt.term();
      String goldText = tokensToCompare[i].term();
      assertEquals("Wrong termText", goldText, termText);
      assertEquals("Wrong startOffset for token \"" + termText + "\"",
          tokensToCompare[i].startOffset(), offsetAtt.startOffset());
      assertEquals("Wrong endOffset for token \"" + termText + "\"",
          tokensToCompare[i].endOffset(), offsetAtt.endOffset());
      assertEquals("Wrong positionIncrement for token \"" + termText + "\"",
          positionIncrements[i], posIncrAtt.getPositionIncrement());
      assertEquals("Wrong type for token \"" + termText + "\"", types[i], typeAtt.type());
      i++;
    }
    assertEquals("ShingleFilter outputted wrong # of tokens. (# output = " + i + "; # expected =" + tokensToCompare.length + ")",
                 tokensToCompare.length, i);
  }

  private static Token createToken(String term, int start, int offset)
  {
    Token token = new Token(start, offset);
    token.setTermBuffer(term);
    return token;
  }
}
