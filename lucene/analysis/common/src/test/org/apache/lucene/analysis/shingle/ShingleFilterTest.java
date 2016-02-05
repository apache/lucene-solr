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
package org.apache.lucene.analysis.shingle;


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.*;

public class ShingleFilterTest extends BaseTokenStreamTestCase {

  public static final Token[] TEST_TOKEN = new Token[] {
      createToken("please", 0, 6),
      createToken("divide", 7, 13),
      createToken("this", 14, 18),
      createToken("sentence", 19, 27),
      createToken("into", 28, 32),
      createToken("shingles", 33, 39),
  };

  public static final int[] UNIGRAM_ONLY_POSITION_INCREMENTS = new int[] {
    1, 1, 1, 1, 1, 1
  };

  public static final String[] UNIGRAM_ONLY_TYPES = new String[] {
    "word", "word", "word", "word", "word", "word"
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
    createToken("_ sentence", 19, 27),
    createToken("sentence", 19, 27),
    createToken("sentence _", 19, 33),
    createToken("_ shingles", 33, 39),
    createToken("shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_WITH_HOLES = new int[] {
    1, 0, 1, 0, 1, 1, 0, 1, 1
  };

  private static final String[] BI_GRAM_TYPES_WITH_HOLES = {
    "word", "shingle", 
    "word", "shingle", "shingle", "word", "shingle", "shingle", "word"
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
  
  public static final Token[] TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS = new Token[] {
    createToken("please divide", 0, 13),
    createToken("please divide this", 0, 18),
    createToken("divide this", 7, 18),
    createToken("divide this sentence", 7, 27),
    createToken("this sentence", 14, 27),
    createToken("this sentence into", 14, 32),
    createToken("sentence into", 19, 32),
    createToken("sentence into shingles", 19, 39),
    createToken("into shingles", 28, 39),
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1
  };
  
  public static final String[] TRI_GRAM_TYPES_WITHOUT_UNIGRAMS = new String[] {
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle",
  };
  
  public static final Token[] FOUR_GRAM_TOKENS = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide", 0, 13),
    createToken("please divide this", 0, 18),
    createToken("please divide this sentence", 0, 27),
    createToken("divide", 7, 13),
    createToken("divide this", 7, 18),
    createToken("divide this sentence", 7, 27),
    createToken("divide this sentence into", 7, 32),
    createToken("this", 14, 18),
    createToken("this sentence", 14, 27),
    createToken("this sentence into", 14, 32),
    createToken("this sentence into shingles", 14, 39),
    createToken("sentence", 19, 27),
    createToken("sentence into", 19, 32),
    createToken("sentence into shingles", 19, 39),
    createToken("into", 28, 32),
    createToken("into shingles", 28, 39),
    createToken("shingles", 33, 39)
  };

  public static final int[] FOUR_GRAM_POSITION_INCREMENTS = new int[] {
    1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 1
  };

  public static final String[] FOUR_GRAM_TYPES = new String[] {
    "word", "shingle", "shingle", "shingle",
    "word", "shingle", "shingle", "shingle",
    "word", "shingle", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle",
    "word"
  };
  
  public static final Token[] FOUR_GRAM_TOKENS_WITHOUT_UNIGRAMS = new Token[] {
    createToken("please divide", 0, 13),
    createToken("please divide this", 0, 18),
    createToken("please divide this sentence", 0, 27),
    createToken("divide this", 7, 18),
    createToken("divide this sentence", 7, 27),
    createToken("divide this sentence into", 7, 32),
    createToken("this sentence", 14, 27),
    createToken("this sentence into", 14, 32),
    createToken("this sentence into shingles", 14, 39),
    createToken("sentence into", 19, 32),
    createToken("sentence into shingles", 19, 39),
    createToken("into shingles", 28, 39),
  };

  public static final int[] FOUR_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS = new int[] {
    1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1
  };
  
  public static final String[] FOUR_GRAM_TYPES_WITHOUT_UNIGRAMS = new String[] {
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",

  };

  public static final Token[] TRI_GRAM_TOKENS_MIN_TRI_GRAM = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide this", 0, 18),
    createToken("divide", 7, 13),
    createToken("divide this sentence", 7, 27),
    createToken("this", 14, 18),
    createToken("this sentence into", 14, 32),
    createToken("sentence", 19, 27),
    createToken("sentence into shingles", 19, 39),
    createToken("into", 28, 32),
    createToken("shingles", 33, 39)
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_MIN_TRI_GRAM = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1, 1
  };

  public static final String[] TRI_GRAM_TYPES_MIN_TRI_GRAM = new String[] {
    "word", "shingle",
    "word", "shingle",
    "word", "shingle",
    "word", "shingle",
    "word",
    "word"
  };
  
  public static final Token[] TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM = new Token[] {
    createToken("please divide this", 0, 18),
    createToken("divide this sentence", 7, 27),
    createToken("this sentence into", 14, 32),
    createToken("sentence into shingles", 19, 39)
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM = new int[] {
    1, 1, 1, 1
  };
  
  public static final String[] TRI_GRAM_TYPES_WITHOUT_UNIGRAMS_MIN_TRI_GRAM = new String[] {
    "shingle",
    "shingle",
    "shingle",
    "shingle"
  };
  
  public static final Token[] FOUR_GRAM_TOKENS_MIN_TRI_GRAM = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide this", 0, 18),
    createToken("please divide this sentence", 0, 27),
    createToken("divide", 7, 13),
    createToken("divide this sentence", 7, 27),
    createToken("divide this sentence into", 7, 32),
    createToken("this", 14, 18),
    createToken("this sentence into", 14, 32),
    createToken("this sentence into shingles", 14, 39),
    createToken("sentence", 19, 27),
    createToken("sentence into shingles", 19, 39),
    createToken("into", 28, 32),
    createToken("shingles", 33, 39)
  };

  public static final int[] FOUR_GRAM_POSITION_INCREMENTS_MIN_TRI_GRAM = new int[] {
    1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 1
  };

  public static final String[] FOUR_GRAM_TYPES_MIN_TRI_GRAM = new String[] {
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle",
    "word",
    "word"
  };
  
  public static final Token[] FOUR_GRAM_TOKENS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM = new Token[] {
    createToken("please divide this", 0, 18),
    createToken("please divide this sentence", 0, 27),
    createToken("divide this sentence", 7, 27),
    createToken("divide this sentence into", 7, 32),
    createToken("this sentence into", 14, 32),
    createToken("this sentence into shingles", 14, 39),
    createToken("sentence into shingles", 19, 39),
  };

  public static final int[] FOUR_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM = new int[] {
    1, 0, 1, 0, 1, 0, 1
  };
  
  public static final String[] FOUR_GRAM_TYPES_WITHOUT_UNIGRAMS_MIN_TRI_GRAM = new String[] {
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle"
  };
  
  public static final Token[] FOUR_GRAM_TOKENS_MIN_FOUR_GRAM = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide this sentence", 0, 27),
    createToken("divide", 7, 13),
    createToken("divide this sentence into", 7, 32),
    createToken("this", 14, 18),
    createToken("this sentence into shingles", 14, 39),
    createToken("sentence", 19, 27),
    createToken("into", 28, 32),
    createToken("shingles", 33, 39)
  };

  public static final int[] FOUR_GRAM_POSITION_INCREMENTS_MIN_FOUR_GRAM = new int[] {
    1, 0, 1, 0, 1, 0, 1, 1, 1
  };

  public static final String[] FOUR_GRAM_TYPES_MIN_FOUR_GRAM = new String[] {
    "word", "shingle",
    "word", "shingle",
    "word", "shingle",
    "word",
    "word",
    "word"
  };
  
  public static final Token[] FOUR_GRAM_TOKENS_WITHOUT_UNIGRAMS_MIN_FOUR_GRAM = new Token[] {
    createToken("please divide this sentence", 0, 27),
    createToken("divide this sentence into", 7, 32),
    createToken("this sentence into shingles", 14, 39),
  };

  public static final int[] FOUR_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_MIN_FOUR_GRAM = new int[] {
    1, 1, 1
  };
  
  public static final String[] FOUR_GRAM_TYPES_WITHOUT_UNIGRAMS_MIN_FOUR_GRAM = new String[] {
    "shingle",
    "shingle",
    "shingle"
  };

  public static final Token[] BI_GRAM_TOKENS_NO_SEPARATOR = new Token[] {
    createToken("please", 0, 6),
    createToken("pleasedivide", 0, 13),
    createToken("divide", 7, 13),
    createToken("dividethis", 7, 18),
    createToken("this", 14, 18),
    createToken("thissentence", 14, 27),
    createToken("sentence", 19, 27),
    createToken("sentenceinto", 19, 32),
    createToken("into", 28, 32),
    createToken("intoshingles", 28, 39),
    createToken("shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_NO_SEPARATOR = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
  };

  public static final String[] BI_GRAM_TYPES_NO_SEPARATOR = new String[] {
    "word", "shingle", "word", "shingle", "word", "shingle", "word",
    "shingle", "word", "shingle", "word"
  };

  public static final Token[] BI_GRAM_TOKENS_WITHOUT_UNIGRAMS_NO_SEPARATOR = new Token[] {
    createToken("pleasedivide", 0, 13),
    createToken("dividethis", 7, 18),
    createToken("thissentence", 14, 27),
    createToken("sentenceinto", 19, 32),
    createToken("intoshingles", 28, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_NO_SEPARATOR = new int[] {
    1, 1, 1, 1, 1
  };

  public static final String[] BI_GRAM_TYPES_WITHOUT_UNIGRAMS_NO_SEPARATOR = new String[] {
    "shingle", "shingle", "shingle", "shingle", "shingle"
  };
  
  public static final Token[] TRI_GRAM_TOKENS_NO_SEPARATOR = new Token[] {
    createToken("please", 0, 6),
    createToken("pleasedivide", 0, 13),
    createToken("pleasedividethis", 0, 18),
    createToken("divide", 7, 13),
    createToken("dividethis", 7, 18),
    createToken("dividethissentence", 7, 27),
    createToken("this", 14, 18),
    createToken("thissentence", 14, 27),
    createToken("thissentenceinto", 14, 32),
    createToken("sentence", 19, 27),
    createToken("sentenceinto", 19, 32),
    createToken("sentenceintoshingles", 19, 39),
    createToken("into", 28, 32),
    createToken("intoshingles", 28, 39),
    createToken("shingles", 33, 39)
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_NO_SEPARATOR = new int[] {
    1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1
  };

  public static final String[] TRI_GRAM_TYPES_NO_SEPARATOR = new String[] {
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle",
    "word"
  };
  
  public static final Token[] TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS_NO_SEPARATOR = new Token[] {
    createToken("pleasedivide", 0, 13),
    createToken("pleasedividethis", 0, 18),
    createToken("dividethis", 7, 18),
    createToken("dividethissentence", 7, 27),
    createToken("thissentence", 14, 27),
    createToken("thissentenceinto", 14, 32),
    createToken("sentenceinto", 19, 32),
    createToken("sentenceintoshingles", 19, 39),
    createToken("intoshingles", 28, 39),
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_NO_SEPARATOR = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1
  };
  
  public static final String[] TRI_GRAM_TYPES_WITHOUT_UNIGRAMS_NO_SEPARATOR = new String[] {
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle",
  };

  public static final Token[] BI_GRAM_TOKENS_ALT_SEPARATOR = new Token[] {
    createToken("please", 0, 6),
    createToken("please<SEP>divide", 0, 13),
    createToken("divide", 7, 13),
    createToken("divide<SEP>this", 7, 18),
    createToken("this", 14, 18),
    createToken("this<SEP>sentence", 14, 27),
    createToken("sentence", 19, 27),
    createToken("sentence<SEP>into", 19, 32),
    createToken("into", 28, 32),
    createToken("into<SEP>shingles", 28, 39),
    createToken("shingles", 33, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_ALT_SEPARATOR = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
  };

  public static final String[] BI_GRAM_TYPES_ALT_SEPARATOR = new String[] {
    "word", "shingle", "word", "shingle", "word", "shingle", "word",
    "shingle", "word", "shingle", "word"
  };

  public static final Token[] BI_GRAM_TOKENS_WITHOUT_UNIGRAMS_ALT_SEPARATOR = new Token[] {
    createToken("please<SEP>divide", 0, 13),
    createToken("divide<SEP>this", 7, 18),
    createToken("this<SEP>sentence", 14, 27),
    createToken("sentence<SEP>into", 19, 32),
    createToken("into<SEP>shingles", 28, 39),
  };

  public static final int[] BI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_ALT_SEPARATOR = new int[] {
    1, 1, 1, 1, 1
  };

  public static final String[] BI_GRAM_TYPES_WITHOUT_UNIGRAMS_ALT_SEPARATOR = new String[] {
    "shingle", "shingle", "shingle", "shingle", "shingle"
  };
  
  public static final Token[] TRI_GRAM_TOKENS_ALT_SEPARATOR = new Token[] {
    createToken("please", 0, 6),
    createToken("please<SEP>divide", 0, 13),
    createToken("please<SEP>divide<SEP>this", 0, 18),
    createToken("divide", 7, 13),
    createToken("divide<SEP>this", 7, 18),
    createToken("divide<SEP>this<SEP>sentence", 7, 27),
    createToken("this", 14, 18),
    createToken("this<SEP>sentence", 14, 27),
    createToken("this<SEP>sentence<SEP>into", 14, 32),
    createToken("sentence", 19, 27),
    createToken("sentence<SEP>into", 19, 32),
    createToken("sentence<SEP>into<SEP>shingles", 19, 39),
    createToken("into", 28, 32),
    createToken("into<SEP>shingles", 28, 39),
    createToken("shingles", 33, 39)
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_ALT_SEPARATOR = new int[] {
    1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1
  };

  public static final String[] TRI_GRAM_TYPES_ALT_SEPARATOR = new String[] {
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle",
    "word"
  };
  
  public static final Token[] TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS_ALT_SEPARATOR = new Token[] {
    createToken("please<SEP>divide", 0, 13),
    createToken("please<SEP>divide<SEP>this", 0, 18),
    createToken("divide<SEP>this", 7, 18),
    createToken("divide<SEP>this<SEP>sentence", 7, 27),
    createToken("this<SEP>sentence", 14, 27),
    createToken("this<SEP>sentence<SEP>into", 14, 32),
    createToken("sentence<SEP>into", 19, 32),
    createToken("sentence<SEP>into<SEP>shingles", 19, 39),
    createToken("into<SEP>shingles", 28, 39),
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_ALT_SEPARATOR = new int[] {
    1, 0, 1, 0, 1, 0, 1, 0, 1
  };
  
  public static final String[] TRI_GRAM_TYPES_WITHOUT_UNIGRAMS_ALT_SEPARATOR = new String[] {
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle",
  };

  public static final Token[] TRI_GRAM_TOKENS_NULL_SEPARATOR = new Token[] {
    createToken("please", 0, 6),
    createToken("pleasedivide", 0, 13),
    createToken("pleasedividethis", 0, 18),
    createToken("divide", 7, 13),
    createToken("dividethis", 7, 18),
    createToken("dividethissentence", 7, 27),
    createToken("this", 14, 18),
    createToken("thissentence", 14, 27),
    createToken("thissentenceinto", 14, 32),
    createToken("sentence", 19, 27),
    createToken("sentenceinto", 19, 32),
    createToken("sentenceintoshingles", 19, 39),
    createToken("into", 28, 32),
    createToken("intoshingles", 28, 39),
    createToken("shingles", 33, 39)
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_NULL_SEPARATOR = new int[] {
    1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1
  };

  public static final String[] TRI_GRAM_TYPES_NULL_SEPARATOR = new String[] {
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle",
    "word"
  };
  
  public static final Token[] TEST_TOKEN_POS_INCR_EQUAL_TO_N = new Token[] {
    createToken("please", 0, 6),
    createToken("divide", 7, 13),
    createToken("this", 14, 18),
    createToken("sentence", 29, 37, 3),
    createToken("into", 38, 42),
    createToken("shingles", 43, 49),
  };

  public static final Token[] TRI_GRAM_TOKENS_POS_INCR_EQUAL_TO_N = new Token[] {
    createToken("please", 0, 6),
    createToken("please divide", 0, 13),
    createToken("please divide this", 0, 18),
    createToken("divide", 7, 13),
    createToken("divide this", 7, 18),
    createToken("divide this _", 7, 29),
    createToken("this", 14, 18),
    createToken("this _", 14, 29),
    createToken("this _ _", 14, 29),
    createToken("_ _ sentence", 29, 37),
    createToken("_ sentence", 29, 37),
    createToken("_ sentence into", 29, 42),
    createToken("sentence", 29, 37),
    createToken("sentence into", 29, 42),
    createToken("sentence into shingles", 29, 49),
    createToken("into", 38, 42),
    createToken("into shingles", 38, 49),
    createToken("shingles", 43, 49)
  };
  
  public static final int[] TRI_GRAM_POSITION_INCREMENTS_POS_INCR_EQUAL_TO_N = new int[] {
    1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1
  };
  
  public static final String[] TRI_GRAM_TYPES_POS_INCR_EQUAL_TO_N = new String[] {
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "shingle", "shingle", "shingle", "word", "shingle", "shingle",
    "word", "shingle",
    "word"
  };
  
  public static final Token[] TRI_GRAM_TOKENS_POS_INCR_EQUAL_TO_N_WITHOUT_UNIGRAMS = new Token[] {
    createToken("please divide", 0, 13),
    createToken("please divide this", 0, 18),
    createToken("divide this", 7, 18),
    createToken("divide this _", 7, 29),
    createToken("this _", 14, 29),
    createToken("this _ _", 14, 29),
    createToken("_ _ sentence", 29, 37),
    createToken("_ sentence", 29, 37),
    createToken("_ sentence into", 29, 42),
    createToken("sentence into", 29, 42),
    createToken("sentence into shingles", 29, 49),
    createToken("into shingles", 38, 49),
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_POS_INCR_EQUAL_TO_N_WITHOUT_UNIGRAMS = new int[] {
    1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 1
  };

  public static final String[] TRI_GRAM_TYPES_POS_INCR_EQUAL_TO_N_WITHOUT_UNIGRAMS = new String[] {
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle", "shingle",
    "shingle", "shingle",
    "shingle",
  };

  public static final Token[] TEST_TOKEN_POS_INCR_GREATER_THAN_N = new Token[] {
    createToken("please", 0, 6),
    createToken("divide", 57, 63, 8),
    createToken("this", 64, 68),
    createToken("sentence", 69, 77),
    createToken("into", 78, 82),
    createToken("shingles", 83, 89),
  };
  
  public static final Token[] TRI_GRAM_TOKENS_POS_INCR_GREATER_THAN_N = new Token[] {
    createToken("please", 0, 6),
    createToken("please _", 0, 57),
    createToken("please _ _", 0, 57),
    createToken("_ _ divide", 57, 63),
    createToken("_ divide", 57, 63),
    createToken("_ divide this", 57, 68),
    createToken("divide", 57, 63),
    createToken("divide this", 57, 68),
    createToken("divide this sentence", 57, 77),
    createToken("this", 64, 68),
    createToken("this sentence", 64, 77),
    createToken("this sentence into", 64, 82),
    createToken("sentence", 69, 77),
    createToken("sentence into", 69, 82),
    createToken("sentence into shingles", 69, 89),
    createToken("into", 78, 82),
    createToken("into shingles", 78, 89),
    createToken("shingles", 83, 89)
  };
  
  public static final int[] TRI_GRAM_POSITION_INCREMENTS_POS_INCR_GREATER_THAN_N = new int[] {
    1, 0, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1
  };
  public static final String[] TRI_GRAM_TYPES_POS_INCR_GREATER_THAN_N = new String[] {
    "word", "shingle", "shingle",
    "shingle",
    "shingle", "shingle", 
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle", "shingle",
    "word", "shingle",
    "word"
  };
  
  public static final Token[] TRI_GRAM_TOKENS_POS_INCR_GREATER_THAN_N_WITHOUT_UNIGRAMS = new Token[] {
    createToken("please _", 0, 57),
    createToken("please _ _", 0, 57),
    createToken("_ _ divide", 57, 63),
    createToken("_ divide", 57, 63),
    createToken("_ divide this", 57, 68),
    createToken("divide this", 57, 68),
    createToken("divide this sentence", 57, 77),
    createToken("this sentence", 64, 77),
    createToken("this sentence into", 64, 82),
    createToken("sentence into", 69, 82),
    createToken("sentence into shingles", 69, 89),
    createToken("into shingles", 78, 89),
  };

  public static final int[] TRI_GRAM_POSITION_INCREMENTS_POS_INCR_GREATER_THAN_N_WITHOUT_UNIGRAMS = new int[] {
    1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1
  };

  public static final String[] TRI_GRAM_TYPES_POS_INCR_GREATER_THAN_N_WITHOUT_UNIGRAMS = new String[] {
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle",
    "shingle", "shingle", "shingle", "shingle", "shingle",
    "shingle",
  };

  @Override
  public void setUp() throws Exception {
    super.setUp();
    testTokenWithHoles = new Token[] {
      createToken("please", 0, 6),
      createToken("divide", 7, 13),
      createToken("sentence", 19, 27, 2),
      createToken("shingles", 33, 39, 2),
    };
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
                           BI_GRAM_POSITION_INCREMENTS_WITH_HOLES, 
                           BI_GRAM_TYPES_WITH_HOLES, 
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
  
  public void testTriGramFilterWithoutUnigrams() throws IOException {
    this.shingleFilterTest(3, TEST_TOKEN, TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS,
                           TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS, TRI_GRAM_TYPES_WITHOUT_UNIGRAMS,
                           false);
  }
  
  public void testFourGramFilter() throws IOException {
    this.shingleFilterTest(4, TEST_TOKEN, FOUR_GRAM_TOKENS,
        FOUR_GRAM_POSITION_INCREMENTS, FOUR_GRAM_TYPES,
                           true);
  }
  
  public void testFourGramFilterWithoutUnigrams() throws IOException {
    this.shingleFilterTest(4, TEST_TOKEN, FOUR_GRAM_TOKENS_WITHOUT_UNIGRAMS,
        FOUR_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS,
        FOUR_GRAM_TYPES_WITHOUT_UNIGRAMS, false);
  }
  
  
  public void testTriGramFilterMinTriGram() throws IOException {
    this.shingleFilterTest(3, 3, TEST_TOKEN, TRI_GRAM_TOKENS_MIN_TRI_GRAM,
                           TRI_GRAM_POSITION_INCREMENTS_MIN_TRI_GRAM,
                           TRI_GRAM_TYPES_MIN_TRI_GRAM,
                           true);
  }
  
  public void testTriGramFilterWithoutUnigramsMinTriGram() throws IOException {
    this.shingleFilterTest(3, 3, TEST_TOKEN, 
                           TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM,
                           TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM, 
                           TRI_GRAM_TYPES_WITHOUT_UNIGRAMS_MIN_TRI_GRAM,
                           false);
  }
  
  public void testFourGramFilterMinTriGram() throws IOException {
    this.shingleFilterTest(3, 4, TEST_TOKEN, FOUR_GRAM_TOKENS_MIN_TRI_GRAM,
                           FOUR_GRAM_POSITION_INCREMENTS_MIN_TRI_GRAM, 
                           FOUR_GRAM_TYPES_MIN_TRI_GRAM,
                           true);
  }
  
  public void testFourGramFilterWithoutUnigramsMinTriGram() throws IOException {
    this.shingleFilterTest(3, 4, TEST_TOKEN, 
                           FOUR_GRAM_TOKENS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM,
                           FOUR_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_MIN_TRI_GRAM,
                           FOUR_GRAM_TYPES_WITHOUT_UNIGRAMS_MIN_TRI_GRAM, false);
  }

  public void testFourGramFilterMinFourGram() throws IOException {
    this.shingleFilterTest(4, 4, TEST_TOKEN, FOUR_GRAM_TOKENS_MIN_FOUR_GRAM,
                           FOUR_GRAM_POSITION_INCREMENTS_MIN_FOUR_GRAM, 
                           FOUR_GRAM_TYPES_MIN_FOUR_GRAM,
                           true);
  }
  
  public void testFourGramFilterWithoutUnigramsMinFourGram() throws IOException {
    this.shingleFilterTest(4, 4, TEST_TOKEN, 
                           FOUR_GRAM_TOKENS_WITHOUT_UNIGRAMS_MIN_FOUR_GRAM,
                           FOUR_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_MIN_FOUR_GRAM,
                           FOUR_GRAM_TYPES_WITHOUT_UNIGRAMS_MIN_FOUR_GRAM, false);
  }
 
  public void testBiGramFilterNoSeparator() throws IOException {
    this.shingleFilterTest("", 2, 2, TEST_TOKEN, BI_GRAM_TOKENS_NO_SEPARATOR,
                           BI_GRAM_POSITION_INCREMENTS_NO_SEPARATOR, 
                           BI_GRAM_TYPES_NO_SEPARATOR, true);
  }

  public void testBiGramFilterWithoutUnigramsNoSeparator() throws IOException {
    this.shingleFilterTest("", 2, 2, TEST_TOKEN, 
                           BI_GRAM_TOKENS_WITHOUT_UNIGRAMS_NO_SEPARATOR,
                           BI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_NO_SEPARATOR, 
                           BI_GRAM_TYPES_WITHOUT_UNIGRAMS_NO_SEPARATOR,
                           false);
  }
  public void testTriGramFilterNoSeparator() throws IOException {
    this.shingleFilterTest("", 2, 3, TEST_TOKEN, TRI_GRAM_TOKENS_NO_SEPARATOR,
                           TRI_GRAM_POSITION_INCREMENTS_NO_SEPARATOR, 
                           TRI_GRAM_TYPES_NO_SEPARATOR, true);
  }
  
  public void testTriGramFilterWithoutUnigramsNoSeparator() throws IOException {
    this.shingleFilterTest("", 2, 3, TEST_TOKEN, 
                           TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS_NO_SEPARATOR,
                           TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_NO_SEPARATOR,
                           TRI_GRAM_TYPES_WITHOUT_UNIGRAMS_NO_SEPARATOR, false);
  }
  
  public void testBiGramFilterAltSeparator() throws IOException {
    this.shingleFilterTest("<SEP>", 2, 2, TEST_TOKEN, BI_GRAM_TOKENS_ALT_SEPARATOR,
                           BI_GRAM_POSITION_INCREMENTS_ALT_SEPARATOR, 
                           BI_GRAM_TYPES_ALT_SEPARATOR, true);
  }

  public void testBiGramFilterWithoutUnigramsAltSeparator() throws IOException {
    this.shingleFilterTest("<SEP>", 2, 2, TEST_TOKEN, 
                           BI_GRAM_TOKENS_WITHOUT_UNIGRAMS_ALT_SEPARATOR,
                           BI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_ALT_SEPARATOR, 
                           BI_GRAM_TYPES_WITHOUT_UNIGRAMS_ALT_SEPARATOR,
                           false);
  }
  public void testTriGramFilterAltSeparator() throws IOException {
    this.shingleFilterTest("<SEP>", 2, 3, TEST_TOKEN, TRI_GRAM_TOKENS_ALT_SEPARATOR,
                           TRI_GRAM_POSITION_INCREMENTS_ALT_SEPARATOR, 
                           TRI_GRAM_TYPES_ALT_SEPARATOR, true);
  }
  
  public void testTriGramFilterWithoutUnigramsAltSeparator() throws IOException {
    this.shingleFilterTest("<SEP>", 2, 3, TEST_TOKEN, 
                           TRI_GRAM_TOKENS_WITHOUT_UNIGRAMS_ALT_SEPARATOR,
                           TRI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS_ALT_SEPARATOR,
                           TRI_GRAM_TYPES_WITHOUT_UNIGRAMS_ALT_SEPARATOR, false);
  }

  public void testTriGramFilterNullSeparator() throws IOException {
    this.shingleFilterTest(null, 2, 3, TEST_TOKEN, TRI_GRAM_TOKENS_NULL_SEPARATOR,
                           TRI_GRAM_POSITION_INCREMENTS_NULL_SEPARATOR, 
                           TRI_GRAM_TYPES_NULL_SEPARATOR, true);
  }

  public void testPositionIncrementEqualToN() throws IOException {
    this.shingleFilterTest(2, 3, TEST_TOKEN_POS_INCR_EQUAL_TO_N, TRI_GRAM_TOKENS_POS_INCR_EQUAL_TO_N,
                           TRI_GRAM_POSITION_INCREMENTS_POS_INCR_EQUAL_TO_N, 
                           TRI_GRAM_TYPES_POS_INCR_EQUAL_TO_N, true);
  }
  
  public void testPositionIncrementEqualToNWithoutUnigrams() throws IOException {
    this.shingleFilterTest(2, 3, TEST_TOKEN_POS_INCR_EQUAL_TO_N, TRI_GRAM_TOKENS_POS_INCR_EQUAL_TO_N_WITHOUT_UNIGRAMS,
                           TRI_GRAM_POSITION_INCREMENTS_POS_INCR_EQUAL_TO_N_WITHOUT_UNIGRAMS, 
                           TRI_GRAM_TYPES_POS_INCR_EQUAL_TO_N_WITHOUT_UNIGRAMS, false);
  }
  
  
  public void testPositionIncrementGreaterThanN() throws IOException {
    this.shingleFilterTest(2, 3, TEST_TOKEN_POS_INCR_GREATER_THAN_N, TRI_GRAM_TOKENS_POS_INCR_GREATER_THAN_N,
                           TRI_GRAM_POSITION_INCREMENTS_POS_INCR_GREATER_THAN_N, 
                           TRI_GRAM_TYPES_POS_INCR_GREATER_THAN_N, true);
  }
  
  public void testPositionIncrementGreaterThanNWithoutUnigrams() throws IOException {
    this.shingleFilterTest(2, 3, TEST_TOKEN_POS_INCR_GREATER_THAN_N, TRI_GRAM_TOKENS_POS_INCR_GREATER_THAN_N_WITHOUT_UNIGRAMS,
                           TRI_GRAM_POSITION_INCREMENTS_POS_INCR_GREATER_THAN_N_WITHOUT_UNIGRAMS, 
                           TRI_GRAM_TYPES_POS_INCR_GREATER_THAN_N_WITHOUT_UNIGRAMS, false);
  }
  
  public void testReset() throws Exception {
    Tokenizer wsTokenizer = new WhitespaceTokenizer();
    wsTokenizer.setReader(new StringReader("please divide this sentence"));
    TokenStream filter = new ShingleFilter(wsTokenizer, 2);
    assertTokenStreamContents(filter,
      new String[]{"please","please divide","divide","divide this","this","this sentence","sentence"},
      new int[]{0,0,7,7,14,14,19}, new int[]{6,13,13,18,18,27,27},
      new String[]{TypeAttribute.DEFAULT_TYPE,"shingle",TypeAttribute.DEFAULT_TYPE,"shingle",TypeAttribute.DEFAULT_TYPE,"shingle",TypeAttribute.DEFAULT_TYPE},
      new int[]{1,0,1,0,1,0,1}
    );
    wsTokenizer.setReader(new StringReader("please divide this sentence"));
    assertTokenStreamContents(filter,
      new String[]{"please","please divide","divide","divide this","this","this sentence","sentence"},
      new int[]{0,0,7,7,14,14,19}, new int[]{6,13,13,18,18,27,27},
      new String[]{TypeAttribute.DEFAULT_TYPE,"shingle",TypeAttribute.DEFAULT_TYPE,"shingle",TypeAttribute.DEFAULT_TYPE,"shingle",TypeAttribute.DEFAULT_TYPE},
      new int[]{1,0,1,0,1,0,1}
    );
  }

  public void testOutputUnigramsIfNoShinglesSingleTokenCase() throws IOException {
    // Single token input with outputUnigrams==false is the primary case where
    // enabling this option should alter program behavior.
    this.shingleFilterTest(2, 2, TEST_SINGLE_TOKEN, SINGLE_TOKEN,
                           SINGLE_TOKEN_INCREMENTS, SINGLE_TOKEN_TYPES,
                           false, true);
  }
 
  public void testOutputUnigramsIfNoShinglesWithSimpleBigram() throws IOException {
    // Here we expect the same result as with testBiGramFilter().
    this.shingleFilterTest(2, 2, TEST_TOKEN, BI_GRAM_TOKENS,
                           BI_GRAM_POSITION_INCREMENTS, BI_GRAM_TYPES,
                           true, true);
  }

  public void testOutputUnigramsIfNoShinglesWithSimpleUnigramlessBigram() throws IOException {
    // Here we expect the same result as with testBiGramFilterWithoutUnigrams().
    this.shingleFilterTest(2, 2, TEST_TOKEN, BI_GRAM_TOKENS_WITHOUT_UNIGRAMS,
                           BI_GRAM_POSITION_INCREMENTS_WITHOUT_UNIGRAMS, BI_GRAM_TYPES_WITHOUT_UNIGRAMS,
                           false, true);
  }

  public void testOutputUnigramsIfNoShinglesWithMultipleInputTokens() throws IOException {
    // Test when the minimum shingle size is greater than the number of input tokens
    this.shingleFilterTest(7, 7, TEST_TOKEN, TEST_TOKEN, 
                           UNIGRAM_ONLY_POSITION_INCREMENTS, UNIGRAM_ONLY_TYPES,
                           false, true);
  }

  protected void shingleFilterTest(int maxSize, Token[] tokensToShingle, Token[] tokensToCompare,
                                   int[] positionIncrements, String[] types,
                                   boolean outputUnigrams)
    throws IOException {

    ShingleFilter filter = new ShingleFilter(new CannedTokenStream(tokensToShingle), maxSize);
    filter.setOutputUnigrams(outputUnigrams);
    shingleFilterTestCommon(filter, tokensToCompare, positionIncrements, types);
  }

  protected void shingleFilterTest(int minSize, int maxSize, Token[] tokensToShingle, 
                                   Token[] tokensToCompare, int[] positionIncrements,
                                   String[] types, boolean outputUnigrams)
    throws IOException {
    ShingleFilter filter 
      = new ShingleFilter(new CannedTokenStream(tokensToShingle), minSize, maxSize);
    filter.setOutputUnigrams(outputUnigrams);
    shingleFilterTestCommon(filter, tokensToCompare, positionIncrements, types);
  }

  protected void shingleFilterTest(int minSize, int maxSize, Token[] tokensToShingle, 
                                   Token[] tokensToCompare, int[] positionIncrements,
                                   String[] types, boolean outputUnigrams, 
                                   boolean outputUnigramsIfNoShingles)
    throws IOException {
    ShingleFilter filter 
      = new ShingleFilter(new CannedTokenStream(tokensToShingle), minSize, maxSize);
    filter.setOutputUnigrams(outputUnigrams);
    filter.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
    shingleFilterTestCommon(filter, tokensToCompare, positionIncrements, types);
  }

  protected void shingleFilterTest(String tokenSeparator, int minSize, int maxSize, Token[] tokensToShingle, 
                                   Token[] tokensToCompare, int[] positionIncrements,
                                   String[] types, boolean outputUnigrams)
    throws IOException {
    ShingleFilter filter 
      = new ShingleFilter(new CannedTokenStream(tokensToShingle), minSize, maxSize);
    filter.setTokenSeparator(tokenSeparator);
    filter.setOutputUnigrams(outputUnigrams);
    shingleFilterTestCommon(filter, tokensToCompare, positionIncrements, types);
  }

  protected void shingleFilterTestCommon(ShingleFilter filter,
                                         Token[] tokensToCompare,
                                         int[] positionIncrements,
                                         String[] types)
    throws IOException {
    String text[] = new String[tokensToCompare.length];
    int startOffsets[] = new int[tokensToCompare.length];
    int endOffsets[] = new int[tokensToCompare.length];
    
    for (int i = 0; i < tokensToCompare.length; i++) {
      text[i] = new String(tokensToCompare[i].buffer(),0, tokensToCompare[i].length());
      startOffsets[i] = tokensToCompare[i].startOffset();
      endOffsets[i] = tokensToCompare[i].endOffset();
    }
    
    assertTokenStreamContents(filter, text, startOffsets, endOffsets, types, positionIncrements);
  }
  
  private static Token createToken(String term, int start, int offset) {
    return createToken(term, start, offset, 1);
  }

  private static Token createToken
    (String term, int start, int offset, int positionIncrement)
  {
    Token token = new Token();
    token.setOffset(start, offset);
    token.copyBuffer(term.toCharArray(), 0, term.length());
    token.setPositionIncrement(positionIncrement);
    return token;
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new ShingleFilter(tokenizer));
      }
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new ShingleFilter(tokenizer));
      }
    };
    checkRandomData(random, a, 100*RANDOM_MULTIPLIER, 8192);
    a.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new ShingleFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }

  public void testTrailingHole1() throws IOException {
    // Analyzing "wizard of", where of is removed as a
    // stopword leaving a trailing hole:
    Token[] inputTokens = new Token[] {createToken("wizard", 0, 6)};
    ShingleFilter filter = new ShingleFilter(new CannedTokenStream(1, 9, inputTokens), 2, 2);

    assertTokenStreamContents(filter,
                              new String[] {"wizard", "wizard _"},
                              new int[] {0, 0},
                              new int[] {6, 9},
                              new int[] {1, 0},
                              9);
  }

  public void testTrailingHole2() throws IOException {
    // Analyzing "purple wizard of", where of is removed as a
    // stopword leaving a trailing hole:
    Token[] inputTokens = new Token[] {createToken("purple", 0, 6),
                                       createToken("wizard", 7, 13)};
    ShingleFilter filter = new ShingleFilter(new CannedTokenStream(1, 16, inputTokens), 2, 2);

    assertTokenStreamContents(filter,
                              new String[] {"purple", "purple wizard", "wizard", "wizard _"},
                              new int[] {0, 0, 7, 7},
                              new int[] {6, 13, 13, 16},
                              new int[] {1, 0, 1, 0},
                              16);
  }

  public void testTwoTrailingHoles() throws IOException {
    // Analyzing "purple wizard of the", where of and the are removed as a
    // stopwords, leaving two trailing holes:
    Token[] inputTokens = new Token[] {createToken("purple", 0, 6),
                                       createToken("wizard", 7, 13)};
    ShingleFilter filter = new ShingleFilter(new CannedTokenStream(2, 20, inputTokens), 2, 2);

    assertTokenStreamContents(filter,
                              new String[] {"purple", "purple wizard", "wizard", "wizard _"},
                              new int[] {0, 0, 7, 7},
                              new int[] {6, 13, 13, 20},
                              new int[] {1, 0, 1, 0},
                              20);
  }

  public void testTwoTrailingHolesTriShingle() throws IOException {
    // Analyzing "purple wizard of the", where of and the are removed as a
    // stopwords, leaving two trailing holes:
    Token[] inputTokens = new Token[] {createToken("purple", 0, 6),
                                       createToken("wizard", 7, 13)};
    ShingleFilter filter = new ShingleFilter(new CannedTokenStream(2, 20, inputTokens), 2, 3);

    assertTokenStreamContents(filter,
                              new String[] {"purple", "purple wizard", "purple wizard _", "wizard", "wizard _", "wizard _ _"},
                              new int[] {0, 0, 0, 7, 7, 7},
                              new int[] {6, 13, 20, 13, 20, 20},
                              new int[] {1, 0, 0, 1, 0, 0},
                              20);
  }

  public void testTwoTrailingHolesTriShingleWithTokenFiller() throws IOException {
    // Analyzing "purple wizard of the", where of and the are removed as a
    // stopwords, leaving two trailing holes:
    Token[] inputTokens = new Token[] {createToken("purple", 0, 6), createToken("wizard", 7, 13)};
    ShingleFilter filter = new ShingleFilter(new CannedTokenStream(2, 20, inputTokens), 2, 3);
    filter.setFillerToken("--");

    assertTokenStreamContents(filter,
        new String[]{"purple", "purple wizard", "purple wizard --", "wizard", "wizard --", "wizard -- --"},
        new int[]{0, 0, 0, 7, 7, 7},
        new int[]{6, 13, 20, 13, 20, 20},
        new int[]{1, 0, 0, 1, 0, 0},
        20);

     filter = new ShingleFilter(new CannedTokenStream(2, 20, inputTokens), 2, 3);
    filter.setFillerToken("");

    assertTokenStreamContents(filter,
        new String[]{"purple", "purple wizard", "purple wizard ", "wizard", "wizard ", "wizard  "},
        new int[]{0, 0, 0, 7, 7, 7},
        new int[]{6, 13, 20, 13, 20, 20},
        new int[]{1, 0, 0, 1, 0, 0},
        20);


    filter = new ShingleFilter(new CannedTokenStream(2, 20, inputTokens), 2, 3);
    filter.setFillerToken(null);

    assertTokenStreamContents(filter,
        new String[] {"purple", "purple wizard", "purple wizard ", "wizard", "wizard ", "wizard  "},
        new int[] {0, 0, 0, 7, 7, 7},
        new int[] {6, 13, 20, 13, 20, 20},
        new int[] {1, 0, 0, 1, 0, 0},
        20);


    filter = new ShingleFilter(new CannedTokenStream(2, 20, inputTokens), 2, 3);
    filter.setFillerToken(null);
    filter.setTokenSeparator(null);

    assertTokenStreamContents(filter,
        new String[] {"purple", "purplewizard", "purplewizard", "wizard", "wizard", "wizard"},
        new int[] {0, 0, 0, 7, 7, 7},
        new int[] {6, 13, 20, 13, 20, 20},
        new int[] {1, 0, 0, 1, 0, 0},
        20);
  }
}
