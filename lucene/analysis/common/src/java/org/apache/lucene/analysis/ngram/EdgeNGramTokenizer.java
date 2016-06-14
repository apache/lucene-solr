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
package org.apache.lucene.analysis.ngram;


import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.AttributeFactory;

/**
 * Tokenizes the input from an edge into n-grams of given size(s).
 * <p>
 * This {@link Tokenizer} create n-grams from the beginning edge of a input token.
 * <p><a name="match_version"></a>As of Lucene 4.4, this class supports
 * {@link #isTokenChar(int) pre-tokenization} and correctly handles
 * supplementary characters.
 */
public class EdgeNGramTokenizer extends NGramTokenizer {
  public static final int DEFAULT_MAX_GRAM_SIZE = 1;
  public static final int DEFAULT_MIN_GRAM_SIZE = 1;

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(int minGram, int maxGram) {
    super(minGram, maxGram, true);
  }

  /**
   * Creates EdgeNGramTokenizer that can generate n-grams in the sizes of the given range
   *
   * @param factory {@link org.apache.lucene.util.AttributeFactory} to use
   * @param minGram the smallest n-gram to generate
   * @param maxGram the largest n-gram to generate
   */
  public EdgeNGramTokenizer(AttributeFactory factory, int minGram, int maxGram) {
    super(factory, minGram, maxGram, true);
  }

}
