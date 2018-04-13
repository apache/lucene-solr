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
package org.apache.lucene.analysis.ko;

import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.KoreanTokenizer.DecompoundMode;
import org.apache.lucene.analysis.ko.dict.UserDictionary;

import static org.apache.lucene.analysis.TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY;

/**
 * Analyzer for Korean that uses morphological analysis.
 * @see KoreanTokenizer
 * @lucene.experimental
 */
public class KoreanAnalyzer extends Analyzer {
  private final UserDictionary userDict;
  private final KoreanTokenizer.DecompoundMode mode;
  private final Set<POS.Tag> stopTags;
  private final boolean outputUnknownUnigrams;

  /**
   * Creates a new KoreanAnalyzer.
   */
  public KoreanAnalyzer() {
    this(null, KoreanTokenizer.DEFAULT_DECOMPOUND, KoreanPartOfSpeechStopFilter.DEFAULT_STOP_TAGS, false);
  }

  /**
   * Creates a new KoreanAnalyzer.
   *
   * @param userDict Optional: if non-null, user dictionary.
   * @param mode Decompound mode.
   * @param stopTags The set of part of speech that should be filtered.
   * @param outputUnknownUnigrams If true outputs unigrams for unknown words.
   */
  public KoreanAnalyzer(UserDictionary userDict, DecompoundMode mode, Set<POS.Tag> stopTags, boolean outputUnknownUnigrams) {
    super();
    this.userDict = userDict;
    this.mode = mode;
    this.stopTags = stopTags;
    this.outputUnknownUnigrams = outputUnknownUnigrams;
  }
  
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer tokenizer = new KoreanTokenizer(DEFAULT_TOKEN_ATTRIBUTE_FACTORY, userDict, mode, outputUnknownUnigrams);
    TokenStream stream = new KoreanPartOfSpeechStopFilter(tokenizer, stopTags);
    stream = new KoreanReadingFormFilter(stream);
    stream = new LowerCaseFilter(stream);
    return new TokenStreamComponents(tokenizer, stream);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = new LowerCaseFilter(in);
    return result;
  }
}
