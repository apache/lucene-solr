// -*- c-basic-offset: 2 -*-
package org.apache.lucene.analysis.morfologik;

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

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

import morfologik.stemming.PolishStemmer.DICTIONARY;

/**
 * {@link org.apache.lucene.analysis.Analyzer} using Morfologik library.
 * @see <a href="http://morfologik.blogspot.com/">Morfologik project page</a>
 */
public class MorfologikAnalyzer extends Analyzer {

  private final DICTIONARY dictionary;
  private final Version version;

  /**
   * Builds an analyzer for a given PolishStemmer.DICTIONARY enum.
   * 
   * @param vers
   *          lucene compatibility version
   * @param dict
   *          A constant specifying which dictionary to choose. See the
   *          Morfologik documentation for details or use the default.
   */
  public MorfologikAnalyzer(final Version vers, final DICTIONARY dict) {
    this.version = vers;
    this.dictionary = dict;
  }

  /**
   * Builds an analyzer for an original MORFOLOGIK dictionary.
   * 
   * @param vers         lucene compatibility version
   */
  public MorfologikAnalyzer(final Version vers) {
    this(vers, DICTIONARY.MORFOLOGIK);
  }

  /**
   * Creates a
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * which tokenizes all the text in the provided {@link Reader}.
   * 
   * @param field ignored field name
   * @param reader source of tokens
   * 
   * @return A
   *         {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from an {@link StandardTokenizer} filtered with
   *         {@link StandardFilter} and {@link MorfologikFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(final String field, final Reader reader) {
    final Tokenizer src = new StandardTokenizer(this.version, reader);
    
    return new TokenStreamComponents(
      src,
      new MorfologikFilter(new StandardFilter(this.version, src), this.dictionary, this.version));
  }
}
