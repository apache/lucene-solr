// -*- c-basic-offset: 2 -*-
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
package org.apache.lucene.analysis.morfologik;

import java.io.Reader;
import morfologik.stemming.Dictionary;
import morfologik.stemming.polish.PolishStemmer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * {@link org.apache.lucene.analysis.Analyzer} using Morfologik library.
 *
 * @see <a href="http://morfologik.blogspot.com/">Morfologik project page</a>
 * @since 4.0.0
 */
public class MorfologikAnalyzer extends Analyzer {
  private final Dictionary dictionary;

  /**
   * Builds an analyzer with an explicit {@link Dictionary} resource.
   *
   * @param dictionary A prebuilt automaton with inflected and base word forms.
   * @see <a href="https://github.com/morfologik/">https://github.com/morfologik/</a>
   */
  public MorfologikAnalyzer(final Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  /** Builds an analyzer with the default Morfologik's Polish dictionary. */
  public MorfologikAnalyzer() {
    this(new PolishStemmer().getDictionary());
  }

  /**
   * Creates a {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} which tokenizes all
   * the text in the provided {@link Reader}.
   *
   * @param field ignored field name
   * @return A {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} built from an
   *     {@link StandardTokenizer} filtered with {@link MorfologikFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(final String field) {
    final Tokenizer src = new StandardTokenizer();

    return new TokenStreamComponents(src, new MorfologikFilter(src, dictionary));
  }
}
