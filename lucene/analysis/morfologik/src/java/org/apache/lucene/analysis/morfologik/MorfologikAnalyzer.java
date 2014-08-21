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

/**
 * {@link org.apache.lucene.analysis.Analyzer} using Morfologik library.
 * @see <a href="http://morfologik.blogspot.com/">Morfologik project page</a>
 */
public class MorfologikAnalyzer extends Analyzer {
  private final String dictionary;

  /**
   * Builds an analyzer with an explicit dictionary resource.
   * 
   * @param dictionaryResource A constant specifying which dictionary to choose. The
   * dictionary resource must be named <code>morfologik/dictionaries/{dictionaryResource}.dict</code>
   * and have an associated <code>.info</code> metadata file. See the Morfologik project
   * for details.
   * 
   * @see "http://morfologik.blogspot.com/"
   */
  public MorfologikAnalyzer(final String dictionaryResource) {
    this.dictionary = dictionaryResource;
  }

  /**
   * @deprecated Use {@link #MorfologikAnalyzer(String)}
   */
  @Deprecated
  public MorfologikAnalyzer(final Version version, final String dictionaryResource) {
    setVersion(version);
    this.dictionary = dictionaryResource;
  }

  /**
   * Builds an analyzer with the default Morfologik's Polish dictionary.
   */
  public MorfologikAnalyzer() {
    this(MorfologikFilterFactory.DEFAULT_DICTIONARY_RESOURCE);
  }

  /**
   * @deprecated Use {@link #MorfologikAnalyzer()}
   */
  @Deprecated
  public MorfologikAnalyzer(final Version version) {
    this(version, MorfologikFilterFactory.DEFAULT_DICTIONARY_RESOURCE);
  }

  /**
   * Creates a
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * which tokenizes all the text in the provided {@link Reader}.
   * 
   * @param field ignored field name
   * @param reader source of tokens
   * @return A {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from an {@link StandardTokenizer} filtered with
   *         {@link StandardFilter} and {@link MorfologikFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(final String field, final Reader reader) {
    final Tokenizer src = new StandardTokenizer(getVersion(), reader);
    
    return new TokenStreamComponents(
        src, 
        new MorfologikFilter(new StandardFilter(getVersion(), src), dictionary, getVersion()));
  }
}
