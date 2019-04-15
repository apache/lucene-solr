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
package org.apache.solr.analysis;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;

/**
 * An analyzer that uses a tokenizer and a list of token filters to
 * create a TokenStream.
 *
 * It should probably be replaced with {@link CustomAnalyzer}.
 * @since 3.1
 */
public final class TokenizerChain extends SolrAnalyzer {
  private static final CharFilterFactory[] EMPTY_CHAR_FITLERS = new CharFilterFactory[0];
  private static final TokenFilterFactory[] EMPTY_TOKEN_FITLERS = new TokenFilterFactory[0];
  
  final private CharFilterFactory[] charFilters;
  final private TokenizerFactory tokenizer;
  final private TokenFilterFactory[] filters;

  /** Copy from CustomAnalyzer. */
  public TokenizerChain(CustomAnalyzer customAnalyzer) {
    this(
        customAnalyzer.getCharFilterFactories().toArray(new CharFilterFactory[0]),
        customAnalyzer.getTokenizerFactory(),
        customAnalyzer.getTokenFilterFactories().toArray(new TokenFilterFactory[0]));
    setPositionIncrementGap(customAnalyzer.getPositionIncrementGap(null));
    setVersion(customAnalyzer.getVersion());
    assert customAnalyzer.getOffsetGap(null) == 1; // note: we don't support setting the offset gap
  }

  /** 
   * Creates a new TokenizerChain w/o any CharFilterFactories.
   *
   * @param tokenizer Factory for the Tokenizer to use, must not be null.
   * @param filters Factories for the TokenFilters to use - if null, will be treated as if empty.
   */
  public TokenizerChain(TokenizerFactory tokenizer, TokenFilterFactory[] filters) {
    this(null,tokenizer,filters);
  }

  /** 
   * Creates a new TokenizerChain.
   *
   * @param charFilters Factories for the CharFilters to use, if any - if null, will be treated as if empty.
   * @param tokenizer Factory for the Tokenizer to use, must not be null.
   * @param filters Factories for the TokenFilters to use if any- if null, will be treated as if empty.
   */
  public TokenizerChain(CharFilterFactory[] charFilters, TokenizerFactory tokenizer, TokenFilterFactory[] filters) {
    charFilters = null == charFilters ? EMPTY_CHAR_FITLERS : charFilters;
    filters = null == filters ? EMPTY_TOKEN_FITLERS : filters;
    if (null == tokenizer) {
      throw new NullPointerException("TokenizerFactory must not be null");
    }
    
    this.charFilters = charFilters;
    this.tokenizer = tokenizer;
    this.filters = filters;
  }

  /** @return array of CharFilterFactories, may be empty but never null */
  public CharFilterFactory[] getCharFilterFactories() { return charFilters; }
  /** @return the TokenizerFactory in use, will never be null */
  public TokenizerFactory getTokenizerFactory() { return tokenizer; }
  /** @return array of TokenFilterFactories, may be empty but never null */
  public TokenFilterFactory[] getTokenFilterFactories() { return filters; }

  @Override
  public Reader initReader(String fieldName, Reader reader) {
    if (charFilters != null && charFilters.length > 0) {
      Reader cs = reader;
      for (CharFilterFactory charFilter : charFilters) {
        cs = charFilter.create(cs);
      }
      reader = cs;
    }
    return reader;
  }

  @Override
  protected Reader initReaderForNormalization(String fieldName, Reader reader) {
    if (charFilters != null && charFilters.length > 0) {
      for (CharFilterFactory charFilter : charFilters) {
        reader = charFilter.normalize(reader);
      }
    }
    return reader;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer tk = tokenizer.create(attributeFactory(fieldName));
    TokenStream ts = tk;
    for (TokenFilterFactory filter : filters) {
      ts = filter.create(ts);
    }
    return new TokenStreamComponents(tk, ts);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = in;
    for (TokenFilterFactory filter : filters) {
      result = filter.normalize(result);
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TokenizerChain(");
    for (CharFilterFactory filter: charFilters) {
      sb.append(filter);
      sb.append(", ");
    }
    sb.append(tokenizer);
    for (TokenFilterFactory filter: filters) {
      sb.append(", ");
      sb.append(filter);
    }
    sb.append(')');
    return sb.toString();
  }

  public Analyzer getMultiTermAnalyzer() {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tk = new KeywordTokenizer();
        TokenStream ts = tk;
        for (TokenFilterFactory filter : filters) {
          ts = filter.normalize(ts);
        }
        return new TokenStreamComponents(tk, ts);
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        if (charFilters != null && charFilters.length > 0) {
          Reader cs = reader;
          for (CharFilterFactory charFilter : charFilters) {
            cs = charFilter.normalize(cs);
          }
          reader = cs;
        }
        return reader;
      }
    };
  }

}
