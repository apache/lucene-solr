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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.CharStream;
import org.apache.lucene.analysis.CharReader;
import org.apache.lucene.analysis.Tokenizer;

import java.io.Reader;
import java.io.IOException;

/**
 * @version $Id$
 */

//
// An analyzer that uses a tokenizer and a list of token filters to
// create a TokenStream.
//
public class TokenizerChain extends SolrAnalyzer {
  final private CharFilterFactory[] charFilters;
  final private TokenizerFactory tokenizer;
  final private TokenFilterFactory[] filters;

  public TokenizerChain(TokenizerFactory tokenizer, TokenFilterFactory[] filters) {
    this(null,tokenizer,filters);
  }

  public TokenizerChain(CharFilterFactory[] charFilters, TokenizerFactory tokenizer, TokenFilterFactory[] filters) {
    this.charFilters = charFilters;
    this.tokenizer = tokenizer;
    this.filters = filters;
  }

  public CharFilterFactory[] getCharFilterFactories() { return charFilters; }
  public TokenizerFactory getTokenizerFactory() { return tokenizer; }
  public TokenFilterFactory[] getTokenFilterFactories() { return filters; }

  @Override
  public TokenStreamInfo getStream(String fieldName, Reader reader) {
    Tokenizer tk = (Tokenizer)tokenizer.create(charStream(reader));
    TokenStream ts = tk;
    for (int i=0; i<filters.length; i++) {
      ts = filters[i].create(ts);
    }
    return new TokenStreamInfo(tk,ts);
  }

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

}
