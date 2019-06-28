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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/** An {@link Analyzer} that filters {@link LetterTokenizer} 
 *  with {@link LowerCaseFilter} 
 *
 * @since 3.1
 **/
public final class SimpleAnalyzer extends Analyzer {

  /**
   * Creates a new {@link SimpleAnalyzer}
   */
  public SimpleAnalyzer() {
  }
  
  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    Tokenizer tokenizer = new LetterTokenizer();
    return new TokenStreamComponents(tokenizer, new LowerCaseFilter(tokenizer));
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return new LowerCaseFilter(in);
  }
}
