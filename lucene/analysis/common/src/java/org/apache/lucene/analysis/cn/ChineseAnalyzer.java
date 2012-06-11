package org.apache.lucene.analysis.cn;

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

import org.apache.lucene.analysis.standard.StandardAnalyzer; // javadoc @link
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;

/**
 * An {@link Analyzer} that tokenizes text with {@link ChineseTokenizer} and
 * filters with {@link ChineseFilter}
 * @deprecated (3.1) Use {@link StandardAnalyzer} instead, which has the same functionality.
 * This analyzer will be removed in Lucene 5.0
 */
@Deprecated
public final class ChineseAnalyzer extends Analyzer {

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link ChineseTokenizer} filtered with
   *         {@link ChineseFilter}
   */
    @Override
    protected TokenStreamComponents createComponents(String fieldName,
        Reader reader) {
      final Tokenizer source = new ChineseTokenizer(reader);
      return new TokenStreamComponents(source, new ChineseFilter(source));
    }
}