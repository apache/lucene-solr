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

import java.io.Reader;
import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

/**
 * Factory for {@link NGramTokenizer}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ngrm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.NGramTokenizerFactory" minGramSize="1" maxGramSize="2"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class NGramTokenizerFactory extends TokenizerFactory {

  /** SPI name */
  public static final String NAME = "nGram";

  private final int maxGramSize;
  private final int minGramSize;

  /** Creates a new NGramTokenizerFactory */
  public NGramTokenizerFactory(Map<String, String> args) {
    super(args);
    minGramSize = getInt(args, "minGramSize", NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE);
    maxGramSize = getInt(args, "maxGramSize", NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public NGramTokenizerFactory() {
    throw defaultCtorException();
  }

  /**
   * Creates the {@link TokenStream} of n-grams from the given {@link Reader} and {@link
   * AttributeFactory}.
   */
  @Override
  public Tokenizer create(AttributeFactory factory) {
    return new NGramTokenizer(factory, minGramSize, maxGramSize);
  }
}
