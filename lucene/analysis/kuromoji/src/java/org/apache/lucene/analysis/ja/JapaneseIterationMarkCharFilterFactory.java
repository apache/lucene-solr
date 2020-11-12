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
package org.apache.lucene.analysis.ja;


import java.io.Reader;
import java.util.Map;

import org.apache.lucene.analysis.CharFilterFactory;

/**
 * Factory for {@link org.apache.lucene.analysis.ja.JapaneseIterationMarkCharFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ja" class="solr.TextField" positionIncrementGap="100" autoGeneratePhraseQueries="false"&gt;
 *   &lt;analyzer&gt;
 *     &lt;charFilter class="solr.JapaneseIterationMarkCharFilterFactory normalizeKanji="true" normalizeKana="true"/&gt;
 *     &lt;tokenizer class="solr.JapaneseTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 4.0.0
 * @lucene.spi {@value #NAME}
 */
public class JapaneseIterationMarkCharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "japaneseIterationMark";

  private static final String NORMALIZE_KANJI_PARAM = "normalizeKanji";
  private static final String NORMALIZE_KANA_PARAM = "normalizeKana";

  private final boolean normalizeKanji;
  private final boolean normalizeKana;
  
  /** Creates a new JapaneseIterationMarkCharFilterFactory */
  public JapaneseIterationMarkCharFilterFactory(Map<String,String> args) {
    super(args);
    normalizeKanji = getBoolean(args, NORMALIZE_KANJI_PARAM, JapaneseIterationMarkCharFilter.NORMALIZE_KANJI_DEFAULT);
    normalizeKana = getBoolean(args, NORMALIZE_KANA_PARAM, JapaneseIterationMarkCharFilter.NORMALIZE_KANA_DEFAULT);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public JapaneseIterationMarkCharFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public Reader create(Reader input) {
    return new JapaneseIterationMarkCharFilter(input, normalizeKanji, normalizeKana);
  }

  @Override
  public Reader normalize(Reader input) {
    return create(input);
  }
}
