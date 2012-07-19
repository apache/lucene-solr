package org.apache.solr.analysis;

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

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.ja.JapaneseIterationMarkCharFilter;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.MultiTermAwareComponent;

import java.io.Reader;
import java.util.Map;

/**
 * Factory for {@link org.apache.lucene.analysis.ja.JapaneseIterationMarkCharFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ja" class="solr.TextField" positionIncrementGap="100" autoGeneratePhraseQueries="false"&gt;
 *   &lt;analyzer&gt;
 *     &lt;charFilter class="solr.JapaneseIterationMarkCharFilterFactory normalizeKanji="true" normalizeKana="true"/&gt;
 *     &lt;tokenizer class="solr.JapaneseTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class JapaneseIterationMarkCharFilterFactory extends CharFilterFactory implements MultiTermAwareComponent {

  private static final String NORMALIZE_KANJI_PARAM = "normalizeKanji";

  private static final String NORMALIZE_KANA_PARAM = "normalizeKana";

  private boolean normalizeKanji = true;

  private boolean normalizeKana = true;

  @Override
  public CharFilter create(Reader input) {
    return new JapaneseIterationMarkCharFilter(input, normalizeKanji, normalizeKana);
  }

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    normalizeKanji = getBoolean(NORMALIZE_KANJI_PARAM, JapaneseIterationMarkCharFilter.NORMALIZE_KANJI_DEFAULT);
    normalizeKana = getBoolean(NORMALIZE_KANA_PARAM, JapaneseIterationMarkCharFilter.NORMALIZE_KANA_DEFAULT);
  }

  @Override
  public AbstractAnalysisFactory getMultiTermComponent() {
    return this;
  }
}
