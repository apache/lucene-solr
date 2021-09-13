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
package org.apache.lucene.analysis.commongrams;

import java.util.Map;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.AbstractWordsFileFilterFactory;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

/**
 * Constructs a {@link CommonGramsFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_cmmngrms" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.CommonGramsFilterFactory" words="commongramsstopwords.txt" ignoreCase="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class CommonGramsFilterFactory extends AbstractWordsFileFilterFactory {

  /** SPI name */
  public static final String NAME = "commonGrams";

  /** Creates a new CommonGramsFilterFactory */
  public CommonGramsFilterFactory(Map<String,String> args) {
    super(args);
  }

  public CharArraySet getCommonWords() {
    return getWords();
  }

  @Override
  protected CharArraySet createDefaultWords() {
    return new CharArraySet(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, isIgnoreCase());
  }

  @Override
  public TokenFilter create(TokenStream input) {
    CommonGramsFilter commonGrams = new CommonGramsFilter(input, getWords());
    return commonGrams;
  }
}

