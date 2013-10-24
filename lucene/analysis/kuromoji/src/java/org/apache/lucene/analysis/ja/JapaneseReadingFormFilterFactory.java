package org.apache.lucene.analysis.ja;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseReadingFormFilter;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;

/**
 * Factory for {@link org.apache.lucene.analysis.ja.JapaneseReadingFormFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ja" class="solr.TextField"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.JapaneseTokenizerFactory"/&gt;
 *     &lt;filter class="solr.JapaneseReadingFormFilterFactory"
 *             useRomaji="false"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 */
public class JapaneseReadingFormFilterFactory extends TokenFilterFactory {
  private static final String ROMAJI_PARAM = "useRomaji";
  private final boolean useRomaji;
  
  /** Creates a new JapaneseReadingFormFilterFactory */
  public JapaneseReadingFormFilterFactory(Map<String,String> args) {
    super(args);
    useRomaji = getBoolean(args, ROMAJI_PARAM, false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new JapaneseReadingFormFilter(input, useRomaji);
  }
}
