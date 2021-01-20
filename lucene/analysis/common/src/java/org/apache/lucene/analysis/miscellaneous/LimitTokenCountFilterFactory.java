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
package org.apache.lucene.analysis.miscellaneous;

import java.util.Map;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Factory for {@link LimitTokenCountFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_lngthcnt" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LimitTokenCountFilterFactory" maxTokenCount="10" consumeAllTokens="false" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * <p>The {@code consumeAllTokens} property is optional and defaults to {@code false}. See {@link
 * LimitTokenCountFilter} for an explanation of its use.
 *
 * @since 3.1.0
 * @lucene.spi {@value #NAME}
 */
public class LimitTokenCountFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "limitTokenCount";

  public static final String MAX_TOKEN_COUNT_KEY = "maxTokenCount";
  public static final String CONSUME_ALL_TOKENS_KEY = "consumeAllTokens";
  final int maxTokenCount;
  final boolean consumeAllTokens;

  /** Creates a new LimitTokenCountFilterFactory */
  public LimitTokenCountFilterFactory(Map<String, String> args) {
    super(args);
    maxTokenCount = requireInt(args, MAX_TOKEN_COUNT_KEY);
    consumeAllTokens = getBoolean(args, CONSUME_ALL_TOKENS_KEY, false);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public LimitTokenCountFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new LimitTokenCountFilter(input, maxTokenCount, consumeAllTokens);
  }
}
