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
 * Factory for {@link DelimitedTermFrequencyTokenFilter}. The field must have {@code
 * omitPositions=true}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_tfdl" class="solr.TextField" omitPositions="true"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DelimitedTermFrequencyTokenFilterFactory" delimiter="|"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since 7.0.0
 * @lucene.spi {@value #NAME}
 */
public class DelimitedTermFrequencyTokenFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "delimitedTermFrequency";

  public static final String DELIMITER_ATTR = "delimiter";

  private final char delimiter;

  /** Creates a new DelimitedPayloadTokenFilterFactory */
  public DelimitedTermFrequencyTokenFilterFactory(Map<String, String> args) {
    super(args);
    delimiter = getChar(args, DELIMITER_ATTR, DelimitedTermFrequencyTokenFilter.DEFAULT_DELIMITER);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public DelimitedTermFrequencyTokenFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public DelimitedTermFrequencyTokenFilter create(TokenStream input) {
    return new DelimitedTermFrequencyTokenFilter(input, delimiter);
  }
}
