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
package org.apache.lucene.analysis.boost;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;

/**
 * Factory for {@link DelimitedBoostTokenFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_dlmtd" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DelimitedBoostTokenFilterFactory" delimiter="|"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @lucene.spi {@value #NAME}
 */
public class DelimitedBoostTokenFilterFactory extends TokenFilterFactory {

  /**
   * SPI name
   */
  public static final String NAME = "delimitedBoost";
  public static final String DELIMITER_ATTR = "delimiter";
  public static final char DEFAULT_DELIMITER = '|';

  private final char delimiter;

  /**
   * Creates a new DelimitedPayloadTokenFilterFactory
   */
  public DelimitedBoostTokenFilterFactory(Map<String, String> args) {
    super(args);
    delimiter = getChar(args, DELIMITER_ATTR, DEFAULT_DELIMITER);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public DelimitedBoostTokenFilter create(TokenStream input) {
    return new DelimitedBoostTokenFilter(input, delimiter);
  }

}
