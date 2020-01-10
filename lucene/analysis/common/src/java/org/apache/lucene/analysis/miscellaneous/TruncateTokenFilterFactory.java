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


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;

/**
 * Factory for {@link org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter}. The following type is recommended for "<i>diacritics-insensitive search</i>" for Turkish.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_tr_ascii_f5" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ApostropheFilterFactory"/&gt;
 *     &lt;filter class="solr.TurkishLowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.ASCIIFoldingFilterFactory" preserveOriginal="true"/&gt;
 *     &lt;filter class="solr.KeywordRepeatFilterFactory"/&gt;
 *     &lt;filter class="solr.TruncateTokenFilterFactory" prefixLength="5"/&gt;
 *     &lt;filter class="solr.RemoveDuplicatesTokenFilterFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * @since 4.8.0
 * @lucene.spi {@value #NAME}
 */
public class TruncateTokenFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "truncate";

  public static final String PREFIX_LENGTH_KEY = "prefixLength";
  private final byte prefixLength;

  public TruncateTokenFilterFactory(Map<String, String> args) {
    super(args);
    prefixLength = Byte.parseByte(get(args, PREFIX_LENGTH_KEY, "5"));
    if (prefixLength < 1)
      throw new IllegalArgumentException(PREFIX_LENGTH_KEY + " parameter must be a positive number: " + prefixLength);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameter(s): " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TruncateTokenFilter(input, prefixLength);
  }
}
