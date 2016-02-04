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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

/**
 * Factory for {@link LengthFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_lngth" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LengthFilterFactory" min="0" max="1" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class LengthFilterFactory extends TokenFilterFactory {
  final int min;
  final int max;
  public static final String MIN_KEY = "min";
  public static final String MAX_KEY = "max";
  private boolean enablePositionIncrements;

  /** Creates a new LengthFilterFactory */
  public LengthFilterFactory(Map<String, String> args) {
    super(args);
    min = requireInt(args, MIN_KEY);
    max = requireInt(args, MAX_KEY);

    if (luceneMatchVersion.onOrAfter(Version.LUCENE_5_0_0) == false) {
      boolean defaultValue = luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0);
      enablePositionIncrements = getBoolean(args, "enablePositionIncrements", defaultValue);
      if (enablePositionIncrements == false && luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
        throw new IllegalArgumentException("enablePositionIncrements=false is not supported anymore as of Lucene 4.4");
      }
    } else if (args.containsKey("enablePositionIncrements")) {
      throw new IllegalArgumentException("enablePositionIncrements is not a valid option as of Lucene 5.0");
    }
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }
  
  @Override
  public TokenFilter create(TokenStream input) {
    if (luceneMatchVersion.onOrAfter(Version.LUCENE_4_4_0)) {
      return new LengthFilter(input, min, max);
    } else {
      @SuppressWarnings("deprecation")
      final TokenFilter filter = new Lucene43LengthFilter(enablePositionIncrements, input, min, max);
      return filter;
    }
  }
}
