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
package org.apache.lucene.analysis.phonetic;


import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.phonetic.DoubleMetaphoneFilter;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link DoubleMetaphoneFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_dblmtphn" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DoubleMetaphoneFilterFactory" inject="true" maxCodeLength="4"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class DoubleMetaphoneFilterFactory extends TokenFilterFactory
{
  /** parameter name: true if encoded tokens should be added as synonyms */
  public static final String INJECT = "inject"; 
  /** parameter name: restricts the length of the phonetic code */
  public static final String MAX_CODE_LENGTH = "maxCodeLength"; 
  /** default maxCodeLength if not specified */
  public static final int DEFAULT_MAX_CODE_LENGTH = 4;

  private final boolean inject;
  private final int maxCodeLength;

  /** Creates a new DoubleMetaphoneFilterFactory */
  public DoubleMetaphoneFilterFactory(Map<String,String> args) {
    super(args);
    inject = getBoolean(args, INJECT, true);
    maxCodeLength = getInt(args, MAX_CODE_LENGTH, DEFAULT_MAX_CODE_LENGTH);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public DoubleMetaphoneFilter create(TokenStream input) {
    return new DoubleMetaphoneFilter(input, maxCodeLength, inject);
  }
}
