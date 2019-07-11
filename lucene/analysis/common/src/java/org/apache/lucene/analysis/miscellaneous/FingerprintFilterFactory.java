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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link FingerprintFilter}.
 * 
 * <pre class="prettyprint">
 * The {@code maxOutputTokenSize} property is optional and defaults to {@code 1024}.  
 * The {@code separator} property is optional and defaults to the space character.  
 * See
 * {@link FingerprintFilter} for an explanation of its use.
 * </pre>
 * @since 5.4.0
 */
public class FingerprintFilterFactory extends TokenFilterFactory {

  public static final String MAX_OUTPUT_TOKEN_SIZE_KEY = "maxOutputTokenSize";
  public static final String SEPARATOR_KEY = "separator";
  final int maxOutputTokenSize;
  final char separator;

  /** Creates a new FingerprintFilterFactory */
  public FingerprintFilterFactory(Map<String, String> args) {
    super(args);
    maxOutputTokenSize = getInt(args, MAX_OUTPUT_TOKEN_SIZE_KEY,
        FingerprintFilter.DEFAULT_MAX_OUTPUT_TOKEN_SIZE);
    separator = getChar(args, SEPARATOR_KEY,
        FingerprintFilter.DEFAULT_SEPARATOR);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new FingerprintFilter(input, maxOutputTokenSize, separator);
  }

}
