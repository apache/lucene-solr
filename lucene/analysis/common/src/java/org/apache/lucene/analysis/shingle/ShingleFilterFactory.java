package org.apache.lucene.analysis.shingle;

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
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Map;

/** 
 * Factory for {@link ShingleFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_shingle" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ShingleFilterFactory" minShingleSize="2" maxShingleSize="2"
 *             outputUnigrams="true" outputUnigramsIfNoShingles="false" tokenSeparator=" "/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class ShingleFilterFactory extends TokenFilterFactory {
  private final int minShingleSize;
  private final int maxShingleSize;
  private final boolean outputUnigrams;
  private final boolean outputUnigramsIfNoShingles;
  private final String tokenSeparator;

  /** Creates a new ShingleFilterFactory */
  public ShingleFilterFactory(Map<String, String> args) {
    super(args);
    maxShingleSize = getInt(args, "maxShingleSize", ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
    if (maxShingleSize < 2) {
      throw new IllegalArgumentException("Invalid maxShingleSize (" + maxShingleSize + ") - must be at least 2");
    }
    minShingleSize = getInt(args, "minShingleSize", ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE);
    if (minShingleSize < 2) {
      throw new IllegalArgumentException("Invalid minShingleSize (" + minShingleSize + ") - must be at least 2");
    }
    if (minShingleSize > maxShingleSize) {
      throw new IllegalArgumentException
          ("Invalid minShingleSize (" + minShingleSize + ") - must be no greater than maxShingleSize (" + maxShingleSize + ")");
    }
    outputUnigrams = getBoolean(args, "outputUnigrams", true);
    outputUnigramsIfNoShingles = getBoolean(args, "outputUnigramsIfNoShingles", false);
    tokenSeparator = get(args, "tokenSeparator", ShingleFilter.TOKEN_SEPARATOR);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public ShingleFilter create(TokenStream input) {
    ShingleFilter r = new ShingleFilter(input, minShingleSize, maxShingleSize);
    r.setOutputUnigrams(outputUnigrams);
    r.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
    r.setTokenSeparator(tokenSeparator);
    return r;
  }
}

