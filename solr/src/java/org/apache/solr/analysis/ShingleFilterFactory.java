
/**
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


package org.apache.solr.analysis;

import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

import java.util.Map;

/** 
 * Factory for {@link ShingleFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_shingle" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.ShingleFilterFactory" minShingleSize="2" maxShingleSize="2"
 *             outputUnigrams="true" outputUnigramsIfNoShingles="false" tokenSeparator=" "/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 */
public class ShingleFilterFactory extends BaseTokenFilterFactory {
  private int minShingleSize;
  private int maxShingleSize;
  private boolean outputUnigrams;
  private boolean outputUnigramsIfNoShingles;
  private String tokenSeparator;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    maxShingleSize = getInt("maxShingleSize", 
                            ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
    if (maxShingleSize < 2) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
                              "Invalid maxShingleSize (" + maxShingleSize
                              + ") - must be at least 2");
    }
    minShingleSize = getInt("minShingleSize",
                            ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE);
    if (minShingleSize < 2) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
                              "Invalid minShingleSize (" + minShingleSize
                              + ") - must be at least 2");
    }
    if (minShingleSize > maxShingleSize) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
                              "Invalid minShingleSize (" + minShingleSize
                              + ") - must be no greater than maxShingleSize ("
                              + maxShingleSize + ")");
    }
    outputUnigrams = getBoolean("outputUnigrams", true);
    outputUnigramsIfNoShingles = getBoolean("outputUnigramsIfNoShingles", false);
    tokenSeparator = args.containsKey("tokenSeparator")
                     ? args.get("tokenSeparator")
                     : ShingleFilter.TOKEN_SEPARATOR;
  }
  public ShingleFilter create(TokenStream input) {
    ShingleFilter r = new ShingleFilter(input, minShingleSize, maxShingleSize);
    r.setOutputUnigrams(outputUnigrams);
    r.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
    r.setTokenSeparator(tokenSeparator);
    return r;
  }
}

