package org.apache.lucene.analysis.id;

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

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.id.IndonesianStemFilter;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/** 
 * Factory for {@link IndonesianStemFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_idstem" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.IndonesianStemFilterFactory" stemDerivational="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 */
public class IndonesianStemFilterFactory extends TokenFilterFactory {
  private final boolean stemDerivational;

  /** Creates a new IndonesianStemFilterFactory */
  public IndonesianStemFilterFactory(Map<String,String> args) {
    super(args);
    stemDerivational = getBoolean(args, "stemDerivational", true);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new IndonesianStemFilter(input, stemDerivational);
  }
}
