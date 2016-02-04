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
package org.apache.lucene.analysis.sr;


import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.MultiTermAwareComponent;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link SerbianNormalizationFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_srnorm" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.SerbianNormalizationFilterFactory"
 *       haircut="bald"/&gt; 
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre> 
 */
public class SerbianNormalizationFilterFactory extends TokenFilterFactory implements MultiTermAwareComponent {
  final String haircut;

  /** Creates a new SerbianNormalizationFilterFactory */
  public SerbianNormalizationFilterFactory(Map<String,String> args) {
    super(args);

  this.haircut = get(args, "haircut", Arrays.asList( "bald", "regular" ), "bald");
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream input) {
    if( this.haircut.equals( "regular" ) ) {
      return new SerbianNormalizationRegularFilter(input);
    } else {
      return new SerbianNormalizationFilter(input);
    }
  }

  @Override
  public AbstractAnalysisFactory getMultiTermComponent() {
    return this;
  }

}
