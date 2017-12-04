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
package org.apache.lucene.analysis.pattern;


import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Factory for {@link PatternCaptureGroupTokenFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ptncapturegroup" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory"/&gt;
 *     &lt;filter class="solr.PatternCaptureGroupFilterFactory" pattern="([^a-z])" preserve_original="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @see PatternCaptureGroupTokenFilter
 * @since 4.4.0
 */
public class PatternCaptureGroupFilterFactory extends TokenFilterFactory {
  private Pattern pattern;
  private boolean preserveOriginal = true;
  
  public  PatternCaptureGroupFilterFactory(Map<String,String> args) {
    super(args);
    pattern = getPattern(args, "pattern");
    preserveOriginal = args.containsKey("preserve_original") ? Boolean.parseBoolean(args.get("preserve_original")) : true;
  }
  @Override
  public PatternCaptureGroupTokenFilter create(TokenStream input) {
    return new PatternCaptureGroupTokenFilter(input, preserveOriginal, pattern);
  }
}
