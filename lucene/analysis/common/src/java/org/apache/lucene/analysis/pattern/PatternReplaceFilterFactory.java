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


import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Factory for {@link PatternReplaceFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ptnreplace" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory"/&gt;
 *     &lt;filter class="solr.PatternReplaceFilterFactory" pattern="([^a-z])" replacement=""
 *             replace="all"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @see PatternReplaceFilter
 *
 * @since 3.1
 * @lucene.spi {@value #NAME}
 */
public class PatternReplaceFilterFactory extends TokenFilterFactory {

  /** SPI name */
  public static final String NAME = "patternReplace";

  final Pattern pattern;
  final String replacement;
  final boolean replaceAll;
  
  /** Creates a new PatternReplaceFilterFactory */
  public PatternReplaceFilterFactory(Map<String, String> args) {
    super(args);
    pattern = getPattern(args, "pattern");
    replacement = get(args, "replacement");
    replaceAll = "all".equals(get(args, "replace", Arrays.asList("all", "first"), "all"));
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public PatternReplaceFilter create(TokenStream input) {
    return new PatternReplaceFilter(input, pattern, replacement, replaceAll);
  }
}
