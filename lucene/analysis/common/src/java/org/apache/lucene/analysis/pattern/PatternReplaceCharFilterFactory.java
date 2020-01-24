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


import java.io.Reader;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.util.CharFilterFactory;

/**
 * Factory for {@link PatternReplaceCharFilter}. 
 * <pre class="prettyprint">
 * &lt;fieldType name="text_ptnreplace" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;charFilter class="solr.PatternReplaceCharFilterFactory" 
 *                    pattern="([^a-z])" replacement=""/&gt;
 *     &lt;tokenizer class="solr.KeywordTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * @since Solr 3.1
 * @lucene.spi {@value #NAME}
 */
public class PatternReplaceCharFilterFactory extends CharFilterFactory {

  /** SPI name */
  public static final String NAME = "patternReplace";

  private final Pattern pattern;
  private final String replacement;

  /** Creates a new PatternReplaceCharFilterFactory */
  public PatternReplaceCharFilterFactory(Map<String, String> args) {
    super(args);
    pattern = getPattern(args, "pattern");
    replacement = get(args, "replacement", "");
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public Reader create(Reader input) {
    return new PatternReplaceCharFilter(pattern, replacement, input);
  }

  @Override
  public Reader normalize(Reader input) {
    return create(input);
  }
}
