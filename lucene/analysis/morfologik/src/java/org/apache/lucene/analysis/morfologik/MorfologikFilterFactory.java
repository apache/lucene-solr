package org.apache.lucene.analysis.morfologik;

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
import java.util.logging.Logger;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Filter factory for {@link MorfologikFilter}. For backward compatibility polish
 * dictionary is used as default. You can change dictionary resource 
 * by dictionary-resource parameter.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_polish" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.MorfologikFilterFactory" dictionary-resource="pl" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * @see <a href="http://morfologik.blogspot.com/">Morfologik web site</a>
 */
public class MorfologikFilterFactory extends TokenFilterFactory {
  /**
   * The default dictionary resource (for Polish). 
   */
  public static final String DEFAULT_DICTIONARY_RESOURCE = "pl";

  /**
   * Stemming dictionary resource. See {@link MorfologikAnalyzer} for more details. 
   */
  private final String dictionaryResource;

  /** Schema attribute. */
  @Deprecated
  public static final String DICTIONARY_SCHEMA_ATTRIBUTE = "dictionary";

  /** Dictionary resource */
  public static final String DICTIONARY_RESOURCE_ATTRIBUTE = "dictionary-resource";

  /** Creates a new MorfologikFilterFactory */
  public MorfologikFilterFactory(Map<String,String> args) {
    super(args);

    // Be specific about no-longer-supported dictionary attribute.
    String dictionaryName = get(args, DICTIONARY_SCHEMA_ATTRIBUTE);
    if (dictionaryName != null && !dictionaryName.isEmpty()) {
      // We do not throw a hard exception on 4.x branch to keep it backward compatible.
      // Emit a warning though.
      Logger.getLogger(MorfologikFilterFactory.class.getName())
        .warning("The " + DICTIONARY_SCHEMA_ATTRIBUTE + " attribute is no "
          + "longer supported (Morfologik now offers one unified Polish dictionary): " + dictionaryName
          + ". Perhaps you wanted to use 'dictionary-resource' attribute instead?");
    }

    dictionaryResource = get(args, DICTIONARY_RESOURCE_ATTRIBUTE, DEFAULT_DICTIONARY_RESOURCE);
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream ts) {
    return new MorfologikFilter(ts, dictionaryResource);
  }
}
