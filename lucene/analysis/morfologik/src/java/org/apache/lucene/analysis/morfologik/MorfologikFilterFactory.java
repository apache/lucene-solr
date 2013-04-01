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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import morfologik.stemming.PolishStemmer.DICTIONARY;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Filter factory for {@link MorfologikFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="text_polish" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.MorfologikFilterFactory" dictionary="MORFOLOGIK" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * <p>Any of Morfologik dictionaries can be used, these are at the moment:
 * <code>MORFOLOGIK</code> (Morfologik's original dictionary),
 * <code>MORFEUSZ</code> (Morfeusz-SIAT),
 * <code>COMBINED</code> (both of the dictionaries above, combined).
 * 
 * @see <a href="http://morfologik.blogspot.com/">Morfologik web site</a>
 */
public class MorfologikFilterFactory extends TokenFilterFactory {
  /** Dictionary. */
  private DICTIONARY dictionary = DICTIONARY.MORFOLOGIK;
  
  /** Schema attribute. */
  public static final String DICTIONARY_SCHEMA_ATTRIBUTE = "dictionary";
  
  /** Creates a new MorfologikFilterFactory */
  public MorfologikFilterFactory(Map<String,String> args) {
    super(args);
    String dictionaryName = get(args, DICTIONARY_SCHEMA_ATTRIBUTE);
    if (dictionaryName != null && !dictionaryName.isEmpty()) {
      try {
        DICTIONARY dictionary = DICTIONARY.valueOf(dictionaryName.toUpperCase(Locale.ROOT));
        assert dictionary != null;
        this.dictionary = dictionary;
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("The " + DICTIONARY_SCHEMA_ATTRIBUTE + " attribute accepts the "
            + "following constants: " + Arrays.toString(DICTIONARY.values()) + ", this value is invalid: "  
            + dictionaryName);
      }
    }
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public TokenStream create(TokenStream ts) {
    return new MorfologikFilter(ts, dictionary, luceneMatchVersion);
  }
}
