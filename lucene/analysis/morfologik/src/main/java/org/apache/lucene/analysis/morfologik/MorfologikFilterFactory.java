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
package org.apache.lucene.analysis.morfologik;


import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import morfologik.stemming.Dictionary;
import morfologik.stemming.DictionaryMetadata;
import morfologik.stemming.polish.PolishStemmer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Filter factory for {@link MorfologikFilter}. 
 * 
 * <p>An explicit resource name of the dictionary ({@code ".dict"}) can be 
 * provided via the <code>dictionary</code> attribute, as the example below demonstrates:
 * <pre class="prettyprint">
 * &lt;fieldType name="text_mylang" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.MorfologikFilterFactory" dictionary="mylang.dict" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * <p>If the dictionary attribute is not provided, the Polish dictionary is loaded
 * and used by default. 
 * 
 * @see <a href="http://morfologik.blogspot.com/">Morfologik web site</a>
 * @since 4.0.0
 */
public class MorfologikFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  /** Dictionary resource attribute (should have {@code ".dict"} suffix), loaded from {@link ResourceLoader}. */
  public static final String DICTIONARY_ATTRIBUTE = "dictionary";

  /** {@link #DICTIONARY_ATTRIBUTE} value passed to {@link #inform}. */
  private String resourceName;

  /** Loaded {@link Dictionary}, initialized on {@link #inform(ResourceLoader)}. */
  private Dictionary dictionary;

  /** Creates a new MorfologikFilterFactory */
  public MorfologikFilterFactory(Map<String,String> args) {
    super(args);

    // Be specific about no-longer-supported dictionary attribute.
    final String DICTIONARY_RESOURCE_ATTRIBUTE = "dictionary-resource";
    String dictionaryResource = get(args, DICTIONARY_RESOURCE_ATTRIBUTE);
    if (dictionaryResource != null && !dictionaryResource.isEmpty()) {
      throw new IllegalArgumentException("The " + DICTIONARY_RESOURCE_ATTRIBUTE + " attribute is no "
          + "longer supported. Use the '" + DICTIONARY_ATTRIBUTE + "' attribute instead (see LUCENE-6833).");
    }

    resourceName = get(args, DICTIONARY_ATTRIBUTE);

    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (resourceName == null) {
      // Get the dictionary lazily, does not hold up memory.
      this.dictionary = new PolishStemmer().getDictionary();
    } else {
      try (InputStream dict = loader.openResource(resourceName);
           InputStream meta = loader.openResource(DictionaryMetadata.getExpectedMetadataFileName(resourceName))) {
        this.dictionary = Dictionary.read(dict, meta);
      }
    }
  }

  @Override
  public TokenStream create(TokenStream ts) {
    return new MorfologikFilter(ts, Objects.requireNonNull(dictionary, "MorfologikFilterFactory was not fully initialized."));
  }
}
