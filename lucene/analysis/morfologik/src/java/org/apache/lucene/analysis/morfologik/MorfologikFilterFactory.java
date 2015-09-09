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

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import morfologik.stemming.Dictionary;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Filter factory for {@link MorfologikFilter}. For backward compatibility polish
 * dictionary is used as default. You can change dictionary resource 
 * by dictionary-resource parameter:
 * <pre class="prettyprint">
 * &lt;fieldType name="text_polish" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.MorfologikFilterFactory" dictionary-resource="pl" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * <p>Alternatively, you can pass in the filenames of FSA ({@code ".dict"} and features "{@code ".info"}" file
 * (if the features file is not given, its name is derived from the FSA file):
 * <pre class="prettyprint">
 * &lt;fieldType name="text_polish" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.MorfologikFilterFactory" dictionary-fsa-file="mylang.dict" dictionary-features-file="mylang.info" /&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 * 
 * @see <a href="http://morfologik.blogspot.com/">Morfologik web site</a>
 */
public class MorfologikFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  /**
   * The default dictionary resource (for Polish). 
   */
  public static final String DEFAULT_DICTIONARY_RESOURCE = "pl";

  /** Schema attribute. */
  @Deprecated
  public static final String DICTIONARY_SCHEMA_ATTRIBUTE = "dictionary";

  /** Dictionary resource */
  public static final String DICTIONARY_RESOURCE_ATTRIBUTE = "dictionary-resource";

  /** Dictionary FSA file (should have {@code ".dict"} suffix), loaded from {@link ResourceLoader}. */
  public static final String DICTIONARY_FSA_FILE_ATTRIBUTE = "dictionary-fsa-file";

  /** Dictionary features/properties file, loaded from {@link ResourceLoader}. If not given, this
   * loads the file with same name like {@link #DICTIONARY_FSA_FILE_ATTRIBUTE}, but with
   * {@code ".info"} suffix.
   */
  public static final String DICTIONARY_FEATURES_FILE_ATTRIBUTE = "dictionary-features-file";

  private final String dictionaryFsaFile, dictionaryFeaturesFile, dictionaryResource;
  private Dictionary dictionary; // initialized on inform()

  /** Creates a new MorfologikFilterFactory */
  public MorfologikFilterFactory(Map<String,String> args) {
    super(args);

    // Be specific about no-longer-supported dictionary attribute.
    String dictionaryName = get(args, DICTIONARY_SCHEMA_ATTRIBUTE);
    if (dictionaryName != null && !dictionaryName.isEmpty()) {
      throw new IllegalArgumentException("The " + DICTIONARY_SCHEMA_ATTRIBUTE + " attribute is no "
          + "longer supported (Morfologik now offers one unified Polish dictionary): " + dictionaryName
          + ". Perhaps you wanted to use 'dictionary-resource' attribute instead?");
    }

    // first check FSA and features (at least FSA must be given, features name is guessed):
    dictionaryFsaFile = get(args, DICTIONARY_FSA_FILE_ATTRIBUTE);
    dictionaryFeaturesFile = get(args, DICTIONARY_FEATURES_FILE_ATTRIBUTE,
        (dictionaryFsaFile == null) ? null : Dictionary.getExpectedFeaturesName(dictionaryFsaFile));
    
    if (dictionaryFsaFile == null && dictionaryFeaturesFile == null) {
      // if we have no FSA/features combination, we resolve the classpath resource:
      dictionaryResource = get(args, DICTIONARY_RESOURCE_ATTRIBUTE, DEFAULT_DICTIONARY_RESOURCE);
    } else if (dictionaryFsaFile == null || dictionaryFeaturesFile == null) {
      // if we have incomplete FSA/features tuple in args
      throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Missing '%s' or '%s' attribute.",
          DICTIONARY_FSA_FILE_ATTRIBUTE, DICTIONARY_FEATURES_FILE_ATTRIBUTE));      
    } else {
      dictionaryResource = null;
      if (get(args, DICTIONARY_RESOURCE_ATTRIBUTE) != null) {
        // fail if both is given: FSA/features files + classpath resource
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Cannot give '%s' and '%s'/'%s' at the same time.",
            DICTIONARY_RESOURCE_ATTRIBUTE, DICTIONARY_FSA_FILE_ATTRIBUTE, DICTIONARY_FEATURES_FILE_ATTRIBUTE));
      }
    }
    
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (dictionaryFsaFile != null) {
      assert dictionaryFeaturesFile != null;
      assert dictionaryResource == null;
      try (final InputStream dictIn = loader.openResource(dictionaryFsaFile);
          final InputStream metaIn = loader.openResource(dictionaryFeaturesFile)) {
        this.dictionary = Dictionary.readAndClose(dictIn, metaIn);
      }
    } else {
      assert dictionaryResource != null;
      this.dictionary = MorfologikFilter.loadDictionaryResource(dictionaryResource);
    }
  }

  @Override
  public TokenStream create(TokenStream ts) {
    return new MorfologikFilter(ts, Objects.requireNonNull(dictionary, "MorfologikFilterFactory was not fully initialized."));
  }
}
