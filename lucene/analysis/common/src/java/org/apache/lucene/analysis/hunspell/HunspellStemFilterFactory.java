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
package org.apache.lucene.analysis.hunspell;


import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/**
 * TokenFilterFactory that creates instances of {@link HunspellStemFilter}.
 * Example config for British English:
 * <pre class="prettyprint">
 * &lt;filter class=&quot;solr.HunspellStemFilterFactory&quot;
 *         dictionary=&quot;en_GB.dic,my_custom.dic&quot;
 *         affix=&quot;en_GB.aff&quot; 
 *         ignoreCase=&quot;false&quot;
 *         longestOnly=&quot;false&quot; /&gt;</pre>
 * Both parameters dictionary and affix are mandatory.
 * Dictionaries for many languages are available through the OpenOffice project.
 * 
 * See <a href="http://wiki.apache.org/solr/Hunspell">http://wiki.apache.org/solr/Hunspell</a>
 * @lucene.experimental
 * @since 3.5.0
 * @lucene.spi {@value #NAME}
 */
public class HunspellStemFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "hunspellStem";

  private static final String PARAM_DICTIONARY    = "dictionary";
  private static final String PARAM_AFFIX         = "affix";
  // NOTE: this one is currently unused?:
  private static final String PARAM_RECURSION_CAP = "recursionCap";
  private static final String PARAM_IGNORE_CASE   = "ignoreCase";
  private static final String PARAM_LONGEST_ONLY  = "longestOnly";

  private final String dictionaryFiles;
  private final String affixFile;
  private final boolean ignoreCase;
  private final boolean longestOnly;
  private Dictionary dictionary;
  
  /** Creates a new HunspellStemFilterFactory */
  public HunspellStemFilterFactory(Map<String,String> args) {
    super(args);
    dictionaryFiles = require(args, PARAM_DICTIONARY);
    affixFile = get(args, PARAM_AFFIX);
    ignoreCase = getBoolean(args, PARAM_IGNORE_CASE, false);
    longestOnly = getBoolean(args, PARAM_LONGEST_ONLY, false);
    // this isnt necessary: we properly load all dictionaries.
    // but recognize and ignore for back compat
    getBoolean(args, "strictAffixParsing", true);
    // this isn't necessary: multi-stage stripping is fixed and 
    // flags like COMPLEXPREFIXES in the data itself control this.
    // but recognize and ignore for back compat
    getInt(args, "recursionCap", 0);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    String dicts[] = dictionaryFiles.split(",");

    InputStream affix = null;
    List<InputStream> dictionaries = new ArrayList<>();

    try {
      dictionaries = new ArrayList<>();
      for (String file : dicts) {
        dictionaries.add(loader.openResource(file));
      }
      affix = loader.openResource(affixFile);

      Path tempPath = Files.createTempDirectory(Dictionary.getDefaultTempDir(), "Hunspell");
      try (Directory tempDir = FSDirectory.open(tempPath)) {
        this.dictionary = new Dictionary(tempDir, "hunspell", affix, dictionaries, ignoreCase);
      } finally {
        IOUtils.rm(tempPath); 
      }
    } catch (ParseException e) {
      throw new IOException("Unable to load hunspell data! [dictionary=" + dictionaries + ",affix=" + affixFile + "]", e);
    } finally {
      IOUtils.closeWhileHandlingException(affix);
      IOUtils.closeWhileHandlingException(dictionaries);
    }
  }

  @Override
  public TokenStream create(TokenStream tokenStream) {
    return new HunspellStemFilter(tokenStream, dictionary, true, longestOnly);
  }
}
