package org.apache.lucene.analysis.hunspell2;

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
import java.text.ParseException;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * TokenFilterFactory that creates instances of {@link Hunspell2StemFilter}.
 * Example config for British English:
 * <pre class="prettyprint">
 * &lt;filter class=&quot;solr.Hunspell2StemFilterFactory&quot;
 *         dictionary=&quot;en_GB.dic&quot;
 *         affix=&quot;en_GB.aff&quot; /&gt;</pre>
 * Both parameters dictionary and affix are mandatory.
 * Dictionaries for many languages are available through the OpenOffice project.
 * 
 * See <a href="http://wiki.apache.org/solr/Hunspell">http://wiki.apache.org/solr/Hunspell</a>
 * @lucene.experimental
 */
public class Hunspell2StemFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  private static final String PARAM_DICTIONARY    = "dictionary";
  private static final String PARAM_AFFIX         = "affix";
  private static final String PARAM_RECURSION_CAP = "recursionCap";

  private final String dictionaryFile;
  private final String affixFile;
  private Dictionary dictionary;
  private int recursionCap;
  
  /** Creates a new Hunspell2StemFilterFactory */
  public Hunspell2StemFilterFactory(Map<String,String> args) {
    super(args);
    dictionaryFile = require(args, PARAM_DICTIONARY);
    affixFile = get(args, PARAM_AFFIX);
    recursionCap = getInt(args, PARAM_RECURSION_CAP, 2);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    try (InputStream affix = loader.openResource(affixFile);
        InputStream dictionary = loader.openResource(dictionaryFile)) {
      try {
        this.dictionary = new Dictionary(affix, dictionary);
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public TokenStream create(TokenStream tokenStream) {
    return new Hunspell2StemFilter(tokenStream, dictionary, recursionCap);
  }
}
