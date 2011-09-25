package org.apache.solr.analysis;

/**
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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.hunspell.HunspellDictionary;
import org.apache.lucene.analysis.hunspell.HunspellStemFilter;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * TokenFilterFactory that creates instances of {@link org.apache.lucene.analysis.hunspell.HunspellStemFilter}.
 * Example config for British English including a custom dictionary:
 * <pre class="prettyprint" >
 * &lt;filter class=&quot;solr.HunspellStemFilterFactory&quot;
 *    dictionary=&quot;en_GB.dic,my_custom.dic&quot;
 *    affix=&quot;en_GB.aff&quot;/&gt;</pre>
 * Dictionaries for many languages are available through the OpenOffice project
 * See http://wiki.services.openoffice.org/wiki/Dictionaries
 */
public class HunspellStemFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  
  private HunspellDictionary dictionary;

  /**
   * Loads the hunspell dictionary and affix files defined in the configuration
   *  
   * @param loader ResourceLoader used to load the files
   */
  public void inform(ResourceLoader loader) {
    assureMatchVersion();
    String dictionaryFiles[] = args.get("dictionary").split(",");
    String affixFile = args.get("affix");

    try {
      List<InputStream> dictionaries = new ArrayList<InputStream>();
      for (String file : dictionaryFiles) {
        dictionaries.add(loader.openResource(file));
      }
      this.dictionary = new HunspellDictionary(loader.openResource(affixFile), dictionaries, luceneMatchVersion);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load hunspell data! [dictionary=" + args.get("dictionary") + ",affix=" + affixFile + "]", e);
    }
  }

  /**
   * Creates an instance of {@link org.apache.lucene.analysis.hunspell.HunspellStemFilter} that will filter the given
   * TokenStream
   *
   * @param tokenStream TokenStream that will be filtered
   * @return HunspellStemFilter that filters the TokenStream 
   */
  public TokenStream create(TokenStream tokenStream) {
    return new HunspellStemFilter(tokenStream, dictionary);
  }
}
