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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.util.plugin.ResourceLoaderAware;

/**
 * TokenFilterFactory that creates instances of {@link org.apache.lucene.analysis.hunspell.HunspellStemFilter}.
 * Example config for British English including a custom dictionary, case insensitive matching:
 * <pre class="prettyprint" >
 * &lt;filter class=&quot;solr.HunspellStemFilterFactory&quot;
 *    dictionary=&quot;en_GB.dic,my_custom.dic&quot;
 *    affix=&quot;en_GB.aff&quot;
 *    ignoreCase=&quot;true&quot; /&gt;</pre>
 * Both parameters dictionary and affix are mandatory.
 * <br/>
 * The parameter ignoreCase (true/false) controls whether matching is case sensitive or not. Default false.
 * <br/> 
 * Dictionaries for many languages are available through the OpenOffice project.
 * 
 * See <a href="http://wiki.apache.org/solr/Hunspell">http://wiki.apache.org/solr/Hunspell</a>
 */
public class HunspellStemFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  
  private static final String PARAM_DICTIONARY = "dictionary";
  private static final String PARAM_AFFIX = "affix";
  private static final String PARAM_IGNORE_CASE = "ignoreCase";
  private static final String TRUE = "true";
  private static final String FALSE = "false";
  
  private HunspellDictionary dictionary;
  private boolean ignoreCase = false;

  /**
   * Loads the hunspell dictionary and affix files defined in the configuration
   *  
   * @param loader ResourceLoader used to load the files
   */
  public void inform(ResourceLoader loader) {
    assureMatchVersion();
    String dictionaryFiles[] = args.get(PARAM_DICTIONARY).split(",");
    String affixFile = args.get(PARAM_AFFIX);
    String pic = args.get(PARAM_IGNORE_CASE);
    if(pic != null) {
      if(pic.equalsIgnoreCase(TRUE)) ignoreCase = true;
      else if(pic.equalsIgnoreCase(FALSE)) ignoreCase = false;
      else throw new SolrException(ErrorCode.UNKNOWN, "Unknown value for "+PARAM_IGNORE_CASE+": "+pic+". Must be true or false");
    }

    try {
      List<InputStream> dictionaries = new ArrayList<InputStream>();
      for (String file : dictionaryFiles) {
        dictionaries.add(loader.openResource(file));
      }
      this.dictionary = new HunspellDictionary(loader.openResource(affixFile), dictionaries, luceneMatchVersion, ignoreCase);
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
