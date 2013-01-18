package org.apache.lucene.analysis.hunspell;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.hunspell.HunspellDictionary;
import org.apache.lucene.analysis.hunspell.HunspellStemFilter;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.IOUtils;

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
 * The parameter strictAffixParsing (true/false) controls whether the affix parsing is strict or not. Default true.
 * If strict an error while reading an affix rule causes a ParseException, otherwise is ignored.
 * <br/>
 * Dictionaries for many languages are available through the OpenOffice project.
 * 
 * See <a href="http://wiki.apache.org/solr/Hunspell">http://wiki.apache.org/solr/Hunspell</a>
 */
public class HunspellStemFilterFactory extends TokenFilterFactory implements ResourceLoaderAware {
  
  private static final String PARAM_DICTIONARY = "dictionary";
  private static final String PARAM_AFFIX = "affix";
  private static final String PARAM_IGNORE_CASE = "ignoreCase";
  private static final String PARAM_STRICT_AFFIX_PARSING = "strictAffixParsing";
  private static final String TRUE = "true";
  private static final String FALSE = "false";
  
  private HunspellDictionary dictionary;
  private boolean ignoreCase = false;

  /**
   * Loads the hunspell dictionary and affix files defined in the configuration
   *  
   * @param loader ResourceLoader used to load the files
   */
  @Override
  public void inform(ResourceLoader loader) throws IOException {
    assureMatchVersion();
    String dictionaryArg = args.get(PARAM_DICTIONARY);
    if (dictionaryArg == null) {
      throw new IllegalArgumentException("Parameter " + PARAM_DICTIONARY + " is mandatory.");
    }
    String dictionaryFiles[] = args.get(PARAM_DICTIONARY).split(",");
    String affixFile = args.get(PARAM_AFFIX);
    String pic = args.get(PARAM_IGNORE_CASE);
    if(pic != null) {
      if(pic.equalsIgnoreCase(TRUE)) ignoreCase = true;
      else if(pic.equalsIgnoreCase(FALSE)) ignoreCase = false;
      else throw new IllegalArgumentException("Unknown value for " + PARAM_IGNORE_CASE + ": " + pic + ". Must be true or false");
    }

    String strictAffixParsingParam = args.get(PARAM_STRICT_AFFIX_PARSING);
    boolean strictAffixParsing = true;
    if(strictAffixParsingParam != null) {
      if(strictAffixParsingParam.equalsIgnoreCase(FALSE)) strictAffixParsing = false;
      else if(strictAffixParsingParam.equalsIgnoreCase(TRUE)) strictAffixParsing = true;
      else throw new IllegalArgumentException("Unknown value for " + PARAM_STRICT_AFFIX_PARSING + ": " + strictAffixParsingParam + ". Must be true or false");
    }

    InputStream affix = null;
    List<InputStream> dictionaries = new ArrayList<InputStream>();

    try {
      dictionaries = new ArrayList<InputStream>();
      for (String file : dictionaryFiles) {
        dictionaries.add(loader.openResource(file));
      }
      affix = loader.openResource(affixFile);

      this.dictionary = new HunspellDictionary(affix, dictionaries, luceneMatchVersion, ignoreCase, strictAffixParsing);
    } catch (ParseException e) {
      throw new IOException("Unable to load hunspell data! [dictionary=" + args.get("dictionary") + ",affix=" + affixFile + "]", e);
    } finally {
      IOUtils.closeWhileHandlingException(affix);
      IOUtils.closeWhileHandlingException(dictionaries);
    }
  }

  /**
   * Creates an instance of {@link org.apache.lucene.analysis.hunspell.HunspellStemFilter} that will filter the given
   * TokenStream
   *
   * @param tokenStream TokenStream that will be filtered
   * @return HunspellStemFilter that filters the TokenStream 
   */
  @Override
  public TokenStream create(TokenStream tokenStream) {
    return new HunspellStemFilter(tokenStream, dictionary);
  }
}
