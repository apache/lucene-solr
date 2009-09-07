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

package org.apache.solr.handler.component;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.*;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * A SearchComponent implementation which provides support for spell checking
 * and suggestions using the Lucene contributed SpellChecker.
 *
 * <p>
 * Refer to http://wiki.apache.org/solr/SpellCheckComponent for more details
 * </p>
 * 
 * @since solr 1.3
 */
public class SpellCheckComponent extends SearchComponent implements SolrCoreAware, SpellingParams {
  private static final Logger LOG = LoggerFactory.getLogger(SpellCheckComponent.class);

  public static final boolean DEFAULT_ONLY_MORE_POPULAR = false;

  /**
   * Base name for all spell checker query parameters. This name is also used to
   * register this component with SearchHandler.
   */
  public static final String COMPONENT_NAME = "spellcheck";

  @SuppressWarnings("unchecked")
  protected NamedList initParams;
  

  /**
   * Key is the dictionary, value is the SpellChecker for that dictionary name
   */
  protected Map<String, SolrSpellChecker> spellCheckers = new ConcurrentHashMap<String, SolrSpellChecker>();

  protected QueryConverter queryConverter;

  @Override
  @SuppressWarnings("unchecked")
  public void init(NamedList args) {
    super.init(args);
    this.initParams = args;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void prepare(ResponseBuilder rb) throws IOException {

    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }
    SolrSpellChecker spellChecker = getSpellChecker(params);
    if (params.getBool(SPELLCHECK_BUILD, false)) {
      spellChecker.build(rb.req.getCore(), rb.req.getSearcher());
      rb.rsp.add("command", "build");
    } else if (params.getBool(SPELLCHECK_RELOAD, false)) {
      spellChecker.reload();
      rb.rsp.add("command", "reload");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false) || spellCheckers.isEmpty()) {
      return;
    }
    String q = params.get(SPELLCHECK_Q);
    SolrSpellChecker spellChecker = getSpellChecker(params);
    Collection<Token> tokens = null;
    if (q != null) {
      //we have a spell check param, tokenize it with the query analyzer applicable for this spellchecker
      tokens = getTokens(q, spellChecker.getQueryAnalyzer());
    } else {
      q = rb.getQueryString();
      if (q == null) {
        q = params.get(CommonParams.Q);
      }
      tokens = queryConverter.convert(q);
    }
    if (tokens != null && tokens.isEmpty() == false) {
      if (spellChecker != null) {
        int count = params.getInt(SPELLCHECK_COUNT, 1);
        boolean onlyMorePopular = params.getBool(SPELLCHECK_ONLY_MORE_POPULAR,
            DEFAULT_ONLY_MORE_POPULAR);
        boolean extendedResults = params.getBool(SPELLCHECK_EXTENDED_RESULTS,
            false);
        NamedList response = new SimpleOrderedMap();
        IndexReader reader = rb.req.getSearcher().getReader();
        boolean collate = params.getBool(SPELLCHECK_COLLATE, false);
        SpellingResult spellingResult = spellChecker.getSuggestions(tokens,
            reader, count, onlyMorePopular, extendedResults);
        if (spellingResult != null) {
          response.add("suggestions", toNamedList(spellingResult, q,
              extendedResults, collate));
          rb.rsp.add("spellcheck", response);
        }

      } else {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
            "Specified dictionary does not exist.");
      }
    }
  }

  private Collection<Token> getTokens(String q, Analyzer analyzer) throws IOException {
    Collection<Token> result = new ArrayList<Token>();
    Token token = null;
    TokenStream ts = analyzer.reusableTokenStream("", new StringReader(q));
    ts.reset();
    while ((token = ts.next()) != null){
      result.add(token);
    }
    return result;
  }

  protected SolrSpellChecker getSpellChecker(SolrParams params) {
    String dictName = params.get(SPELLCHECK_DICT);
    if (dictName == null) {
      dictName = SolrSpellChecker.DEFAULT_DICTIONARY_NAME;
    }
    return spellCheckers.get(dictName);
  }
  
  /**
   * @return the spellchecker registered to a given name
   */
  public SolrSpellChecker getSpellChecker(String name) {
    return spellCheckers.get(name);
  }

  protected NamedList toNamedList(SpellingResult spellingResult, String origQuery, boolean extendedResults, boolean collate) {
    NamedList result = new NamedList();
    Map<Token, LinkedHashMap<String, Integer>> suggestions = spellingResult.getSuggestions();
    boolean hasFreqInfo = spellingResult.hasTokenFrequencyInfo();
    boolean isCorrectlySpelled = false;
    Map<Token, String> best = null;
    if (collate == true){
      best = new LinkedHashMap<Token, String>(suggestions.size());
    }
    
    // will be flipped to false if any of the suggestions are not in the index and hasFreqInfo is true
    if(suggestions.size() > 0) {
      isCorrectlySpelled = true;
    }
    
    for (Map.Entry<Token, LinkedHashMap<String, Integer>> entry : suggestions.entrySet()) {
      Token inputToken = entry.getKey();
      Map<String, Integer> theSuggestions = entry.getValue();
      if (theSuggestions != null && theSuggestions.size() > 0) {
        SimpleOrderedMap suggestionList = new SimpleOrderedMap();
        suggestionList.add("numFound", theSuggestions.size());
        suggestionList.add("startOffset", inputToken.startOffset());
        suggestionList.add("endOffset", inputToken.endOffset());
        if (extendedResults && hasFreqInfo) {
          suggestionList.add("origFreq", spellingResult.getTokenFrequency(inputToken));
          for (Map.Entry<String, Integer> suggEntry : theSuggestions.entrySet()) {
            SimpleOrderedMap<Object> suggestionItem = new SimpleOrderedMap<Object>();
            suggestionItem.add("frequency", suggEntry.getValue());
            suggestionItem.add("word", suggEntry.getKey());
            suggestionList.add("suggestion", suggestionItem);
          }
        } else {
          suggestionList.add("suggestion", theSuggestions.keySet());
        }
        if (collate == true ){//set aside the best suggestion for this token
          best.put(inputToken, theSuggestions.keySet().iterator().next());
        }
        if (hasFreqInfo) {
          isCorrectlySpelled = isCorrectlySpelled && spellingResult.getTokenFrequency(inputToken) > 0;
        }
        result.add(new String(inputToken.termBuffer(), 0, inputToken.termLength()), suggestionList);
      }
    }
    if (hasFreqInfo) {
      result.add("correctlySpelled", isCorrectlySpelled);
    } else if(extendedResults && suggestions.size() == 0) { // if the word is misspelled, its added to suggestions with freqinfo
      result.add("correctlySpelled", true);
    }
    if (collate == true){
      StringBuilder collation = new StringBuilder(origQuery);
      int offset = 0;
      for (Iterator<Map.Entry<Token, String>> bestIter = best.entrySet().iterator(); bestIter.hasNext();) {
        Map.Entry<Token, String> entry = bestIter.next();
        Token tok = entry.getKey();
        collation.replace(tok.startOffset() + offset, 
          tok.endOffset() + offset, entry.getValue());
        offset += entry.getValue().length() - (tok.endOffset() - tok.startOffset());
      }
      String collVal = collation.toString();
      if (collVal.equals(origQuery) == false) {
        LOG.debug("Collation:" + collation);
        result.add("collation", collVal);
      }
    }
    return result;
  }

  public void inform(SolrCore core) {
    if (initParams != null) {
      LOG.info("Initializing spell checkers");
      boolean hasDefault = false;
      for (int i = 0; i < initParams.size(); i++) {
        if (initParams.getName(i).equals("spellchecker")) {
          NamedList spellchecker = (NamedList) initParams.getVal(i);
          String className = (String) spellchecker.get("classname");
          if (className == null)
            className = IndexBasedSpellChecker.class.getName();
          SolrResourceLoader loader = core.getResourceLoader();
          SolrSpellChecker checker = (SolrSpellChecker) loader.newInstance(className);
          if (checker != null) {
            String dictionary = checker.init(spellchecker, core);
            if (dictionary != null) {
              boolean isDefault = dictionary.equals(SolrSpellChecker.DEFAULT_DICTIONARY_NAME);
              if (isDefault == true && hasDefault == false){
                hasDefault = true;
              } else if (isDefault == true && hasDefault == true){
                throw new RuntimeException("More than one dictionary is missing name.");
              }
              spellCheckers.put(dictionary, checker);
            } else {
              if (hasDefault == false){
                spellCheckers.put(SolrSpellChecker.DEFAULT_DICTIONARY_NAME, checker);
                hasDefault = true;
              } else {
                throw new RuntimeException("More than one dictionary is missing name.");
              }
            }
            // Register event listeners for this SpellChecker
            core.registerFirstSearcherListener(new SpellCheckerListener(core, checker, false, false));
            boolean buildOnCommit = Boolean.parseBoolean((String) spellchecker.get("buildOnCommit"));
            boolean buildOnOptimize = Boolean.parseBoolean((String) spellchecker.get("buildOnOptimize"));
            if (buildOnCommit || buildOnOptimize)   {
              LOG.info("Registering newSearcher listener for spellchecker: " + checker.getDictionaryName());
              core.registerNewSearcherListener(new SpellCheckerListener(core, checker, buildOnCommit, buildOnOptimize));
            }
          } else {
            throw new RuntimeException("Can't load spell checker: " + className);
          }
        }
     }

      Map<String, QueryConverter> queryConverters = new HashMap<String, QueryConverter>();
      core.initPlugins(queryConverters,QueryConverter.class);

      //ensure that there is at least one query converter defined
      if (queryConverters.size() == 0) {
        LOG.warn("No queryConverter defined, using default converter");
        queryConverters.put("queryConverter", new SpellingQueryConverter());
      }

      //there should only be one
      if (queryConverters.size() == 1) {
        queryConverter = queryConverters.values().iterator().next();
        IndexSchema schema = core.getSchema();
        String fieldTypeName = (String) initParams.get("queryAnalyzerFieldType");
        FieldType fieldType = schema.getFieldTypes().get(fieldTypeName);
        Analyzer analyzer = fieldType == null ? new WhitespaceAnalyzer()
                : fieldType.getQueryAnalyzer();
        //TODO: There's got to be a better way!  Where's Spring when you need it?
        queryConverter.setAnalyzer(analyzer);
      }
    }
  }

  private static class SpellCheckerListener implements SolrEventListener {
    private final SolrCore core;
    private final SolrSpellChecker checker;
    private final boolean buildOnCommit;
    private final boolean buildOnOptimize;

    public SpellCheckerListener(SolrCore core, SolrSpellChecker checker, boolean buildOnCommit, boolean buildOnOptimize) {
      this.core = core;
      this.checker = checker;
      this.buildOnCommit = buildOnCommit;
      this.buildOnOptimize = buildOnOptimize;
    }

    public void init(NamedList args) {
    }

    public void newSearcher(SolrIndexSearcher newSearcher,
                            SolrIndexSearcher currentSearcher) {
      if (currentSearcher == null) {
        // firstSearcher event
        try {
          LOG.info("Loading spell index for spellchecker: "
                  + checker.getDictionaryName());
          checker.reload();
        } catch (IOException e) {
          log.error( "Exception in reloading spell check index for spellchecker: " + checker.getDictionaryName(), e);
        }
      } else {
        // newSearcher event
        if (buildOnCommit)  {
          buildSpellIndex(newSearcher);
        } else if (buildOnOptimize) {
          if (newSearcher.getReader().isOptimized())  {
            buildSpellIndex(newSearcher);
          } else  {
            LOG.info("Index is not optimized therefore skipping building spell check index for: " + checker.getDictionaryName());
          }
        }
      }

    }

    private void buildSpellIndex(SolrIndexSearcher newSearcher) {
      try {
        LOG.info("Building spell index for spell checker: " + checker.getDictionaryName());
        checker.build(core, newSearcher);
      } catch (Exception e) {
        log.error(
                "Exception in building spell check index for spellchecker: " + checker.getDictionaryName(), e);
      }
    }

    public void postCommit() {
    }
  }

  public Map<String, SolrSpellChecker> getSpellCheckers() {
    return Collections.unmodifiableMap(spellCheckers);
  }

  // ///////////////////////////////////////////
  // / SolrInfoMBean
  // //////////////////////////////////////////

  @Override
  public String getDescription() {
    return "A Spell Checker component";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

}
