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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.spelling.IndexBasedSpellChecker;
import org.apache.solr.spelling.SolrSpellChecker;
import org.apache.solr.spelling.SpellingResult;
import org.apache.solr.spelling.QueryConverter;
import org.apache.solr.util.plugin.NamedListPluginLoader;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

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
public class SpellCheckComponent extends SearchComponent implements SolrCoreAware {
  private static final Logger LOG = Logger.getLogger(SpellCheckComponent.class.getName());

  private static WhitespaceAnalyzer whitespace = new WhitespaceAnalyzer();

  public static final boolean DEFAULT_ONLY_MORE_POPULAR = false;

  /**
   * Base name for all spell checker query parameters. This name is also used to
   * register this component with SearchHandler.
   */
  public static final String COMPONENT_NAME = "spellcheck";

  public static final String SPELLCHECK_PREFIX = "spellcheck.";

  /**
   * The name of the dictionary to be used for giving the suggestion for a
   * request. The value for this parameter is configured in solrconfig.xml
   */
  public static final String SPELLCHECK_DICT = SPELLCHECK_PREFIX + "dictionary";

  /**
   * The count of suggestions needed for a given query.
   * <p/>
   * If this parameter is absent in the request then only one suggestion is
   * returned. If it is more than one then a maximum of given suggestions are
   * returned for each token in the query.
   */
  public static final String SPELLCHECK_COUNT = SPELLCHECK_PREFIX + "count";

  /**
   * When this parameter is set to true and the misspelled word exists in the
   * user field, only words that occur more frequently in the Solr field than
   * the one given will be returned. The default value is false.
   * <p/>
   * <b>This is applicable only for dictionaries built from Solr fields.</b>
   */
  public static final String SPELLCHECK_ONLY_MORE_POPULAR = SPELLCHECK_PREFIX + "onlyMorePopular";

  /**
   * Whether to use the extended response format, which is more complicated but
   * richer. Returns the document frequency for each suggestion and returns one
   * suggestion block for each term in the query string. Default is false.
   * <p/>
   * <b>This is applicable only for dictionaries built from Solr fields.</b>
   */
  public static final String SPELLCHECK_EXTENDED_RESULTS = SPELLCHECK_PREFIX + "extendedResults";

  /**
   * Use the value for this parameter as the query to spell check.
   * <p/>
   * This parameter is <b>optional</b>. If absent, then the q parameter is
   * used.
   */
  public static final String SPELLCHECK_Q = SPELLCHECK_PREFIX + "q";

  /**
   * Whether to build the index or not. Optional and false by default.
   */
  public static final String SPELLCHECK_BUILD = SPELLCHECK_PREFIX + "build";

  /**
   * Whether to reload the index. Optional and false by default.
   */
  public static final String SPELLCHECK_RELOAD = SPELLCHECK_PREFIX + "reload";

  /**
   * Take the top suggestion for each token and create a new query from it
   */
  public static final String SPELLCHECK_COLLATE = SPELLCHECK_PREFIX + "collate";

  @SuppressWarnings("unchecked")
  protected NamedList initParams;
  
  @SuppressWarnings("unchecked")
  protected SolrParams defaults;

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
    if (args != null)   {
      Object o = args.get("defaults");
      if (o != null && o instanceof NamedList) {
        defaults = SolrParams.toSolrParams((NamedList)o);
      }
    }
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
      spellChecker.build(rb.req.getCore());
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
      q = params.get(CommonParams.Q);
      tokens = queryConverter.convert(q);
    }
    if (tokens != null && tokens.isEmpty() == false) {
      if (spellChecker != null) {
        int count = params.getInt(SPELLCHECK_COUNT, defaults.getInt(
            SPELLCHECK_COUNT, 1));
        boolean onlyMorePopular = params.getBool(SPELLCHECK_ONLY_MORE_POPULAR,
            defaults.getBool(SPELLCHECK_ONLY_MORE_POPULAR,
                DEFAULT_ONLY_MORE_POPULAR));
        boolean extendedResults = params.getBool(SPELLCHECK_EXTENDED_RESULTS,
            defaults.getBool(SPELLCHECK_EXTENDED_RESULTS, false));
        NamedList response = new SimpleOrderedMap();
        IndexReader reader = rb.req.getSearcher().getReader();
        boolean collate = params.getBool(SPELLCHECK_COLLATE, defaults.getBool(
            SPELLCHECK_COLLATE, false));
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
    TokenStream ts = analyzer.tokenStream("", new StringReader(q));
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

  protected NamedList toNamedList(SpellingResult spellingResult, String origQuery, boolean extendedResults, boolean collate) {
    NamedList result = new NamedList();
    Map<Token, LinkedHashMap<String, Integer>> suggestions = spellingResult.getSuggestions();
    boolean hasFreqInfo = spellingResult.hasTokenFrequencyInfo();
    boolean isCorrectlySpelled = true;
    Map<Token, String> best = null;
    if (collate == true){
      best = new HashMap<Token, String>(suggestions.size());
    }
    for (Map.Entry<Token, LinkedHashMap<String, Integer>> entry : suggestions.entrySet()) {
      Token inputToken = entry.getKey();
      Map<String, Integer> theSuggestions = entry.getValue();
      if (theSuggestions != null && theSuggestions.size() > 0) {
        NamedList suggestionList = new NamedList();
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
    }
    if (collate == true){
      StringBuilder collation = new StringBuilder(origQuery);
      for (Iterator<Map.Entry<Token, String>> bestIter = best.entrySet().iterator(); bestIter.hasNext();) {
        Map.Entry<Token, String> entry = bestIter.next();
        Token tok = entry.getKey();
        collation.replace(tok.startOffset(), tok.endOffset(), entry.getValue());
      }
      String collVal = collation.toString();
      if (collVal.equals(origQuery) == false) {
        LOG.fine("Collation:" + collation);
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
            String dictionary = checker.init(spellchecker, loader);
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
          } else {
            throw new RuntimeException("Can't load spell checker: " + className);
          }
        }
     }
      String xpath = "queryConverter";
      SolrConfig solrConfig = core.getSolrConfig();
      NodeList nodes = (NodeList) solrConfig.evaluate(xpath, XPathConstants.NODESET);

      Map<String, QueryConverter> queryConverters = new HashMap<String, QueryConverter>();
      NamedListPluginLoader<QueryConverter> loader =
              new NamedListPluginLoader<QueryConverter>("[solrconfig.xml] " + xpath, queryConverters);

      loader.load(solrConfig.getResourceLoader(), nodes);
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
      } else {
        //TODO: Is there a better way?
        throw new RuntimeException("One and only one queryConverter may be defined");
      }
    }
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
    return "$Revision:$";
  }

  @Override
  public String getSourceId() {
    return "$Id:$";
  }

  @Override
  public String getSource() {
    return "$URL:$";
  }

}
