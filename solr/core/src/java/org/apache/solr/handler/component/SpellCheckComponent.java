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

package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
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
import org.apache.solr.spelling.AbstractLuceneSpellChecker;
import org.apache.solr.spelling.ConjunctionSolrSpellChecker;
import org.apache.solr.spelling.IndexBasedSpellChecker;
import org.apache.solr.spelling.QueryConverter;
import org.apache.solr.spelling.SolrSpellChecker;
import org.apache.solr.spelling.SpellCheckCollation;
import org.apache.solr.spelling.SpellCheckCollator;
import org.apache.solr.spelling.SpellingOptions;
import org.apache.solr.spelling.SpellingQueryConverter;
import org.apache.solr.spelling.SpellingResult;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      spellChecker.reload(rb.req.getCore(), rb.req.getSearcher());
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
    boolean shardRequest = "true".equals(params.get(ShardParams.IS_SHARD));
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
        boolean onlyMorePopular = params.getBool(SPELLCHECK_ONLY_MORE_POPULAR, DEFAULT_ONLY_MORE_POPULAR);
        boolean extendedResults = params.getBool(SPELLCHECK_EXTENDED_RESULTS, false); 
        boolean collate = params.getBool(SPELLCHECK_COLLATE, false);
        float accuracy = params.getFloat(SPELLCHECK_ACCURACY, Float.MIN_VALUE);
        Integer alternativeTermCount = params.getInt(SpellingParams.SPELLCHECK_ALTERNATIVE_TERM_COUNT); 
        Integer maxResultsForSuggest = params.getInt(SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST);
        ModifiableSolrParams customParams = new ModifiableSolrParams();
        for (String checkerName : getDictionaryNames(params)) {
          customParams.add(getCustomParams(checkerName, params));
        }
        
        Integer hitsInteger = (Integer) rb.rsp.getToLog().get("hits");
        long hits = 0;
        if (hitsInteger == null) {
          hits = rb.getNumberDocumentsFound();
        } else {
          hits = hitsInteger.longValue();
        }
        SpellingResult spellingResult = null;
        if (maxResultsForSuggest == null || hits <= maxResultsForSuggest) {
          SuggestMode suggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
          if (onlyMorePopular) {
            suggestMode = SuggestMode.SUGGEST_MORE_POPULAR;
          } else if (alternativeTermCount != null) {
            suggestMode = SuggestMode.SUGGEST_ALWAYS;
          }
          
          IndexReader reader = rb.req.getSearcher().getIndexReader();
          SpellingOptions options = new SpellingOptions(tokens, reader, count,
              alternativeTermCount, suggestMode, extendedResults, accuracy,
              customParams);
          spellingResult = spellChecker.getSuggestions(options);
        } else {
          spellingResult = new SpellingResult();
        }
        boolean isCorrectlySpelled = hits > (maxResultsForSuggest==null ? 0 : maxResultsForSuggest);
        NamedList suggestions = toNamedList(shardRequest, spellingResult, q,
            extendedResults, collate, isCorrectlySpelled);
        if (collate) {
          addCollationsToResponse(params, spellingResult, rb, q, suggestions, spellChecker.isSuggestionsMayOverlap());
        }
        NamedList response = new SimpleOrderedMap();
        response.add("suggestions", suggestions);
        rb.rsp.add("spellcheck", response);

      } else {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
            "Specified dictionaries do not exist: " + getDictionaryNameAsSingleString(getDictionaryNames(params)));
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void addCollationsToResponse(SolrParams params, SpellingResult spellingResult, ResponseBuilder rb, String q,
      NamedList response, boolean suggestionsMayOverlap) {
    int maxCollations = params.getInt(SPELLCHECK_MAX_COLLATIONS, 1);
    int maxCollationTries = params.getInt(SPELLCHECK_MAX_COLLATION_TRIES, 0);
    int maxCollationEvaluations = params.getInt(SPELLCHECK_MAX_COLLATION_EVALUATIONS, 10000);
    boolean collationExtendedResults = params.getBool(SPELLCHECK_COLLATE_EXTENDED_RESULTS, false);
    int maxCollationCollectDocs = params.getInt(SPELLCHECK_COLLATE_MAX_COLLECT_DOCS, 0);
    // If not reporting hits counts, don't bother collecting more than 1 document per try.
    if (!collationExtendedResults) {
      maxCollationCollectDocs = 1;
    }
    boolean shard = params.getBool(ShardParams.IS_SHARD, false);
    SpellCheckCollator collator = new SpellCheckCollator()
        .setMaxCollations(maxCollations)
        .setMaxCollationTries(maxCollationTries)
        .setMaxCollationEvaluations(maxCollationEvaluations)
        .setSuggestionsMayOverlap(suggestionsMayOverlap)
        .setDocCollectionLimit(maxCollationCollectDocs)
    ;
    List<SpellCheckCollation> collations = collator.collate(spellingResult, q, rb);
    //by sorting here we guarantee a non-distributed request returns all 
    //results in the same order as a distributed request would,
    //even in cases when the internal rank is the same.
    Collections.sort(collations);

    for (SpellCheckCollation collation : collations) {
      if (collationExtendedResults) {
        NamedList extendedResult = new NamedList();
        extendedResult.add("collationQuery", collation.getCollationQuery());
        extendedResult.add("hits", collation.getHits());
        extendedResult.add("misspellingsAndCorrections", collation.getMisspellingsAndCorrections());
        if(maxCollationTries>0 && shard)
        {
          extendedResult.add("collationInternalRank", collation.getInternalRank());
        }
        response.add("collation", extendedResult);
      } else {
        response.add("collation", collation.getCollationQuery());
        if(maxCollationTries>0 && shard)
        {
          response.add("collationInternalRank", collation.getInternalRank());
        }
      }
    }
  }

  /**
   * For every param that is of the form "spellcheck.[dictionary name].XXXX=YYYY, add
   * XXXX=YYYY as a param to the custom param list
   * @param params The original SolrParams
   * @return The new Params
   */
  protected SolrParams getCustomParams(String dictionary, SolrParams params) {
    ModifiableSolrParams result = new ModifiableSolrParams();
    Iterator<String> iter = params.getParameterNamesIterator();
    String prefix = SpellingParams.SPELLCHECK_PREFIX + "." + dictionary + ".";
    while (iter.hasNext()){
      String nxt = iter.next();
      if (nxt.startsWith(prefix)){
        result.add(nxt.substring(prefix.length()), params.getParams(nxt));
      }
    }
    return result;
  }


  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) return;
    int purpose = rb.grouping() ? ShardRequest.PURPOSE_GET_TOP_GROUPS : ShardRequest.PURPOSE_GET_TOP_IDS;   
    if ((sreq.purpose & purpose) != 0) {
      // fetch at least 5 suggestions from each shard
      int count = sreq.params.getInt(SPELLCHECK_COUNT, 1);
      if (count < 5)  count = 5;
      sreq.params.set(SPELLCHECK_COUNT, count);
      sreq.params.set("spellcheck", "true");
    } else  {
      sreq.params.set("spellcheck", "false");
    }
  }

  @Override
  @SuppressWarnings({"unchecked", "deprecation"})
  public void finishStage(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false) || rb.stage != ResponseBuilder.STAGE_GET_FIELDS)
      return;

    boolean extendedResults = params.getBool(SPELLCHECK_EXTENDED_RESULTS, false);
    boolean collate = params.getBool(SPELLCHECK_COLLATE, false);
    boolean collationExtendedResults = params.getBool(SPELLCHECK_COLLATE_EXTENDED_RESULTS, false);
    int maxCollationTries = params.getInt(SPELLCHECK_MAX_COLLATION_TRIES, 0);
    int maxCollations = params.getInt(SPELLCHECK_MAX_COLLATIONS, 1);
    Integer maxResultsForSuggest = params.getInt(SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST); 
    int count = rb.req.getParams().getInt(SPELLCHECK_COUNT, 1);
    int numSug = Math.max(count, AbstractLuceneSpellChecker.DEFAULT_SUGGESTION_COUNT);

    String origQuery = params.get(SPELLCHECK_Q);
    if (origQuery == null) {
      origQuery = rb.getQueryString();
      if (origQuery == null) {
        origQuery = params.get(CommonParams.Q);
      }
    }
    
    long hits = rb.grouping() ? rb.totalHitCount : rb.getNumberDocumentsFound();
    boolean isCorrectlySpelled = hits > (maxResultsForSuggest==null ? 0 : maxResultsForSuggest);
    
    SpellCheckMergeData mergeData = new SpellCheckMergeData();  
    if (maxResultsForSuggest==null || !isCorrectlySpelled) {
      for (ShardRequest sreq : rb.finished) {
        for (ShardResponse srsp : sreq.responses) {
          NamedList nl = (NamedList) srsp.getSolrResponse().getResponse().get("spellcheck");
          LOG.info(srsp.getShard() + " " + nl);
          if (nl != null) {
            mergeData.totalNumberShardResponses++;
            collectShardSuggestions(nl, mergeData);
            collectShardCollations(mergeData, nl, maxCollationTries);
          }
        }
      }
    }

    // all shard responses have been collected
    // create token and get top suggestions
    SolrSpellChecker checker = getSpellChecker(rb.req.getParams());    
    SpellingResult result = checker.mergeSuggestions(mergeData, numSug, count, extendedResults);
    
    NamedList response = new SimpleOrderedMap();
    NamedList suggestions = toNamedList(false, result, origQuery,
        extendedResults, collate, isCorrectlySpelled);
    if (collate) {
      SpellCheckCollation[] sortedCollations = mergeData.collations.values()
          .toArray(new SpellCheckCollation[mergeData.collations.size()]);
      Arrays.sort(sortedCollations);
      int i = 0;
      while (i < maxCollations && i < sortedCollations.length) {
        SpellCheckCollation collation = sortedCollations[i];
        i++;
        if (collationExtendedResults) {
          NamedList extendedResult = new NamedList();
          extendedResult.add("collationQuery", collation.getCollationQuery());
          extendedResult.add("hits", collation.getHits());
          extendedResult.add("misspellingsAndCorrections", collation
              .getMisspellingsAndCorrections());
          suggestions.add("collation", extendedResult);
        } else {
          suggestions.add("collation", collation.getCollationQuery());
        }
      }
    }
    
    response.add("suggestions", suggestions);
    rb.rsp.add("spellcheck", response);
  }
    
  @SuppressWarnings("unchecked")
  private void collectShardSuggestions(NamedList nl, SpellCheckMergeData mergeData) {
    SpellCheckResponse spellCheckResp = new SpellCheckResponse(nl);
    for (SpellCheckResponse.Suggestion suggestion : spellCheckResp.getSuggestions()) {
      mergeData.origVsSuggestion.put(suggestion.getToken(), suggestion);
      HashSet<String> suggested = mergeData.origVsSuggested.get(suggestion.getToken());
      if (suggested == null) {
        suggested = new HashSet<String>();
        mergeData.origVsSuggested.put(suggestion.getToken(), suggested);
      }

      // sum up original frequency          
      int origFreq = 0;
      Integer o = mergeData.origVsFreq.get(suggestion.getToken());
      if (o != null)  origFreq += o;
      origFreq += suggestion.getOriginalFrequency();
      mergeData.origVsFreq.put(suggestion.getToken(), origFreq);
      
      //# shards reporting
      Integer origShards = mergeData.origVsShards.get(suggestion.getToken());
      if(origShards==null) {
        mergeData.origVsShards.put(suggestion.getToken(), 1);
      } else {
        mergeData.origVsShards.put(suggestion.getToken(), ++origShards);
      }            

      // find best suggestions
      for (int i = 0; i < suggestion.getNumFound(); i++) {
        String alternative = suggestion.getAlternatives().get(i);
        suggested.add(alternative);
        SuggestWord sug = mergeData.suggestedVsWord.get(alternative);
        if (sug == null)  {
          sug = new SuggestWord();
          mergeData.suggestedVsWord.put(alternative, sug);
        }
        sug.string = alternative;
        // alternative frequency is present only for extendedResults=true
        if (suggestion.getAlternativeFrequencies() != null
            && suggestion.getAlternativeFrequencies().size() > 0) {
          Integer freq = suggestion.getAlternativeFrequencies().get(i);
          if (freq != null) sug.freq += freq;
        }
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private void collectShardCollations(SpellCheckMergeData mergeData, NamedList spellCheckResponse, int maxCollationTries) {
    Map<String, SpellCheckCollation> collations = mergeData.collations;
    NamedList suggestions = (NamedList) spellCheckResponse.get("suggestions");
    if(suggestions != null) {
      List<Object> collationList = suggestions.getAll("collation");
      List<Object> collationRankList = suggestions.getAll("collationInternalRank");
      int i=0;
      if(collationList != null) {
        for(Object o : collationList)
        {
          if(o instanceof String)
          {
            SpellCheckCollation coll = new SpellCheckCollation();
            coll.setCollationQuery((String) o);
            if(collationRankList!= null && collationRankList.size()>0)
            {
              coll.setInternalRank((Integer) collationRankList.get(i));
              i++;
            }
            SpellCheckCollation priorColl = collations.get(coll.getCollationQuery());
            if(priorColl != null)
            {
              coll.setInternalRank(Math.max(coll.getInternalRank(),priorColl.getInternalRank()));
            }
            collations.put(coll.getCollationQuery(), coll);
          } else
          {
            NamedList expandedCollation = (NamedList) o;                  
            SpellCheckCollation coll = new SpellCheckCollation();
            coll.setCollationQuery((String) expandedCollation.get("collationQuery"));
            coll.setHits((Integer) expandedCollation.get("hits"));
            if(maxCollationTries>0)
            {
              coll.setInternalRank((Integer) expandedCollation.get("collationInternalRank"));
            }
            coll.setMisspellingsAndCorrections((NamedList) expandedCollation.get("misspellingsAndCorrections"));
            SpellCheckCollation priorColl = collations.get(coll.getCollationQuery());
            if(priorColl != null)
            {
              coll.setHits(coll.getHits() + priorColl.getHits());
              coll.setInternalRank(Math.max(coll.getInternalRank(),priorColl.getInternalRank()));
            }
            collations.put(coll.getCollationQuery(), coll);
          }
        }
      }
    }
  }

  private Collection<Token> getTokens(String q, Analyzer analyzer) throws IOException {
    Collection<Token> result = new ArrayList<Token>();
    assert analyzer != null;
    TokenStream ts = analyzer.tokenStream("", q);
    ts.reset();
    // TODO: support custom attributes
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
    TypeAttribute typeAtt = ts.addAttribute(TypeAttribute.class);
    FlagsAttribute flagsAtt = ts.addAttribute(FlagsAttribute.class);
    PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);
    PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
    
    while (ts.incrementToken()){
      Token token = new Token();
      token.copyBuffer(termAtt.buffer(), 0, termAtt.length());
      token.setOffset(offsetAtt.startOffset(), offsetAtt.endOffset());
      token.setType(typeAtt.type());
      token.setFlags(flagsAtt.getFlags());
      token.setPayload(payloadAtt.getPayload());
      token.setPositionIncrement(posIncAtt.getPositionIncrement());
      result.add(token);
    }
    ts.end();
    ts.close();
    return result;
  }

  protected SolrSpellChecker getSpellChecker(SolrParams params) {
    String[] dictName = getDictionaryNames(params);
    if (dictName.length == 1) {
      return spellCheckers.get(dictName[0]);
    } else {
      String singleStr = getDictionaryNameAsSingleString(dictName);
      SolrSpellChecker ssc = spellCheckers.get(singleStr);
      if (ssc == null) {
        ConjunctionSolrSpellChecker cssc = new ConjunctionSolrSpellChecker();
        for (String dn : dictName) {
          cssc.addChecker(spellCheckers.get(dn));
        }
        ssc = cssc;
      }
      return ssc;
    }
  }
  
  private String getDictionaryNameAsSingleString(String[] dictName) {
    StringBuilder sb = new StringBuilder();
    for (String dn : dictName) {
      if (sb.length() > 0) {
        sb.append(" ");
      }
      sb.append(dn);
    }
    return sb.toString();
  }

  private String[] getDictionaryNames(SolrParams params) {
    String[] dictName = params.getParams(SPELLCHECK_DICT);
    if (dictName == null) {
      return new String[] {SolrSpellChecker.DEFAULT_DICTIONARY_NAME};
    }
    return dictName;
  }

  /**
   * @return the spellchecker registered to a given name
   */
  public SolrSpellChecker getSpellChecker(String name) {
    return spellCheckers.get(name);
  }

  protected NamedList toNamedList(boolean shardRequest,
      SpellingResult spellingResult, String origQuery, boolean extendedResults,
      boolean collate, boolean correctlySpelled) {
    NamedList result = new NamedList();
    Map<Token,LinkedHashMap<String,Integer>> suggestions = spellingResult
        .getSuggestions();
    boolean hasFreqInfo = spellingResult.hasTokenFrequencyInfo();
    boolean hasSuggestions = false;
    boolean hasZeroFrequencyToken = false;
    for (Map.Entry<Token,LinkedHashMap<String,Integer>> entry : suggestions
        .entrySet()) {
      Token inputToken = entry.getKey();
      String tokenString = new String(inputToken.buffer(), 0, inputToken
          .length());
      Map<String,Integer> theSuggestions = new LinkedHashMap<String,Integer>(
          entry.getValue());
      Iterator<String> sugIter = theSuggestions.keySet().iterator();
      while (sugIter.hasNext()) {
        String sug = sugIter.next();
        if (sug.equals(tokenString)) {
          sugIter.remove();
        }
      }
      if (theSuggestions.size() > 0) {
        hasSuggestions = true;
      }
      if (theSuggestions != null && (theSuggestions.size() > 0 || shardRequest)) {
        SimpleOrderedMap suggestionList = new SimpleOrderedMap();
        suggestionList.add("numFound", theSuggestions.size());
        suggestionList.add("startOffset", inputToken.startOffset());
        suggestionList.add("endOffset", inputToken.endOffset());
        
        // Logical structure of normal (non-extended) results:
        // "suggestion":["alt1","alt2"]
        //
        // Logical structure of the extended results:
        // "suggestion":[
        // {"word":"alt1","freq":7},
        // {"word":"alt2","freq":4}
        // ]
        if (extendedResults && hasFreqInfo) {
          suggestionList.add("origFreq", spellingResult
              .getTokenFrequency(inputToken));
          
          ArrayList<SimpleOrderedMap> sugs = new ArrayList<SimpleOrderedMap>();
          suggestionList.add("suggestion", sugs);
          for (Map.Entry<String,Integer> suggEntry : theSuggestions.entrySet()) {
            SimpleOrderedMap sugEntry = new SimpleOrderedMap();
            sugEntry.add("word", suggEntry.getKey());
            sugEntry.add("freq", suggEntry.getValue());
            sugs.add(sugEntry);
          }
        } else {
          suggestionList.add("suggestion", theSuggestions.keySet());
        }
        
        if (hasFreqInfo) {
          int tokenFrequency = spellingResult.getTokenFrequency(inputToken);
          if (tokenFrequency == 0) {
            hasZeroFrequencyToken = true;
          }
        }
        result.add(tokenString, suggestionList);
      }
    }
    
    if (extendedResults) {
      result.add("correctlySpelled", correctlySpelled);     
    }
    return result;
  }

  @Override
  public void inform(SolrCore core) {
    if (initParams != null) {
      LOG.info("Initializing spell checkers");
      boolean hasDefault = false;
      for (int i = 0; i < initParams.size(); i++) {
        if (initParams.getName(i).equals("spellchecker")) {
          NamedList spellchecker = (NamedList) initParams.getVal(i);
          String className = (String) spellchecker.get("classname");
          // TODO: this is a little bit sneaky: warn if class isnt supplied
          // so that its mandatory in a future release?
          if (className == null)
            className = IndexBasedSpellChecker.class.getName();
          SolrResourceLoader loader = core.getResourceLoader();
          SolrSpellChecker checker = loader.newInstance(className, SolrSpellChecker.class);
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
        LOG.info("No queryConverter defined, using default converter");
        queryConverters.put("queryConverter", new SpellingQueryConverter());
      }

      //there should only be one
      if (queryConverters.size() == 1) {
        queryConverter = queryConverters.values().iterator().next();
        IndexSchema schema = core.getLatestSchema();
        String fieldTypeName = (String) initParams.get("queryAnalyzerFieldType");
        FieldType fieldType = schema.getFieldTypes().get(fieldTypeName);
        Analyzer analyzer = fieldType == null ? new WhitespaceAnalyzer(core.getSolrConfig().luceneMatchVersion)
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

    @Override
    public void init(NamedList args) {
    }

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher,
                            SolrIndexSearcher currentSearcher) {
      if (currentSearcher == null) {
        // firstSearcher event
        try {
          LOG.info("Loading spell index for spellchecker: "
                  + checker.getDictionaryName());
          checker.reload(core, newSearcher);
        } catch (IOException e) {
          log.error( "Exception in reloading spell check index for spellchecker: " + checker.getDictionaryName(), e);
        }
      } else {
        // newSearcher event
        if (buildOnCommit)  {
          buildSpellIndex(newSearcher);
        } else if (buildOnOptimize) {
          if (newSearcher.getIndexReader().leaves().size() == 1)  {
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

    @Override
    public void postCommit() {
    }

    @Override
    public void postSoftCommit() {
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
  public String getSource() {
    return "$URL$";
  }

}
