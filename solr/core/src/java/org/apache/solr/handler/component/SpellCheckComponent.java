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
import java.lang.invoke.MethodHandles;
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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
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
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
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
import org.apache.solr.spelling.Token;
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
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final boolean DEFAULT_ONLY_MORE_POPULAR = false;

  /**
   * Base name for all spell checker query parameters. This name is also used to
   * register this component with SearchHandler.
   */
  public static final String COMPONENT_NAME = "spellcheck";

  @SuppressWarnings({"rawtypes"})
  protected NamedList initParams;


  /**
   * Key is the dictionary, value is the SpellChecker for that dictionary name
   */
  protected Map<String, SolrSpellChecker> spellCheckers = new ConcurrentHashMap<>();

  protected QueryConverter queryConverter;

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
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

    SolrSpellChecker spellChecker = getSpellChecker(params);
    if (spellChecker != null) {
      Collection<Token> tokens;
      String q = params.get(SPELLCHECK_Q);
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
        int count = params.getInt(SPELLCHECK_COUNT, 1);
        boolean onlyMorePopular = params.getBool(SPELLCHECK_ONLY_MORE_POPULAR, DEFAULT_ONLY_MORE_POPULAR);
        boolean extendedResults = params.getBool(SPELLCHECK_EXTENDED_RESULTS, false);
        boolean collate = params.getBool(SPELLCHECK_COLLATE, false);
        float accuracy = params.getFloat(SPELLCHECK_ACCURACY, Float.MIN_VALUE);
        int alternativeTermCount = params.getInt(SpellingParams.SPELLCHECK_ALTERNATIVE_TERM_COUNT, 0);
        //If specified, this can be a discrete # of results, or a percentage of fq results.
        Integer maxResultsForSuggest = maxResultsForSuggest(rb);

        ModifiableSolrParams customParams = new ModifiableSolrParams();
        for (String checkerName : getDictionaryNames(params)) {
          customParams.add(getCustomParams(checkerName, params));
        }

        Number hitsLong = (Number) rb.rsp.getToLog().get("hits");
        long hits = 0;
        if (hitsLong == null) {
          hits = rb.getNumberDocumentsFound();
        } else {
          hits = hitsLong.longValue();
        }

        SpellingResult spellingResult = null;
        if (maxResultsForSuggest == null || hits <= maxResultsForSuggest) {
          SuggestMode suggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
          if (onlyMorePopular) {
            suggestMode = SuggestMode.SUGGEST_MORE_POPULAR;
          } else if (alternativeTermCount > 0) {
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

        @SuppressWarnings({"rawtypes"})
        NamedList response = new SimpleOrderedMap();
        @SuppressWarnings({"rawtypes"})
        NamedList suggestions = toNamedList(shardRequest, spellingResult, q, extendedResults);
        response.add("suggestions", suggestions);

        if (extendedResults) {
          response.add("correctlySpelled", isCorrectlySpelled);
        }
        if (collate) {
          addCollationsToResponse(params, spellingResult, rb, q, response, spellChecker.isSuggestionsMayOverlap());
        }
        if (shardRequest) {
          addOriginalTermsToResponse(response, tokens);
        }

        rb.rsp.add("spellcheck", response);
      }
    } else {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
            "Specified dictionaries do not exist: " + getDictionaryNameAsSingleString(getDictionaryNames(params)));
    }
  }

  private Integer maxResultsForSuggest(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    float maxResultsForSuggestParamValue = params.getFloat(SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST, 0.0f);
    Integer maxResultsForSuggest = null;

    if (maxResultsForSuggestParamValue > 0.0f) {
      if (maxResultsForSuggestParamValue == (int) maxResultsForSuggestParamValue) {
        // If a whole number was passed in, this is a discrete number of documents
        maxResultsForSuggest = (int) maxResultsForSuggestParamValue;
      } else {
        // If a fractional value was passed in, this is the % of documents returned by the specified filter
        // If no specified filter, we use the most restrictive filter of the fq parameters
        String maxResultsFilterQueryString = params.get(SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST_FQ);

        int maxResultsByFilters = Integer.MAX_VALUE;
        SolrIndexSearcher searcher = rb.req.getSearcher();

        try {
          if (maxResultsFilterQueryString != null) {
            // Get the default Lucene query parser
            QParser parser = QParser.getParser(maxResultsFilterQueryString, rb.req);
            DocSet s = searcher.getDocSet(parser.getQuery());
            maxResultsByFilters = s.size();
          } else {
            List<Query> filters = rb.getFilters();

            // Get the maximum possible hits within these filters (size of most restrictive filter).
            if (filters != null) {
              for (Query query : filters) {
                DocSet s = searcher.getDocSet(query);
                if (s != null) {
                  maxResultsByFilters = Math.min(s.size(), maxResultsByFilters);
                }
              }
            }
          }
        } catch (IOException e){
          log.error("Error", e);
          return null;
        } catch (SyntaxError e) {
          log.error("Error", e);
          return null;
        }

        // Recalculate maxResultsForSuggest if filters were specified
        if (maxResultsByFilters != Integer.MAX_VALUE) {
          maxResultsForSuggest = Math.round(maxResultsByFilters * maxResultsForSuggestParamValue);
        }
      }
    }
    return maxResultsForSuggest;
  }

  @SuppressWarnings("unchecked")
  protected void addCollationsToResponse(SolrParams params, SpellingResult spellingResult, ResponseBuilder rb, String q,
      @SuppressWarnings({"rawtypes"})NamedList response, boolean suggestionsMayOverlap) {
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

    @SuppressWarnings({"rawtypes"})
    NamedList collationList = new NamedList();
    for (SpellCheckCollation collation : collations) {
      if (collationExtendedResults) {
        @SuppressWarnings({"rawtypes"})
        NamedList extendedResult = new SimpleOrderedMap();
        extendedResult.add("collationQuery", collation.getCollationQuery());
        extendedResult.add("hits", collation.getHits());
        extendedResult.add("misspellingsAndCorrections", collation.getMisspellingsAndCorrections());
        if(maxCollationTries>0 && shard)
        {
          extendedResult.add("collationInternalRank", collation.getInternalRank());
        }
        collationList.add("collation", extendedResult);
      } else {
        collationList.add("collation", collation.getCollationQuery());
        if (maxCollationTries>0 && shard) {
          collationList.add("collationInternalRank", collation.getInternalRank());
        }
      }
    }
    response.add("collations", collationList);
  }

  @SuppressWarnings({"unchecked"})
  private void addOriginalTermsToResponse(@SuppressWarnings({"rawtypes"})NamedList response, Collection<Token> originalTerms) {
    List<String> originalTermStr = new ArrayList<String>();
    for(Token t : originalTerms) {
      originalTermStr.add(t.toString());
    }
    response.add("originalTerms", originalTermStr);
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
    String prefix = SpellingParams.SPELLCHECK_PREFIX + dictionary + ".";
    while (iter.hasNext()) {
      String nxt = iter.next();
      if (nxt.startsWith(prefix)) {
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
    Integer maxResultsForSuggest = maxResultsForSuggest(rb);
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
          @SuppressWarnings({"rawtypes"})
          NamedList nl = null;
          try {
            nl = (NamedList) srsp.getSolrResponse().getResponse().get("spellcheck");
          } catch (Exception e) {
            if (ShardParams.getShardsTolerantAsBool(rb.req.getParams())) {
              continue; // looks like a shard did not return anything
            }
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Unable to read spelling info for shard: " + srsp.getShard(), e);
          }
          if (log.isInfoEnabled()) {
            log.info("{} {}", srsp.getShard(), nl);
          }
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

    @SuppressWarnings({"rawtypes"})
    NamedList response = new SimpleOrderedMap();

    @SuppressWarnings({"rawtypes"})
    NamedList suggestions = toNamedList(false, result, origQuery, extendedResults);
    response.add("suggestions", suggestions);

    if (extendedResults) {
      response.add("correctlySpelled", isCorrectlySpelled);
    }

    if (collate) {
      SpellCheckCollation[] sortedCollations = mergeData.collations.values()
          .toArray(new SpellCheckCollation[mergeData.collations.size()]);
      Arrays.sort(sortedCollations);

      @SuppressWarnings({"rawtypes"})
      NamedList collations = new NamedList();
      int i = 0;
      while (i < maxCollations && i < sortedCollations.length) {
        SpellCheckCollation collation = sortedCollations[i];
        i++;
        if (collationExtendedResults) {
          @SuppressWarnings({"rawtypes"})
          SimpleOrderedMap extendedResult = new SimpleOrderedMap();
          extendedResult.add("collationQuery", collation.getCollationQuery());
          extendedResult.add("hits", collation.getHits());
          extendedResult.add("misspellingsAndCorrections", collation
              .getMisspellingsAndCorrections());
          collations.add("collation", extendedResult);
        } else {
          collations.add("collation", collation.getCollationQuery());
        }
      }

      response.add("collations", collations);
    }

    rb.rsp.add("spellcheck", response);
  }

  @SuppressWarnings("unchecked")
  private void collectShardSuggestions(@SuppressWarnings({"rawtypes"})NamedList nl, SpellCheckMergeData mergeData) {
    SpellCheckResponse spellCheckResp = new SpellCheckResponse(nl);
    Iterable<Object> originalTermStrings = (Iterable<Object>) nl.get("originalTerms");
    if(originalTermStrings!=null) {
      mergeData.originalTerms = new HashSet<>();
      for (Object originalTermObj : originalTermStrings) {
        mergeData.originalTerms.add(originalTermObj.toString());
      }
    }
    for (SpellCheckResponse.Suggestion suggestion : spellCheckResp.getSuggestions()) {
      mergeData.origVsSuggestion.put(suggestion.getToken(), suggestion);
      HashSet<String> suggested = mergeData.origVsSuggested.get(suggestion.getToken());
      if (suggested == null) {
        suggested = new HashSet<>();
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
  private void collectShardCollations(SpellCheckMergeData mergeData, @SuppressWarnings({"rawtypes"})NamedList spellCheckResponse, int maxCollationTries) {
    Map<String, SpellCheckCollation> collations = mergeData.collations;
    @SuppressWarnings({"rawtypes"})
    NamedList collationHolder = (NamedList) spellCheckResponse.get("collations");
    if(collationHolder != null) {
      List<Object> collationList = collationHolder.getAll("collation");
      List<Object> collationRankList = collationHolder.getAll("collationInternalRank");
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
            @SuppressWarnings({"rawtypes"})
            NamedList expandedCollation = (NamedList) o;
            SpellCheckCollation coll = new SpellCheckCollation();
            coll.setCollationQuery((String) expandedCollation.get("collationQuery"));
            coll.setHits(((Number) expandedCollation.get("hits")).longValue());
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
    Collection<Token> result = new ArrayList<>();
    assert analyzer != null;
    try (TokenStream ts = analyzer.tokenStream("", q)) {
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
      return result;
    }
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

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected NamedList toNamedList(boolean shardRequest,
      SpellingResult spellingResult, String origQuery, boolean extendedResults) {
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
      Map<String,Integer> theSuggestions = new LinkedHashMap<>(
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

          ArrayList<SimpleOrderedMap> sugs = new ArrayList<>();
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
          Integer tokenFrequency = spellingResult.getTokenFrequency(inputToken);
          if (tokenFrequency==null || tokenFrequency == 0) {
            hasZeroFrequencyToken = true;
          }
        }
        result.add(tokenString, suggestionList);
      }
    }
    return result;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void inform(SolrCore core) {
    if (initParams != null) {
      log.info("Initializing spell checkers");
      boolean hasDefault = false;
      for (int i = 0; i < initParams.size(); i++) {
        if (initParams.getName(i).equals("spellchecker")) {
          Object cfg = initParams.getVal(i);
          if (cfg instanceof NamedList) {
            addSpellChecker(core, hasDefault, (NamedList) cfg);
          } else if (cfg instanceof Map) {
            addSpellChecker(core, hasDefault, new NamedList((Map) cfg));
          } else if (cfg instanceof List) {
            for (Object o : (List) cfg) {
              if (o instanceof Map) {
                addSpellChecker(core, hasDefault, new NamedList((Map) o));
              }
            }
          }
        }
      }

      Map<String, QueryConverter> queryConverters = new HashMap<>();
      core.initPlugins(queryConverters,QueryConverter.class);

      //ensure that there is at least one query converter defined
      if (queryConverters.size() == 0) {
        log.trace("No queryConverter defined, using default converter");
        queryConverters.put("queryConverter", new SpellingQueryConverter());
      }

      //there should only be one
      if (queryConverters.size() == 1) {
        queryConverter = queryConverters.values().iterator().next();
        IndexSchema schema = core.getLatestSchema();
        String fieldTypeName = (String) initParams.get("queryAnalyzerFieldType");
        FieldType fieldType = schema.getFieldTypes().get(fieldTypeName);
        Analyzer analyzer = fieldType == null ? new WhitespaceAnalyzer()
                : fieldType.getQueryAnalyzer();
        //TODO: There's got to be a better way!  Where's Spring when you need it?
        queryConverter.setAnalyzer(analyzer);
      }
    }
  }

  @SuppressWarnings({"rawtypes"})private boolean addSpellChecker(SolrCore core, boolean hasDefault, @SuppressWarnings({"rawtypes"})NamedList spellchecker) {
    String className = (String) spellchecker.get("classname");
    if (className == null) className = (String) spellchecker.get("class");
    // TODO: this is a little bit sneaky: warn if class isnt supplied
    // so that it's mandatory in a future release?
    if (className == null)
      className = IndexBasedSpellChecker.class.getName();
    SolrResourceLoader loader = core.getResourceLoader();
    SolrSpellChecker checker = loader.newInstance(className, SolrSpellChecker.class);
    if (checker != null) {
      String dictionary = checker.init(spellchecker, core);
      if (dictionary != null) {
        boolean isDefault = dictionary.equals(SolrSpellChecker.DEFAULT_DICTIONARY_NAME);
        if (isDefault && !hasDefault) {
          hasDefault = true;
        } else if (isDefault && hasDefault) {
          throw new RuntimeException("More than one dictionary is missing name.");
        }
        spellCheckers.put(dictionary, checker);
      } else {
        if (!hasDefault) {
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
      if (buildOnCommit || buildOnOptimize) {
        if (log.isInfoEnabled()) {
          log.info("Registering newSearcher listener for spellchecker: {}", checker.getDictionaryName());
        }
        core.registerNewSearcherListener(new SpellCheckerListener(core, checker, buildOnCommit, buildOnOptimize));
      }
    } else {
      throw new RuntimeException("Can't load spell checker: " + className);
    }
    return hasDefault;
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
    public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    }

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher,
                            SolrIndexSearcher currentSearcher) {
      if (currentSearcher == null) {
        // firstSearcher event
        try {
          if (log.isInfoEnabled()) {
            log.info("Loading spell index for spellchecker: {}", checker.getDictionaryName());
          }
          checker.reload(core, newSearcher);
        } catch (IOException e) {
          log.error( "Exception in reloading spell check index for spellchecker: {}", checker.getDictionaryName(), e);
        }
      } else {
        // newSearcher event
        if (buildOnCommit)  {
          buildSpellIndex(newSearcher);
        } else if (buildOnOptimize) {
          if (newSearcher.getIndexReader().leaves().size() == 1)  {
            buildSpellIndex(newSearcher);
          } else  {
            if (log.isInfoEnabled()) {
              log.info("Index is not optimized therefore skipping building spell check index for: {}"
                  , checker.getDictionaryName());
            }
          }
        }
      }

    }

    private void buildSpellIndex(SolrIndexSearcher newSearcher) {
      try {
        if (log.isInfoEnabled()) {
          log.info("Building spell index for spell checker: {}", checker.getDictionaryName());
        }
        checker.build(core, newSearcher);
      } catch (Exception e) {
        log.error("Exception in building spell check index for spellchecker: {}", checker.getDictionaryName(), e);
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
  // / SolrInfoBean
  // //////////////////////////////////////////

  @Override
  public String getDescription() {
    return "A Spell Checker component";
  }

  @Override
  public Category getCategory() {
    return Category.SPELLCHECKER;
  }
}
