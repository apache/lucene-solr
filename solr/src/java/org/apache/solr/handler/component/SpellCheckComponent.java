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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.search.spell.LevensteinDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
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
        boolean onlyMorePopular = params.getBool(SPELLCHECK_ONLY_MORE_POPULAR,
            DEFAULT_ONLY_MORE_POPULAR);
        boolean extendedResults = params.getBool(SPELLCHECK_EXTENDED_RESULTS,
            false);
        NamedList response = new SimpleOrderedMap();
        IndexReader reader = rb.req.getSearcher().getIndexReader();
        boolean collate = params.getBool(SPELLCHECK_COLLATE, false);
        float accuracy = params.getFloat(SPELLCHECK_ACCURACY, Float.MIN_VALUE);
        SolrParams customParams = getCustomParams(getDictionaryName(params), params, shardRequest);
        SpellingOptions options = new SpellingOptions(tokens, reader, count, onlyMorePopular, extendedResults,
                accuracy, customParams);                       
        SpellingResult spellingResult = spellChecker.getSuggestions(options);
        if (spellingResult != null) {
        	NamedList suggestions = toNamedList(shardRequest, spellingResult, q, extendedResults, collate);					
					if (collate) {						
						addCollationsToResponse(params, spellingResult, rb, q, suggestions);
					}
					response.add("suggestions", suggestions);
					rb.rsp.add("spellcheck", response);
        }

      } else {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND,
            "Specified dictionary does not exist: " + getDictionaryName(params));
      }
    }
  }
  
  @SuppressWarnings("unchecked")
	protected void addCollationsToResponse(SolrParams params, SpellingResult spellingResult, ResponseBuilder rb, String q,
			NamedList response) {
		int maxCollations = params.getInt(SPELLCHECK_MAX_COLLATIONS, 1);
		int maxCollationTries = params.getInt(SPELLCHECK_MAX_COLLATION_TRIES, 0);
		int maxCollationEvaluations = params.getInt(SPELLCHECK_MAX_COLLATION_EVALUATIONS, 10000);
		boolean collationExtendedResults = params.getBool(SPELLCHECK_COLLATE_EXTENDED_RESULTS, false);
		boolean shard = params.getBool(ShardParams.IS_SHARD, false);

		SpellCheckCollator collator = new SpellCheckCollator();
		List<SpellCheckCollation> collations = collator.collate(spellingResult, q, rb, maxCollations, maxCollationTries, maxCollationEvaluations);
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
  protected SolrParams getCustomParams(String dictionary, SolrParams params, boolean shardRequest) {
    ModifiableSolrParams result = new ModifiableSolrParams();
    Iterator<String> iter = params.getParameterNamesIterator();
    String prefix = SpellingParams.SPELLCHECK_PREFIX + "." + dictionary + ".";
    while (iter.hasNext()){
      String nxt = iter.next();
      if (nxt.startsWith(prefix)){
        result.add(nxt.substring(prefix.length()), params.getParams(nxt));
      }
    }
    if(shardRequest)
    {
    	result.add(ShardParams.IS_SHARD, "true");
    }
    return result;
  }


  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    SolrParams params = rb.req.getParams();
    // Turn on spellcheck only only when retrieving fields
    if (!params.getBool(COMPONENT_NAME, false)) return;
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
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

    String origQuery = params.get(SPELLCHECK_Q);
    if (origQuery == null) {
      origQuery = rb.getQueryString();
      if (origQuery == null) {
        origQuery = params.get(CommonParams.Q);
      }
    }

    int count = rb.req.getParams().getInt(SPELLCHECK_COUNT, 1);
    float min = 0.5f;
    StringDistance sd = null;
    int numSug = Math.max(count, AbstractLuceneSpellChecker.DEFAULT_SUGGESTION_COUNT);
    SolrSpellChecker checker = getSpellChecker(rb.req.getParams());
    if (checker instanceof AbstractLuceneSpellChecker) {
      AbstractLuceneSpellChecker spellChecker = (AbstractLuceneSpellChecker) checker;
      min = spellChecker.getAccuracy();
      sd = spellChecker.getStringDistance();
    }
    if (sd == null)
      sd = new LevensteinDistance();

    Collection<Token> tokens = null;
    try {
      tokens = getTokens(origQuery, checker.getQueryAnalyzer());
    } catch (IOException e) {
      LOG.error("Could not get tokens (this should never happen)", e);
    }

    // original token -> corresponding Suggestion object (keep track of start,end)
    Map<String, SpellCheckResponse.Suggestion> origVsSuggestion = new HashMap<String, SpellCheckResponse.Suggestion>();
    // original token string -> summed up frequency
    Map<String, Integer> origVsFreq = new HashMap<String, Integer>();
    // original token string -> # of shards reporting it as misspelled
    Map<String, Integer> origVsShards = new HashMap<String, Integer>();
    // original token string -> set of alternatives
    // must preserve order because collation algorithm can only work in-order
    Map<String, HashSet<String>> origVsSuggested = new LinkedHashMap<String, HashSet<String>>();
    // alternative string -> corresponding SuggestWord object
    Map<String, SuggestWord> suggestedVsWord = new HashMap<String, SuggestWord>();
    Map<String, SpellCheckCollation> collations = new HashMap<String, SpellCheckCollation>();
    
    int totalNumberShardResponses = 0;
    for (ShardRequest sreq : rb.finished) {
      for (ShardResponse srsp : sreq.responses) {
        NamedList nl = (NamedList) srsp.getSolrResponse().getResponse().get("spellcheck");
        LOG.info(srsp.getShard() + " " + nl);
        if (nl != null) {
        	totalNumberShardResponses++;
          SpellCheckResponse spellCheckResp = new SpellCheckResponse(nl);
          for (SpellCheckResponse.Suggestion suggestion : spellCheckResp.getSuggestions()) {
            origVsSuggestion.put(suggestion.getToken(), suggestion);
            HashSet<String> suggested = origVsSuggested.get(suggestion.getToken());
            if (suggested == null) {
              suggested = new HashSet<String>();
              origVsSuggested.put(suggestion.getToken(), suggested);
            }

            // sum up original frequency          
            int origFreq = 0;
            Integer o = origVsFreq.get(suggestion.getToken());
            if (o != null)  origFreq += o;
            origFreq += suggestion.getOriginalFrequency();
            origVsFreq.put(suggestion.getToken(), origFreq);
            
            //# shards reporting
            Integer origShards = origVsShards.get(suggestion.getToken());
            if(origShards==null) {
            	origVsShards.put(suggestion.getToken(), 1);
            } else {
            	origVsShards.put(suggestion.getToken(), ++origShards);
            }            

            // find best suggestions
            for (int i = 0; i < suggestion.getNumFound(); i++) {
              String alternative = suggestion.getAlternatives().get(i);
              suggested.add(alternative);
              SuggestWord sug = suggestedVsWord.get(alternative);
              if (sug == null)  {
                sug = new SuggestWord();
                suggestedVsWord.put(alternative, sug);
              }
              sug.string = alternative;
              // alternative frequency is present only for extendedResults=true
              if (suggestion.getAlternativeFrequencies() != null && suggestion.getAlternativeFrequencies().size() > 0) {
                Integer freq = suggestion.getAlternativeFrequencies().get(i);
                if (freq != null) sug.freq += freq;
              }
            }
          }
          NamedList suggestions = (NamedList) nl.get("suggestions");
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
      }
    }

    // all shard responses have been collected
    // create token and get top suggestions
    SpellingResult result = new SpellingResult(tokens); //todo: investigate, why does it need tokens beforehand?
    for (Map.Entry<String, HashSet<String>> entry : origVsSuggested.entrySet()) {
      String original = entry.getKey();
      
      //Only use this suggestion if all shards reported it as misspelled.
      Integer numShards = origVsShards.get(original);
      if(numShards<totalNumberShardResponses) {
      	continue;
      }
      
      HashSet<String> suggested = entry.getValue();
      SuggestWordQueue sugQueue = new SuggestWordQueue(numSug);
      for (String suggestion : suggested) {
        SuggestWord sug = suggestedVsWord.get(suggestion);
        sug.score = sd.getDistance(original, sug.string);
        if (sug.score < min) continue;
        sugQueue.insertWithOverflow(sug);
        if (sugQueue.size() == numSug) {
          // if queue full, maintain the minScore score
          min = sugQueue.top().score;
        }
      }

      // create token
      SpellCheckResponse.Suggestion suggestion = origVsSuggestion.get(original);
      Token token = new Token(original, suggestion.getStartOffset(), suggestion.getEndOffset());

      // get top 'count' suggestions out of 'sugQueue.size()' candidates
      SuggestWord[] suggestions = new SuggestWord[Math.min(count, sugQueue.size())];
      // skip the first sugQueue.size() - count elements
      for (int k=0; k < sugQueue.size() - count; k++) sugQueue.pop();
      // now collect the top 'count' responses
      for (int k = Math.min(count, sugQueue.size()) - 1; k >= 0; k--)  {
        suggestions[k] = sugQueue.pop();
      }

      if (extendedResults) {
        Integer o = origVsFreq.get(original);
        if (o != null) result.addFrequency(token, o);
        for (SuggestWord word : suggestions)
          result.add(token, word.string, word.freq);
      } else {
        List<String> words = new ArrayList<String>(sugQueue.size());
        for (SuggestWord word : suggestions) words.add(word.string);
        result.add(token, words);
      }
    }
    
    NamedList response = new SimpleOrderedMap();
		NamedList suggestions = toNamedList(false, result, origQuery, extendedResults, collate);
		if (collate) {
			SpellCheckCollation[] sortedCollations = collations.values().toArray(new SpellCheckCollation[collations.size()]);
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

  private Collection<Token> getTokens(String q, Analyzer analyzer) throws IOException {
    Collection<Token> result = new ArrayList<Token>();
    TokenStream ts = analyzer.reusableTokenStream("", new StringReader(q));
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
    return result;
  }

  protected SolrSpellChecker getSpellChecker(SolrParams params) {
    return spellCheckers.get(getDictionaryName(params));
  }

  private String getDictionaryName(SolrParams params) {
    String dictName = params.get(SPELLCHECK_DICT);
    if (dictName == null) {
      dictName = SolrSpellChecker.DEFAULT_DICTIONARY_NAME;
    }
    return dictName;
  }

  /**
   * @return the spellchecker registered to a given name
   */
  public SolrSpellChecker getSpellChecker(String name) {
    return spellCheckers.get(name);
  }

  protected NamedList toNamedList(boolean shardRequest, SpellingResult spellingResult, String origQuery, boolean extendedResults, boolean collate) {
    NamedList result = new NamedList();
    Map<Token, LinkedHashMap<String, Integer>> suggestions = spellingResult.getSuggestions();
    boolean hasFreqInfo = spellingResult.hasTokenFrequencyInfo();
    boolean isCorrectlySpelled = false;
    
    int numSuggestions = 0;
    for(LinkedHashMap<String, Integer> theSuggestion : suggestions.values())
    {
    	if(theSuggestion.size()>0)
    	{
    		numSuggestions++;
    	}
    } 
    
    // will be flipped to false if any of the suggestions are not in the index and hasFreqInfo is true
    if(numSuggestions > 0) {
      isCorrectlySpelled = true;
    }
    
    for (Map.Entry<Token, LinkedHashMap<String, Integer>> entry : suggestions.entrySet()) {
      Token inputToken = entry.getKey();
      Map<String, Integer> theSuggestions = entry.getValue();
      if (theSuggestions != null && (theSuggestions.size()>0 || shardRequest)) {
        SimpleOrderedMap suggestionList = new SimpleOrderedMap();
        suggestionList.add("numFound", theSuggestions.size());
        suggestionList.add("startOffset", inputToken.startOffset());
        suggestionList.add("endOffset", inputToken.endOffset());

        // Logical structure of normal (non-extended) results:
        // "suggestion":["alt1","alt2"]
        //
        // Logical structure of the extended results:
        // "suggestion":[
        //     {"word":"alt1","freq":7},
        //     {"word":"alt2","freq":4}
        // ]
        if (extendedResults && hasFreqInfo) {
          suggestionList.add("origFreq", spellingResult.getTokenFrequency(inputToken));

          ArrayList<SimpleOrderedMap> sugs = new ArrayList<SimpleOrderedMap>();
          suggestionList.add("suggestion", sugs);
          for (Map.Entry<String, Integer> suggEntry : theSuggestions.entrySet()) {
            SimpleOrderedMap sugEntry = new SimpleOrderedMap();
            sugEntry.add("word",suggEntry.getKey());
            sugEntry.add("freq",suggEntry.getValue());
            sugs.add(sugEntry);
          }
        } else {
          suggestionList.add("suggestion", theSuggestions.keySet());
        }

        if (hasFreqInfo) {
          isCorrectlySpelled = isCorrectlySpelled && spellingResult.getTokenFrequency(inputToken) > 0;
        }
        result.add(new String(inputToken.buffer(), 0, inputToken.length()), suggestionList);
      }
    }
    if (hasFreqInfo) {
      result.add("correctlySpelled", isCorrectlySpelled);
    } else if(extendedResults && suggestions.size() == 0) { // if the word is misspelled, its added to suggestions with freqinfo
      result.add("correctlySpelled", true);
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

    public void init(NamedList args) {
    }

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
          if (newSearcher.getIndexReader().isOptimized())  {
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
