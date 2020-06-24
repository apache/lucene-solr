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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.shingle.ShingleFilterFactory;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;

import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.SolrPluginUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A component that can be used in isolation, or in conjunction with {@link QueryComponent} to identify 
 * &amp; score "phrases" found in the input string, based on shingles in indexed fields.
 *
 * <p>
 * The most common way to use this component is in conjunction with field that use 
 * {@link ShingleFilterFactory} on both the <code>index</code> and <code>query</code> analyzers.  
 * An example field type configuration would be something like this...
 * </p>
 * <pre class="prettyprint">
 * &lt;fieldType name="phrases" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer type="index"&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.ShingleFilterFactory" minShingleSize="2" maxShingleSize="3" outputUnigrams="true"/&gt;
 *   &lt;/analyzer&gt;
 *   &lt;analyzer type="query"&gt;
 *     &lt;tokenizer class="solr.StandardTokenizerFactory"/&gt;
 *     &lt;filter class="solr.LowerCaseFilterFactory"/&gt;
 *     &lt;filter class="solr.ShingleFilterFactory" minShingleSize="2" maxShingleSize="7" outputUnigramsIfNoShingles="true" outputUnigrams="true"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;
 * </pre>
 * <p>
 * ...where the <code>query</code> analyzer's <code>maxShingleSize="7"</code> determines the maximum 
 * possible phrase length that can be hueristically deduced, the <code>index</code> analyzer's 
 * <code>maxShingleSize="3"</code> determines the accuracy of phrases identified.  The large the 
 * indexed <code>maxShingleSize</code> the higher the accuracy.  Both analyzers must include 
 * <code>minShingleSize="2" outputUnigrams="true"</code>.
 * </p>
 * <p>
 * With a field type like this, one or more fields can be specified (with weights) via a 
 * <code>phrases.fields</code> param to request that this component identify possible phrases in the 
 * input <code>q</code> param, or an alternative <code>phrases.q</code> override param.  The identified
 * phrases will include their scores relative each field specified, as well an overal weighted score based
 * on the field weights provided by the client.  Higher score values indicate a greater confidence in the 
 * Phrase.
 * </p>
 * 
 * <p>
 * <b>NOTE:</b> In a distributed request, this component uses a single phase (piggy backing on the 
 * {@link ShardRequest#PURPOSE_GET_TOP_IDS} generated by {@link QueryComponent} if it is in use) to 
 * collect all field &amp; shingle stats.  No "refinement" requests are used.
 * </p>
 *
 * @lucene.experimental
 */
public class PhrasesIdentificationComponent extends SearchComponent {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** The only shard purpose that will cause this component to do work &amp; return data during shard req */
  public static final int SHARD_PURPOSE = ShardRequest.PURPOSE_GET_TOP_IDS;
  
  /** Name, also used as a request param to identify whether the user query concerns this component */
  public static final String COMPONENT_NAME = "phrases";

  // TODO: ideally these should live in a commons.params class?
  public static final String PHRASE_INPUT = "phrases.q";
  public static final String PHRASE_FIELDS = "phrases.fields";
  public static final String PHRASE_ANALYSIS_FIELD = "phrases.analysis.field";
  public static final String PHRASE_SUMMARY_PRE = "phrases.pre";
  public static final String PHRASE_SUMMARY_POST = "phrases.post";
  public static final String PHRASE_INDEX_MAXLEN = "phrases.maxlength.index";
  public static final String PHRASE_QUERY_MAXLEN = "phrases.maxlength.query";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    final SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }
    if (params.getBool(ShardParams.IS_SHARD, false)) {
      // only one stage/purpose where we should do any work on a shard
      if (0 == (SHARD_PURPOSE & params.getInt(ShardParams.SHARDS_PURPOSE, 0))) {
        return;
      }
    }

    // if we're still here, then we should parse & validate our input, 
    // putting it in the request context so our process method knows it should do work
    rb.req.getContext().put(this.getClass(), PhrasesContextData.parseAndValidateRequest(rb.req));
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) {
    final PhrasesContextData contextData = (PhrasesContextData) rb.req.getContext().get(this.getClass());
    if (null == contextData) {
      // if prepare didn't give us anything to work with, then we should do nothing
      return ResponseBuilder.STAGE_DONE;
    }

    if (rb.stage < ResponseBuilder.STAGE_EXECUTE_QUERY) {
      return ResponseBuilder.STAGE_EXECUTE_QUERY;
  
    } else if (rb.stage == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      // if we're being used in conjunction with QueryComponent, it should have already created
      // (in this staged) the only ShardRequest we need...
      for (ShardRequest sreq : rb.outgoing) {
        if (0 != (SHARD_PURPOSE & sreq.purpose) ) {
          return ResponseBuilder.STAGE_GET_FIELDS;
        }
      }
      // ...if we can't find it, then evidently we're being used in isolation,
      // and we need to create our own ShardRequest...
      ShardRequest sreq = new ShardRequest();
      sreq.purpose = SHARD_PURPOSE;
      sreq.params = new ModifiableSolrParams(rb.req.getParams());
      sreq.params.remove(ShardParams.SHARDS);
      rb.addRequest(this, sreq);
      return ResponseBuilder.STAGE_GET_FIELDS;
      
    } else if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      // NOTE: we don't do any actual work in this stage, but we need to ensure that even if
      // we are being used in isolation w/o QueryComponent that SearchHandler "tracks" a STAGE_GET_FIELDS
      // so that finishStage(STAGE_GET_FIELDS) is called on us and we can add our merged results
      // (w/o needing extra code paths for merging phrase results when QueryComponent is/is not used)
      return ResponseBuilder.STAGE_DONE;
    }

    return ResponseBuilder.STAGE_DONE;
  }
  
  @Override
  public void finishStage(ResponseBuilder rb) {
    // NOTE: we don't do this after STAGE_EXECUTE_QUERY because if we're also being used with
    // QueryComponent, we don't want to add our results to the response until *after*
    // QueryComponent adds the main DocList
    
    final PhrasesContextData contextData = (PhrasesContextData) rb.req.getContext().get(this.getClass());
    if (null == contextData || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) {
      // if prepare didn't give us anything to work with, or this isn't our stage, then do nothing
      return;
    }
      
    // sanity check: the shard requests we use/piggy-back on should only hapen once per shard,
    // but let's future proof ourselves against the possibility that some shards might get/respond
    // to the same request "purpose" multiple times...
    final BitSet shardsHandled = new BitSet(rb.shards.length);
    
    // Collect Shard responses
    for (ShardRequest sreq : rb.finished) {
      if (0 != (sreq.purpose & SHARD_PURPOSE)) {
        for (ShardResponse shardRsp : sreq.responses) {
          final int shardNum = rb.getShardNum(shardRsp.getShard());
          if (! shardsHandled.get(shardNum)) {
            shardsHandled.set(shardNum);
            // shards.tolerant=true can cause nulls on exceptions/errors
            // if we don't get phrases/stats from a shard, just ignore that shard
            final SolrResponse rsp = shardRsp.getSolrResponse();
            if (null == rsp) continue;
            final NamedList<Object> top = rsp.getResponse();
            if (null == top) continue;
            @SuppressWarnings({"unchecked"})
            final NamedList<Object> phrasesWrapper = (NamedList<Object>) top.get("phrases");
            if (null == phrasesWrapper) continue;
            @SuppressWarnings({"unchecked"})
            final List<NamedList<Object>> shardPhrases = (List<NamedList<Object>>) phrasesWrapper.get("_all");
            if (null == shardPhrases) continue;
            
            Phrase.populateStats(contextData.allPhrases, shardPhrases);
          }
        }
      }
    }
    scoreAndAddResultsToResponse(rb, contextData);
  }

  
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    final PhrasesContextData contextData = (PhrasesContextData) rb.req.getContext().get(this.getClass());
    if (null == contextData) {
      // if prepare didn't give us anything to work with, then we should do nothing
      return;
    }

    // regardless of single node / shard, we need local stats...
    Phrase.populateStats(contextData.allPhrases, contextData.fieldWeights.keySet(), rb.req.getSearcher());

    if ( rb.req.getParams().getBool(ShardParams.IS_SHARD, false) ) {
      // shard request, return stats for all phrases (in original order)
      SimpleOrderedMap<Object> output = new SimpleOrderedMap<>();
      output.add("_all", Phrase.formatShardResponse(contextData.allPhrases));
      // TODO: might want to add numDocs() & getSumTotalTermFreq(f)/getDocCount(f) stats from each field...
      // so that we can sum/merge them for use in scoring?
      rb.rsp.add("phrases", output);
    } else {
      // full single node request...
      scoreAndAddResultsToResponse(rb, contextData);
    }
  }

  /** 
   * Helper method (suitable for both single node &amp; distributed coordinator node) to 
   * score, sort, and format the end user response once all phrases have been populated with stats.
   */
  private void scoreAndAddResultsToResponse(final ResponseBuilder rb, final PhrasesContextData contextData) {
    assert null != contextData : "Should not be called if no phrase data to use";
    if (null == contextData) {
      // if prepare didn't give us anything to work with, then we should do nothing
      return;
    }
    
    SimpleOrderedMap<Object> output = new SimpleOrderedMap<>();
    rb.rsp.add("phrases", output);
    output.add("input", contextData.rawInput);

    if (0 == contextData.allPhrases.size()) {
      // w/o any phrases, the summary is just the input again...
      output.add("summary", contextData.rawInput);
      output.add("details", Collections.<Object>emptyList());
      return;
    }
    
    Phrase.populateScores(contextData);
    final int maxPosition = contextData.allPhrases.get(contextData.allPhrases.size()-1).getPositionEnd();
    
    final List<Phrase> validScoringPhrasesSorted = contextData.allPhrases.stream()
      // TODO: ideally this cut off of "0.0" should be a request option...
      // so users can tune how aggresive/conservative they want to be in finding phrases
      // but for that to be useful, we need:
      //  - more hard & fast documentation about the "range" of scores that may be returned
      //  - "useful" scores for single words
      .filter(p -> 0.0D < p.getTotalScore())
      .sorted(Comparator.comparing((p -> p.getTotalScore()), Collections.reverseOrder()))
      .collect(Collectors.toList());

    // we want to return only high scoring phrases that don't overlap w/higher scoring phrase
    final BitSet positionsCovered = new BitSet(maxPosition+1);
    final List<Phrase> results = new ArrayList<>(maxPosition);
    for (Phrase phrase : validScoringPhrasesSorted) {
      final BitSet phrasePositions = phrase.getPositionsBitSet();
      
      if (! phrasePositions.intersects(positionsCovered)) {
        // we can use this phrase, record it...
        positionsCovered.or(phrasePositions);
        results.add(phrase);
      } // else: overlaps higher scoring position(s), skip this phrase
      
      if (positionsCovered.cardinality() == maxPosition+1) {
        // all positions are covered, so we can bail out and skip the rest
        break;
      }
    }
    
    // a "quick summary" of the suggested parsing
    output.add("summary", contextData.summarize(results));
    // useful user level info on every (high scoring) phrase found (in current, descending score, order)
    output.add("details", results.stream()
               .map(p -> p.getDetails()).collect(Collectors.toList()));
  }
  
  @Override
  public String getDescription() {
    return "Phrases Identification Component";
  }

  /** 
   * Simple container for all request options and data this component needs to store in the Request Context 
   * @lucene.internal
   */
  public static final class PhrasesContextData {

    public final String rawInput;
    public final int maxIndexedPositionLength; 
    public final int maxQueryPositionLength; 
    public final Map<String,Double> fieldWeights;
    public final SchemaField analysisField;
    public final List<Phrase> allPhrases;
    public final String summaryPre;
    public final String summaryPost;

    // TODO: add an option to bias field weights based on sumTTF of the fields
    // (easy enough to "sum the sums" across multiple shards before scoring)

    /**
     * Parses the params included in this request, throwing appropriate user level 
     * Exceptions for invalid input, and returning a <code>PhrasesContextData</code>
     * suitable for use in this request.
     */
    public static PhrasesContextData parseAndValidateRequest(final SolrQueryRequest req) throws SolrException {
      return new PhrasesContextData(req);
    }
    private PhrasesContextData(final SolrQueryRequest req) throws SolrException {
      final SolrParams params = req.getParams();

      this.rawInput = params.get(PHRASE_INPUT, params.get(CommonParams.Q));
      if (null == this.rawInput) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "phrase identification requires a query string or "
                                + PHRASE_INPUT + " param override");
      }

      { // field weights & analysis field...
        
        SchemaField tmpAnalysisField = null;
        Map<String,Double> tmpWeights = new TreeMap<>();
        
        final String analysisFieldName = params.get(PHRASE_ANALYSIS_FIELD);
        if (null != analysisFieldName) {
          tmpAnalysisField = req.getSchema().getFieldOrNull(analysisFieldName);
          if (null == tmpAnalysisField) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                                    PHRASE_ANALYSIS_FIELD + " param specifies a field name that does not exist: " +
                                    analysisFieldName);
          }
        }
        
        final Map<String,Float> rawFields = SolrPluginUtils.parseFieldBoosts(params.getParams(PHRASE_FIELDS));
        if (rawFields.isEmpty()) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
                                  PHRASE_FIELDS + " param must specify a (weighted) list of fields " +
                                  "to evaluate for phrase identification");
        }
        
        for (Map.Entry<String,Float> entry : rawFields.entrySet()) {
          final SchemaField field = req.getSchema().getFieldOrNull(entry.getKey());
          if (null == field) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
                                  PHRASE_FIELDS + " param contains a field name that does not exist: " +
                                  entry.getKey());
          }
          if (null == tmpAnalysisField) {
            tmpAnalysisField = field;
          }
          if ( null == analysisFieldName ) {
            if (! field.getType().equals(tmpAnalysisField.getType())) {
              throw new SolrException
                (ErrorCode.BAD_REQUEST,
                 "All fields specified in " + PHRASE_FIELDS + " must have the same fieldType, " +
                 "or the advanced " + PHRASE_ANALYSIS_FIELD + " option must specify an override");
            }
          }
          // if a weight isn't specified, assume "1.0" 
          final double weight = null == entry.getValue() ? 1.0D : entry.getValue();
          if (weight < 0) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                                    PHRASE_FIELDS + " param must use non-negative weight value for field " + field.getName());
          }
          tmpWeights.put(entry.getKey(), weight);
        }
        assert null != tmpAnalysisField;
        
        this.analysisField = tmpAnalysisField;
        this.fieldWeights = Collections.unmodifiableMap(tmpWeights);
      }

      { // index/query max phrase sizes...
        final FieldType ft = analysisField.getType();
        this.maxIndexedPositionLength = req.getParams().getInt(PHRASE_INDEX_MAXLEN,
                                                               getMaxShingleSize(ft.getIndexAnalyzer()));
        if (this.maxIndexedPositionLength < 0) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
                                  "Unable to determine max position length of indexed phrases using " +
                                  "index analyzer for analysis field: " + analysisField.getName() +
                                  " and no override detected using param: " + PHRASE_INDEX_MAXLEN);
        }
        this.maxQueryPositionLength = req.getParams().getInt(PHRASE_QUERY_MAXLEN,
                                                             getMaxShingleSize(ft.getQueryAnalyzer()));
        if (this.maxQueryPositionLength < 0) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
                                  "Unable to determine max position length of query phrases using " +
                                  "query analyzer for analysis field: " + analysisField.getName() +
                                  " and no override detected using param: " + PHRASE_QUERY_MAXLEN);
        }
        if (this.maxQueryPositionLength < this.maxIndexedPositionLength) {
          throw new SolrException
            (ErrorCode.BAD_REQUEST,
             "Effective value of " + PHRASE_INDEX_MAXLEN + " (either from index analyzer shingle factory, " +
             " or expert param override) must be less then or equal to the effective value of " +
             PHRASE_QUERY_MAXLEN + " (either from query analyzer shingle factory, or expert param override)");
        }
      }
      
      this.summaryPre = params.get(PHRASE_SUMMARY_PRE, "{");
      this.summaryPost = params.get(PHRASE_SUMMARY_POST, "}");

      this.allPhrases = Phrase.extractPhrases(this.rawInput, this.analysisField,
                                              this.maxIndexedPositionLength,
                                              this.maxQueryPositionLength);
        
    }
    
    /**
     * Given a list of phrases to be returned to the user, summarizes those phrases by decorating the 
     * original input string to indicate where the identified phrases exist, using {@link #summaryPre} 
     * and {@link #summaryPost}
     *
     * @param results a list of (non overlapping) Phrases that have been identified, sorted from highest scoring to lowest
     * @return the original user input, decorated to indicate the identified phrases
     */
    public String summarize(final List<Phrase> results) {
      final StringBuffer out = new StringBuffer(rawInput);
      
      // sort by *reverse* position so we can go back to front 
      final List<Phrase> reversed = results.stream()
        .sorted(Comparator.comparing((p -> p.getPositionStart()), Collections.reverseOrder()))
        .collect(Collectors.toList());

      for (Phrase p : reversed) {
        out.insert(p.getOffsetEnd(), summaryPost);
        out.insert(p.getOffsetStart(), summaryPre);
      }
      return out.toString();
    }
  }
      
  
  /** 
   * Model the data known about a single (candidate) Phrase -- which may or may not be indexed 
   * @lucene.internal
   */
  public static final class Phrase {

    /**
     * Factory method for constructing a list of Phrases given the specified input and using the analyzer
     * for the specified field.  The <code>maxIndexedPositionLength</code> and 
     * <code>maxQueryPositionLength</code> provided *must* match the effective values used by 
     * respective analyzers.
     */
    public static List<Phrase> extractPhrases(final String input, final SchemaField analysisField,
                                              final int maxIndexedPositionLength,
                                              final int maxQueryPositionLength) {

      // TODO: rather then requiring the query analyzer to produce the Phrases for us (assuming Shingles)
      // we could potentially just require that it produces unigrams compatible with the unigrams in the
      // indexed fields, and then build our own Phrases at query time -- making the maxQueryPositionLength
      // a 100% run time configuration option.
      // But that could be tricky given an arbitrary analyzer -- we'd have pay careful attention
      // to positions, and we'd have to guess/assume what placeholders/fillers was used in the indexed Phrases
      // (typically shingles)

      assert maxIndexedPositionLength <= maxQueryPositionLength;
      
      final CharsRefBuilder buffer = new CharsRefBuilder();
      final FieldType ft = analysisField.getType();
      final Analyzer analyzer = ft.getQueryAnalyzer();
      final List<Phrase> results = new ArrayList<>(42);
      try (TokenStream tokenStream = analyzer.tokenStream(analysisField.getName(), input)) {
        
        final OffsetAttribute offsetAttr = tokenStream.addAttribute(OffsetAttribute.class);
        final PositionIncrementAttribute posIncAttr = tokenStream.addAttribute(PositionIncrementAttribute.class);
        final PositionLengthAttribute posLenAttr = tokenStream.addAttribute(PositionLengthAttribute.class);
        final TermToBytesRefAttribute termAttr = tokenStream.addAttribute(TermToBytesRefAttribute.class);
        
        int position = 0;
        int lastPosLen = -1;
        
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
          final Phrase phrase = new Phrase();

          final int posInc = posIncAttr.getPositionIncrement();
          final int posLen = posLenAttr.getPositionLength();

          if (0 == posInc && posLen <= lastPosLen) {
            // This requirement of analyzers to return tokens in ascending order of length
            // is currently neccessary for the "linking" logic below to work
            // if people run into real world sitautions where this is problematic,
            // we can relax this check if we also make the linking logic more complex
            // (ie: less optimzied)
            throw new SolrException
              (ErrorCode.BAD_REQUEST, "Phrase identification currently requires that " +
               "the analyzer used must produce tokens that overlap in increasing order of length. ");
          }
          
          position += posInc;
          lastPosLen = posLen;
          
          phrase.position_start = position;
          phrase.position_end = position + posLen;
          
          phrase.is_indexed = (posLen <= maxIndexedPositionLength);
          
          phrase.offset_start = offsetAttr.startOffset();
          phrase.offset_end = offsetAttr.endOffset();

          // populate the subsequence directly from the raw input using the offsets,
          // (instead of using the TermToBytesRefAttribute) so we preserve the original
          // casing, whitespace, etc...
          phrase.subSequence = input.subSequence(phrase.offset_start, phrase.offset_end);
          
          if (phrase.is_indexed) {
            // populate the bytes so we can build term queries
            phrase.bytes = BytesRef.deepCopyOf(termAttr.getBytesRef());
          }
          
          results.add(phrase);
        }
        tokenStream.end();
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
                                "Analysis error extracting phrases from: " + input, e); 
      }
      
      // fill in the relationships of each phrase
      //
      // NOTE: this logic currently requries that the phrases are sorted by position ascending
      // (automatic because of how PositionIncrementAttribute works) then by length ascending
      // (when positions are tied).
      // We could de-optimize this code if we find that secondary ordering is too restrictive for
      // some analyzers
      //
      // NOTE changes to scoring model may be allow optimize/prune down the relationships tracked,
      // ...OR.... may require us to add/track more details about sub/parent phrases
      //
      for (int p = 0; p < results.size(); p++) {
        final Phrase current = results.get(p);
        if (! current.is_indexed) {
          // we're not an interesting sub phrase of anything
          continue;
        }
        
        // setup links from the phrase to itself if needed
        addLinkages(current, current, maxIndexedPositionLength);
        
        // scan backwards looking for phrases that might include us...
        BEFORE: for (int i = p-1; 0 <= i; i--) {
          final Phrase previous = results.get(i);
          if (previous.position_start < (current.position_end - maxQueryPositionLength)) {
            // we've scanned so far back nothing else is viable
            break BEFORE;
          }
          // any 'previous' phrases must start where current starts or earlier,
          // so only need to check the end...
          if (current.position_end <= previous.position_end) {
            addLinkages(previous, current, maxIndexedPositionLength);
          }
        }
        // scan forwards looking for phrases that might include us...
        AFTER: for (int i = p+1; i < results.size(); i++) {
          final Phrase next = results.get(i);
          // the only way a phrase that comes after current can include current is
          // if they have the same start position...
          if (current.position_start != next.position_start) {
            // we've scanned so far forward nothing else is viable
            break AFTER;
          }
          // any 'next' phrases must start where current starts, so only need to check the end...
          if (current.position_end <= next.position_end) {
            addLinkages(next, current, maxIndexedPositionLength);
          }
        }
      }
      
      return Collections.unmodifiableList(results);
    }

    /** 
     * Given two phrases, one of which is a super set of the other, adds the neccessary linkages 
     * needed by the scoring model
     */
    private static void addLinkages(final Phrase outer, final Phrase inner,
                                    final int maxIndexedPositionLength) {
      
      assert outer.position_start <= inner.position_start;
      assert inner.position_end <= outer.position_end;
      assert inner.is_indexed;
      
      final int inner_len = inner.getPositionLength();
      if (1 == inner_len) {
        outer.individualIndexedTerms.add(inner);
      }
      if (maxIndexedPositionLength == inner_len
          || (inner == outer && inner_len < maxIndexedPositionLength)) {
        outer.largestIndexedSubPhrases.add(inner);
      }
      if (outer.is_indexed && inner != outer) {
        inner.indexedSuperPhrases.add(outer);
      }
    }

    /**
     * Format the phrases suitable for returning in a shard response
     * @see #populateStats(List,List)
     */
    public static List<NamedList<Object>> formatShardResponse(final List<Phrase> phrases) {
      List<NamedList<Object>> results = new ArrayList<>(phrases.size());
      for (Phrase p : phrases) {
        NamedList<Object> data = new SimpleOrderedMap<>();
        // quick and dirty way to validate that our shards aren't using different analyzers
        // so the coordinating node can fail fast when mergingthe results
        data.add("checksum", p.getChecksum());
        if (p.is_indexed) {
          data.add("ttf", new NamedList<Object>(p.phrase_ttf));
          data.add("df", new NamedList<Object>(p.phrase_df));
        }
        data.add("conj_dc", new NamedList<Object>(p.subTerms_conjunctionCounts));

        results.add(data);
      }
      return results;
    }
    
    /**
     * Populates the phrases with (merged) stats from a remote shard
     * @see #formatShardResponse
     */
    @SuppressWarnings({"unchecked"})
    public static void populateStats(final List<Phrase> phrases, final List<NamedList<Object>> shardData) {
      final int numPhrases = phrases.size();
      if (shardData.size() != numPhrases) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                "num phrases in shard data not consistent: " +
                                numPhrases + " vs " + shardData.size());
      }
      for (int i = 0; i < phrases.size(); i++) {
        // rather then being paranoid about the expected structure, we'll just let the low level
        // code throw an NPE / CCE / AIOOBE / etc. and wrap & rethrow later...
        try {
          final Phrase p = phrases.get(i);
          final NamedList<Object> data = shardData.get(i);
          // sanity check the correct phrase
          if (! p.getChecksum().equals(data.get("checksum"))) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                    "phrase #" + i + " in shard data had invalid checksum");
          }
          if (p.is_indexed) {
            for (Map.Entry<String,Long> ttf : (NamedList<Long>) data.get("ttf")) {
              p.phrase_ttf.merge(ttf.getKey(), ttf.getValue(), Long::sum);
            }
            for (Map.Entry<String,Long> df : (NamedList<Long>) data.get("df")) {
              p.phrase_df.merge(df.getKey(), df.getValue(), Long::sum);
            }
          }
          for (Map.Entry<String,Long> conj_dc : (NamedList<Long>) data.get("conj_dc")) {
            p.subTerms_conjunctionCounts.merge(conj_dc.getKey(), conj_dc.getValue(), Long::sum);
          }
        } catch (RuntimeException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                  "shard data for phrase#" + i + " not consistent", e);
        }
      }
    }
    
    /**
     * Populates the phrases with stats from the local index for the specified fields 
     */
    public static void populateStats(final List<Phrase> phrases, final Collection<String> fieldNames,
                                     final SolrIndexSearcher searcher) throws IOException {
      final IndexReader reader = searcher.getIndexReader();
      for (String field : fieldNames) {
        for (Phrase phrase : phrases) {
          if (phrase.is_indexed) {
            // add stats based on this entire phrase as an indexed term
            final Term t = new Term(field, phrase.bytes);
            phrase.phrase_ttf.put(field, reader.totalTermFreq(t));
            phrase.phrase_df.put(field, (long)reader.docFreq(t));
          }

          // even if our phrase is too long to be indexed whole, add stats based on the
          // conjunction of all the individual terms in the phrase
          List<Query> filters = new ArrayList<>(phrase.individualIndexedTerms.size());
          for (Phrase term : phrase.individualIndexedTerms) {
            // trust the SolrIndexSearcher to cache & intersect the individual terms so that this
            // can be efficient regardless of how often terms are re-used multiple times in the input/phrases
            filters.add(new TermQuery(new Term(field, term.bytes)));
          }
          final long count = searcher.getDocSet(filters).size();
          phrase.subTerms_conjunctionCounts.put(field, count);
        }
      }
    }
    
    /** 
     * Uses the previously popuated stats to populate each Phrase with it's scores for the specified fields, 
     * and it's over all (weighted) total score.  This is not needed on shard requests.
     * 
     * @see #populateStats
     * @see #getFieldScore(String)
     * @see #getTotalScore
     */
    public static void populateScores(final PhrasesContextData contextData) {
      populateScores(contextData.allPhrases, contextData.fieldWeights, 
                     contextData.maxIndexedPositionLength,
                     contextData.maxQueryPositionLength);
    }
    
    /** 
     * Public for testing purposes
     * @see #populateScores(PhrasesIdentificationComponent.PhrasesContextData)
     * @lucene.internal
     */
    public static void populateScores(final List<Phrase> phrases, final Map<String,Double> fieldWeights,
                                      final int maxIndexedPositionLength,
                                      final int maxQueryPositionLength) {
      final double total_weight = fieldWeights.values().stream().mapToDouble(Double::doubleValue).sum();
      for (Phrase phrase : phrases) {
        double phrase_cumulative_score = 0.0D;
        for (Map.Entry<String,Double> entry : fieldWeights.entrySet()) {
          final String field = entry.getKey();
          final double weight = entry.getValue();
          double field_score = computeFieldScore(phrase, field,
                                                 maxIndexedPositionLength, maxQueryPositionLength);
          phrase.fieldScores.put(field,field_score);
          phrase_cumulative_score += (field_score * weight);
        }
        phrase.total_score = (total_weight < 0 ? Double.NEGATIVE_INFINITY
                              : (phrase_cumulative_score / total_weight));
      }
    }
    
    private Phrase() {
      // No-Op
    }

    private boolean is_indexed;
    private double total_score = -1.0D; // until we get a computed score, this is "not a phrase"
    
    private CharSequence subSequence;
    private BytesRef bytes;
    private int offset_start;
    private int offset_end;
    private int position_start;
    private int position_end;
    private Integer checksum = null;
    
    /** NOTE: Indexed phrases of length 1 are the (sole) individual terms of themselves */
    private final List<Phrase> individualIndexedTerms = new ArrayList<>(7);
    /** 
     * NOTE: Indexed phrases of length less then the max indexed length are the (sole) 
     * largest sub-phrases of themselves 
     */
    private final List<Phrase> largestIndexedSubPhrases = new ArrayList<>(7);
    /** Phrases larger then this phrase which are indexed and fully contain it */
    private final List<Phrase> indexedSuperPhrases = new ArrayList<>(7);
    
    // NOTE: keys are field names
    private final Map<String,Long> subTerms_conjunctionCounts = new TreeMap<>();
    private final Map<String,Long> phrase_ttf = new TreeMap<>();
    private final Map<String,Long> phrase_df = new TreeMap<>();
    private final Map<String,Double> fieldScores = new TreeMap<>();

    public String toString() {
      return "'" + subSequence + "'"
        + "[" + offset_start + ":" + offset_end + "]"
        + "[" + position_start + ":" + position_end + "]";
    }

    @SuppressWarnings({"rawtypes"})
    public NamedList getDetails() {
      SimpleOrderedMap<Object> out = new SimpleOrderedMap<Object>();
      out.add("text", subSequence);
      out.add("offset_start", getOffsetStart());
      out.add("offset_end", getOffsetEnd());
      out.add("score", getTotalScore());
      out.add("field_scores", fieldScores);
      return out;
    }
    
    /** 
     * Computes &amp; caches the checksum of this Phrase (if not already cached).
     * needed only when merging shard data to validate no inconsistencies with the remote shards
     */
    private Integer getChecksum() {
      if (null == checksum) {
        checksum = Arrays.hashCode(new int[] { offset_start, offset_end, position_start, position_end });
      }
      return checksum;
    }
    /** The characters from the original input that corrispond with this Phrase */
    public CharSequence getSubSequence() {
      return subSequence;
    }
    
    /** 
     * Returns the list of "individual" (ie: <code>getPositionLength()==1</code> terms.
     * NOTE: Indexed phrases of length 1 are the (sole) individual terms of themselves
     */
    public List<Phrase> getIndividualIndexedTerms() { 
      return individualIndexedTerms;
    }
    /** 
     * Returns the list of (overlapping) sub phrases that have the largest possible size based on 
     * the effective value of {@link PhrasesContextData#maxIndexedPositionLength}. 
     * NOTE: Indexed phrases of length less then the max indexed length are the (sole) 
     * largest sub-phrases of themselves.
     */
    public List<Phrase> getLargestIndexedSubPhrases() {
      return largestIndexedSubPhrases;
    }
    /** 
     * Returns all phrases larger then this phrase, which fully include this phrase, and are indexed.
     * NOTE: A Phrase is <em>never</em> the super phrase of itself.
     */
    public List<Phrase> getIndexedSuperPhrases() {
      return indexedSuperPhrases;
    }

    /** NOTE: positions start at '1' */
    public int getPositionStart() {
      return position_start;
    }
    /** NOTE: positions start at '1' */
    public int getPositionEnd() {
      return position_end;
    }
    public int getPositionLength() {
      return position_end - position_start;
    }
    /** Each set bit identifies a position filled by this Phrase */
    public BitSet getPositionsBitSet() {
      final BitSet result = new BitSet();
      result.set(position_start, position_end);
      return result;
    }
    public int getOffsetStart() {
      return offset_start;
    }
    public int getOffsetEnd() {
      return offset_end;
    }
    
    /** 
     * Returns the overall score for this Phrase.  In the current implementation, 
     * the only garuntee made regarding the range of possible values is that 0 (or less) means 
     * it is not a good phrase.
     *
     * @return A numeric value indicating the confidence in this Phrase, higher numbers are higher confidence.
     */
    public double getTotalScore() {
      return total_score;
    }
    /** 
     * Returns the score for this Phrase in this given field. In the current implementation, 
     * the only garuntee made regarding the range of possible values is that 0 (or less) means 
     * it is not a good phrase.
     *
     * @return A numeric value indicating the confidence in this Phrase for this field, higher numbers are higher confidence.
     */
    public double getFieldScore(String field) {
      return fieldScores.getOrDefault(field, -1.0D);
    }
    
    /** 
     * Returns the number of total TTF of this (indexed) Phrase <em>as term</em> in the specified field. 
     * NOTE: behavior of calling this method is undefined unless one of the {@link #populateStats} 
     * methods has been called with this field.
     */
    public long getTTF(String field) {
      if (!is_indexed) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                "TTF is only available for indexed phrases");
      }
      return phrase_ttf.getOrDefault(field, 0L);
    }
    /** 
     * Returns the number of documents that contain <em>all</em> of the {@link #getIndividualIndexedTerms} 
     * that make up this Phrase, in the specified field. 
     * NOTE: behavior of calling this method is undefined unless one of the {@link #populateStats} 
     * methods has been called with this field.
     */
    public long getConjunctionDocCount(String field) {
      return subTerms_conjunctionCounts.getOrDefault(field, 0L);
    }
    /** 
     * Returns the number of documents that contain this (indexed) Phrase <em>as term</em> 
     * in the specified field. 
     * NOTE: behavior of calling this method is undefined unless one of the {@link #populateStats} 
     * methods has been called with this field.
     */
    public long getDocFreq(String field) {
      if (!is_indexed) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                "DF is only available for indexed phrases");
      }
      return phrase_df.getOrDefault(field, 0L);
    }

    /** 
     * Uses the previously popuated stats to compute a score for the specified field.
     *
     * <p>
     * The current implementation returns scores in the range of <code>[0,1]</code>, but this 
     * may change in future implementations.  The only current garuntees are:
     * </p>
     * 
     * <ul>
     * <li>0 (or less) means this is garunteed to not be a phrase</li>
     * <li>larger numbers are higher confidence</li>
     * </li>
     * 
     * @see #populateStats
     * @see #populateScores
     * @see #getFieldScore(String)
     * @return a score value
     */
    private static double computeFieldScore(final Phrase input,
                                            final String field,
                                            final int maxIndexedPositionLength,
                                            final int maxQueryPositionLength) {
      final long num_indexed_sub_phrases = input.getLargestIndexedSubPhrases().size();
      assert 0 <= num_indexed_sub_phrases; // should be impossible

      if (input.getIndividualIndexedTerms().size() < input.getPositionLength()) {
        // there are "gaps" in our input, where individual words have not been indexed (stop words, 
        // or multivalue position gap) which means we are not a viable candidate for being a valid Phrase.
        return -1.0D;
      }
      
      final long phrase_conj_count = input.getConjunctionDocCount(field);
      // if there isn't a single document containing all the terms in our
      // phrase, then it is 100% not a phrase
      if (phrase_conj_count <= 0) {
        return -1.0D;
      }
      
      // single words automatically score 0.0 (unless they already scored less for not existing
      if (input.getPositionLength() <= 1) {
        return 0.0D;
      }
      
      double field_score = 0.0D;
      long max_sub_conj_count = phrase_conj_count;
      
      // At the moment, the contribution of each "words" sub-Phrase to the field score to the input
      // Phrase is independent of any context of "input".  Depending on if/how sub-phrase scoring
      // changes, we might consider computing the scores of all the indexed phrases first, and
      // aching the portions of their values that are re-used when computing the scores of
      // longer phrases?
      //
      // This would make the overall scoring of all phrases a lot more complicated,
      // but could save CPU cycles? 
      // (particularly when maxIndexedPositionLength <<< maxQueryPositionLength ???)
      //
      // My gut says that knowing the conj_count(input) "context" should help us score the 
      // sub-phrases better, but i can't yet put my finger on why/how.  maybe by comparing
      // the conj_count(input) to the max(conj_count(parent of words)) ?
      
      // for each of the longest indexed phrases, aka indexed sub-sequence of "words", we have...
      for (Phrase words : input.getLargestIndexedSubPhrases()) {
        // we're going to compute scores in range of [-1:1] to indicate the likelihood that our
        // "words" should be used as a "phrase", based on a bayesian document categorization model,
        // where the "words as a phrase" (aka: phrase) is our candidate category.
        //
        //  P(words|phrase) * P(phrase) - P(words|not phrase) * P(not phrase)
        //
        // Where...
        //  P(words|phrase)     =  phrase_ttf / min(word_ttf)
        //  P(phrase)           =~ phrase_docFreq / conj_count(words in phrase)      *SEE NOTE BELOW*
        //  P(words|not phrase) =  phrase_ttf / max(word_ttf) 
        //  P(not a phrase)     =  1 - P(phrase)
        //
        //       ... BUT! ...
        //
        // NOTE: we're going to reduce our "P(phrase) by the max "P(phrase)" of all the (indexed)
        // candidate phrases we are a sub-phrase of, to try to offset the inherent bias in favor 
        // of small indexed phrases -- because anytime the super-phrase exists, the sub-phrase exists

        
        // IDEA: consider replacing this entire baysian model with LLR (or rootLLR)...
        //  http://mahout.apache.org/docs/0.13.0/api/docs/mahout-math/org/apache/mahout/math/stats/LogLikelihood.html
        // ...where we compute LLR over each of the TTF of the pairs of adjacent sub-phrases of each 
        // indexed phrase and take the min|max|avg of the LLR scores.
        //
        // ie: for indexed shingle "quick brown fox" compute LLR(ttf("quick"), ttf("brown fox")) &
        // LLR(ttf("quick brown"), ttf("fox")) using ttf("quick brown fox") as the co-occurance
        // count, and sumTTF-ttf("quick")-ttf("brown")-ttf("fox") as the "something else"
        //
        // (we could actually compute LLR stats over TTF and DF and combine them)
        //
        // NOTE: Going the LLR/rootLLR route would require building a full "tree" of every (indexed)
        // sub-phrase of every other phrase (or at least: all siblings of diff sizes that add up to
        // an existing phrase).  As well as require us to give up on a predictible "range" of
        // legal values for scores (IIUC from the LLR docs)
        
        final long phrase_ttf = words.getTTF(field);
        final long phrase_df = words.getDocFreq(field);
        final long words_conj_count = words.getConjunctionDocCount(field);
        max_sub_conj_count = Math.max(words_conj_count, max_sub_conj_count);
        
        final double max_wrapper_phrase_probability = 
          words.getIndexedSuperPhrases().stream()
          .mapToDouble(p -> p.getConjunctionDocCount(field) <= 0 ?
                       // special case check -- we already know *our* conj count > 0,
                       // but we need a similar check for wrapper phrases: if <= 0, their probability is 0
                       0.0D : ((double)p.getDocFreq(field) / p.getConjunctionDocCount(field))).max().orElse(0.0D);
        
        final LongSummaryStatistics words_ttfs = 
          words.getIndividualIndexedTerms().stream()
          .collect(Collectors.summarizingLong(t -> t.getTTF(field)));
        
        final double words_phrase_prob = (phrase_ttf / (double)words_ttfs.getMin());
        final double words_not_phrase_prob = (phrase_ttf / (double)words_ttfs.getMax());
        
        final double phrase_prob = (phrase_conj_count / (double)words_conj_count);
        
          
        final double phrase_score = words_phrase_prob * (phrase_prob - max_wrapper_phrase_probability);
        final double not_phrase_score =  words_not_phrase_prob * (1 - (phrase_prob - max_wrapper_phrase_probability));
        final double words_score = phrase_score - not_phrase_score;
        
        field_score += words_score;
      }

      // NOTE: the "scaling" factors below can "increase" negative scores (by reducing the unsigned value)
      // when they should ideally be penalizing the scores further, but since we currently don't care
      // about any score lower then 0, it's not worth worrying about.
      
      // Average the accumulated score over the number of actual indexed sub-phrases that contributed
      //
      // NOTE: since we subsequently want to multiply the score by a fraction with num_indexed_sub_phrases
      // in the numerator, we can skip this...
      // SEE BELOW // field_score /= (double) num_indexed_sub_phrases;
      
      // If we leave field_score as is, then a phrase longer then the maxIndexedPositionLength
      // will never score higher then the highest scoring sub-phrase it has (because we've averaged them)
      // so we scale the scores against the longest possible phrase length we're considering
      //
      // NOTE: We don't use num_indexed_sub_phrases in the numerator since we skipped it when
      // averating above...
      field_score *= ( 1.0D // SEE ABOVE // * ( (double)num_indexed_sub_phrases )
                       / (1 + maxQueryPositionLength - maxIndexedPositionLength) );
      
      // scale the field_score based on the ratio of the conjunction docCount for the whole phrase
      // realtive to the largest conjunction docCount of it's (largest indexed) sub phrases, to penalize
      // the scores of very long phrases that exist very rarely relative to the how often their
      // sub phrases exist in the index
      field_score *= ( ((double) phrase_conj_count) / max_sub_conj_count);

      return field_score;
    }
  }

  /** 
   * Helper method, public for testing purposes only.
   * <p>
   * Given an analyzer, inspects it to determine if:
   * <ul>
   *  <li>it is a {@link TokenizerChain}</li>
   *  <li>it contains exactly one instance of {@link ShingleFilterFactory}</li>
   * </ul>
   * <p>
   * If these these conditions are met, then this method returns the <code>maxShingleSize</code> 
   * in effect for this analyzer, otherwise returns -1.
   * </p>
   * 
   * @param analyzer An analyzer inspect
   * @return <code>maxShingleSize</code> if available
   * @lucene.internal
   */
  public static int getMaxShingleSize(Analyzer analyzer) {
    if (!TokenizerChain.class.isInstance(analyzer)) {
      return -1;
    }
    
    final TokenFilterFactory[] factories = ((TokenizerChain) analyzer).getTokenFilterFactories();
    if (0 == factories.length) {
      return -1;
    }
    int result = -1;
    for (TokenFilterFactory tff : factories) {
      if (ShingleFilterFactory.class.isInstance(tff)) {
        if (0 < result) {
          // more then one shingle factory in our analyzer, which is weird, so make no assumptions...
          return -1;
        }
        // would be nice if there was an easy way to just ask a factory for the effective value
        // of an arguement...
        final Map<String,String> args = tff.getOriginalArgs();
        result = args.containsKey("maxShingleSize")
          ? Integer.parseInt(args.get("maxShingleSize")) : ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE;
      }
    }
    return result;
  }
}
