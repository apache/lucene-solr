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
import java.util.Collections;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.DocValuesStats;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

/**
 * Stats component calculates simple statistics on numeric field values
 * @since solr 1.4
 */
public class StatsComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "stats";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(StatsParams.STATS,false)) {
      rb.setNeedDocSet( true );
      rb.doStats = true;
      rb._statsInfo = new StatsInfo(rb);
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (!rb.doStats) return;

    boolean isShard = rb.req.getParams().getBool(ShardParams.IS_SHARD, false);
    NamedList<Object> out = new SimpleOrderedMap<>();
    NamedList<Object> stats_fields = new SimpleOrderedMap<>();

    for (StatsField statsField : rb._statsInfo.getStatsFields()) {
      DocSet docs = statsField.computeBaseDocSet();
      NamedList<?> stv = statsField.computeLocalStatsValues(docs).getStatsValues();
      
      if (isShard == true || (Long) stv.get("count") > 0) {
        stats_fields.add(statsField.getOutputKey(), stv);
      } else {
        stats_fields.add(statsField.getOutputKey(), null);
      }
    }
    
    out.add("stats_fields", stats_fields);
    rb.rsp.add( "stats", out );
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.doStats) return;

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) != 0) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_STATS;
    } else {
      // turn off stats on other requests
      sreq.params.set(StatsParams.STATS, "false");
      // we could optionally remove stats params
    }
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (!rb.doStats || (sreq.purpose & ShardRequest.PURPOSE_GET_STATS) == 0) return;

    Map<String, StatsValues> allStatsValues = rb._statsInfo.getAggregateStatsValues();

    for (ShardResponse srsp : sreq.responses) {
      NamedList stats = null;
      try {
        stats = (NamedList) srsp.getSolrResponse().getResponse().get("stats");
      } catch (Exception e) {
        if (rb.req.getParams().getBool(ShardParams.SHARDS_TOLERANT, false)) {
          continue; // looks like a shard did not return anything
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unable to read stats info for shard: " + srsp.getShard(), e);
      }

      NamedList stats_fields = (NamedList) stats.get("stats_fields");
      if (stats_fields != null) {
        for (int i = 0; i < stats_fields.size(); i++) {
          String key = stats_fields.getName(i);
          StatsValues stv = allStatsValues.get(key);
          NamedList shardStv = (NamedList) stats_fields.get(key);
          stv.accumulate(shardStv);
        }
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (!rb.doStats || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
    // wait until STAGE_GET_FIELDS
    // so that "result" is already stored in the response (for aesthetics)

    Map<String, StatsValues> allStatsValues = rb._statsInfo.getAggregateStatsValues();

    NamedList<NamedList<Object>> stats = new SimpleOrderedMap<>();
    NamedList<Object> stats_fields = new SimpleOrderedMap<>();
    stats.add("stats_fields", stats_fields);
    
    for (Map.Entry<String,StatsValues> entry : allStatsValues.entrySet()) {
      String key = entry.getKey();
      NamedList stv = entry.getValue().getStatsValues();
      if ((Long) stv.get("count") != 0) {
        stats_fields.add(key, stv);
      } else {
        stats_fields.add(key, null);
      }
    }

    rb.rsp.add("stats", stats);
    rb._statsInfo = null; // free some objects 
  }


  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Calculate Statistics";
  }
}

/**
 * Models all of the information about stats needed for a single request
 * @see StatsField
 */
class StatsInfo {

  private final ResponseBuilder rb;
  private final List<StatsField> statsFields = new ArrayList<>(7);
  private final Map<String, StatsValues> distribStatsValues = new LinkedHashMap<>();

  public StatsInfo(ResponseBuilder rb) { 
    this.rb = rb;
    SolrParams params = rb.req.getParams();
    String[] statsParams = params.getParams(StatsParams.STATS_FIELD);
    if (null == statsParams) {
      // no stats.field params, nothing to parse.
      return;
    }

    for (String paramValue : statsParams) {
      StatsField current = new StatsField(rb, paramValue);
      statsFields.add(current);
      distribStatsValues.put(current.getOutputKey(), current.buildNewStatsValues());
    }
  }

  /**
   * Returns an immutable list of {@link StatsField} instances
   * modeling each of the {@link StatsParams#STATS_FIELD} params specified
   * as part of this request
   */
  public List<StatsField> getStatsFields() {
    return Collections.<StatsField>unmodifiableList(statsFields);
  }

  /**
   * Returns an immutable map of response key =&gt; {@link StatsValues}
   * instances for the current distributed request.  
   * Depending on where we are in the process of handling this request, 
   * these {@link StatsValues} instances may not be complete -- but they 
   * will never be null.
   */
  public Map<String, StatsValues> getAggregateStatsValues() {
    return Collections.<String, StatsValues>unmodifiableMap(distribStatsValues);
  }

}

/**
 * Models all of the information associated with a single {@link StatsParams#STATS_FIELD}
 * instance.
 */
class StatsField {

  private final SolrIndexSearcher searcher;
  private final ResponseBuilder rb;
  private final String originalParam; // for error messages
  private final SolrParams localParams;
  private final SchemaField sf;
  private final String fieldName;
  private final String key;
  private final boolean calcDistinct;
  private final String[] facets;
  private final List<String> excludeTagList;

  /**
   * @param rb the current request/response
   * @param statsParam the raw {@link StatsParams#STATS_FIELD} string
   */
  public StatsField(ResponseBuilder rb, String statsParam) { 
    this.rb = rb;
    this.searcher = rb.req.getSearcher();
    this.originalParam = statsParam;

    SolrParams params = rb.req.getParams();

    try {
      SolrParams localParams = QueryParsing.getLocalParams(statsParam, params);
      if (null == localParams) {
        localParams = new ModifiableSolrParams();
      }
      this.localParams = localParams;
    } catch (SyntaxError e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to parse " + 
                              StatsParams.STATS_FIELD + ": " + originalParam + " due to: "
                              + e.getMessage(), e);
    }

    // pull fieldName out of localParams, or default to original param value
    this.fieldName = localParams.get(CommonParams.VALUE, statsParam);
    // allow explicit set of the key via localparams, default to fieldName
    this.key = localParams.get(CommonParams.OUTPUT_KEY, fieldName);

    calcDistinct = params.getFieldBool(fieldName, StatsParams.STATS_CALC_DISTINCT, false);

    String[] facets = params.getFieldParams(key, StatsParams.STATS_FACET);
    this.facets = (null == facets) ? new String[0] : facets;

    // figure out if we need a new base DocSet
    String excludeStr = localParams.get(CommonParams.EXCLUDE);
    this.excludeTagList = (null == excludeStr) 
      ? Collections.<String>emptyList()
      : StrUtils.splitSmart(excludeStr,',');

    this.sf = searcher.getSchema().getField(fieldName);
  }

  /** 
   * The key to be used when refering to this {@link StatsField} instance in the 
   * response tp clients.
   */
  public String getOutputKey() {
    return key;
  }

  /**
   * Returns a new, empty, {@link StatsValues} instance that can be used for
   * accumulating the appropriate stats from this {@link StatsField}
   */
  public StatsValues buildNewStatsValues() {
    return StatsValuesFactory.createStatsValues(sf, calcDistinct);
  }

  /**
   * Computes a base {@link DocSet} for the current request to be used
   * when computing global stats for the local index.
   *
   * This is typically the same as the main DocSet for the {@link ResponseBuilder}
   * unless {@link CommonParams#TAG tag}ged filter queries have been excluded using 
   * the {@link CommonParams#EXCLUDE ex} local param
   */
  public DocSet computeBaseDocSet() throws IOException {

    DocSet docs = rb.getResults().docSet;
    Map<?,?> tagMap = (Map<?,?>) rb.req.getContext().get("tags");

    if (excludeTagList.isEmpty() || null == tagMap) {
      // either the exclude list is empty, or there
      // aren't any tagged filters to exclude anyway.
      return docs;
    }

    IdentityHashMap<Query,Boolean> excludeSet = new IdentityHashMap<Query,Boolean>();
    for (String excludeTag : excludeTagList) {
      Object olst = tagMap.get(excludeTag);
      // tagMap has entries of List<String,List<QParser>>, but subject to change in the future
      if (!(olst instanceof Collection)) continue;
      for (Object o : (Collection<?>)olst) {
        if (!(o instanceof QParser)) continue;
        QParser qp = (QParser)o;
        try {
          excludeSet.put(qp.getQuery(), Boolean.TRUE);
        } catch (SyntaxError e) {
          // this shouldn't be possible since the request should have already
          // failed when attempting to execute the query, but just in case...
          throw new SolrException(ErrorCode.BAD_REQUEST, "Excluded query can't be parsed: " + 
                                  originalParam + " due to: " + e.getMessage(), e);
        }
      }
    }
    if (excludeSet.size() == 0) return docs;
    
    List<Query> qlist = new ArrayList<Query>();
    
    // add the base query
    if (!excludeSet.containsKey(rb.getQuery())) {
      qlist.add(rb.getQuery());
    }
    
    // add the filters
    if (rb.getFilters() != null) {
      for (Query q : rb.getFilters()) {
        if (!excludeSet.containsKey(q)) {
          qlist.add(q);
        }
      }
    }
    
    // get the new base docset for this facet
    return searcher.getDocSet(qlist);
  }

  /**
   * Computes the {@link StatsValues} for this {@link StatsField} relative to the 
   * specified {@link DocSet} 
   * @see #computeBaseDocSet
   */
  public StatsValues computeLocalStatsValues(DocSet base) throws IOException {

    if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
      // TODO: should this also be used for single-valued string fields? (should work fine)
      return DocValuesStats.getCounts(searcher, fieldName, base, calcDistinct, facets);
    } else {
      return getFieldCacheStats(base);
    }
  }

  private StatsValues getFieldCacheStats(DocSet base) throws IOException {
    IndexSchema schema = searcher.getSchema();
    final StatsValues allstats = StatsValuesFactory.createStatsValues(sf, calcDistinct);

    List<FieldFacetStats> facetStats = new ArrayList<>();
    for( String facetField : facets ) {
      SchemaField fsf = schema.getField(facetField);

      if ( fsf.multiValued()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Stats can only facet on single-valued fields, not: " + facetField );
      }

      facetStats.add(new FieldFacetStats(searcher, facetField, sf, fsf, calcDistinct));
    }

    final Iterator<AtomicReaderContext> ctxIt = searcher.getIndexReader().leaves().iterator();
    AtomicReaderContext ctx = null;
    for (DocIterator docsIt = base.iterator(); docsIt.hasNext(); ) {
      final int doc = docsIt.nextDoc();
      if (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc()) {
        // advance
        do {
          ctx = ctxIt.next();
        } while (ctx == null || doc >= ctx.docBase + ctx.reader().maxDoc());
        assert doc >= ctx.docBase;

        // propagate the context among accumulators.
        allstats.setNextReader(ctx);
        for (FieldFacetStats f : facetStats) {
          f.setNextReader(ctx);
        }
      }

      // accumulate
      allstats.accumulate(doc - ctx.docBase);
      for (FieldFacetStats f : facetStats) {
        f.facet(doc - ctx.docBase);
      }
    }

    for (FieldFacetStats f : facetStats) {
      allstats.addFacet(f.name, f.facetStatsValues);
    }
    return allstats;
  }

}
