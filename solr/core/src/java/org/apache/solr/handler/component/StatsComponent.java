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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.DocSet;

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
    Map<String, StatsValues> statsValues = new LinkedHashMap<>();

    for (StatsField statsField : rb._statsInfo.getStatsFields()) {
      DocSet docs = statsField.computeBaseDocSet();
      statsValues.put(statsField.getOutputKey(), statsField.computeLocalStatsValues(docs));
    }
    
    rb.rsp.add( "stats", convertToResponse(statsValues) );
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
        stats = (NamedList<NamedList<NamedList<?>>>) 
          srsp.getSolrResponse().getResponse().get("stats");
      } catch (Exception e) {
        if (rb.req.getParams().getBool(ShardParams.SHARDS_TOLERANT, false)) {
          continue; // looks like a shard did not return anything
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unable to read stats info for shard: " + srsp.getShard(), e);
      }

      NamedList stats_fields = unwrapStats(stats);
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
    rb.rsp.add("stats", convertToResponse(allStatsValues));

    rb._statsInfo = null; // free some objects 
  }

  /**
   * Helper to pull the "stats_fields" out of the extra "stats" wrapper
   */
  public static NamedList<NamedList<?>> unwrapStats(NamedList<NamedList<NamedList<?>>> stats) {
    if (null == stats) return null;

    return stats.get("stats_fields");
  }

  /**
   * Given a map of {@link StatsValues} using the appropriate response key,
   * builds up the necessary "stats" data structure for including in the response -- 
   * including the esoteric "stats_fields" wrapper.
   */
  public static NamedList<NamedList<NamedList<?>>> convertToResponse
    (Map<String,StatsValues> statsValues) {

    NamedList<NamedList<NamedList<?>>> stats = new SimpleOrderedMap<>();
    NamedList<NamedList<?>> stats_fields = new SimpleOrderedMap<>();
    stats.add("stats_fields", stats_fields);
    
    for (Map.Entry<String,StatsValues> entry : statsValues.entrySet()) {
      String key = entry.getKey();
      NamedList stv = entry.getValue().getStatsValues();
      stats_fields.add(key, stv);
    }
    return stats;
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
  private final Map<String, StatsField> statsFieldMap = new LinkedHashMap<>();
  private final Map<String, List<StatsField>> tagToStatsFields = new LinkedHashMap<>();

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
      for (String tag : current.getTagList()) {
        List<StatsField> fieldList = tagToStatsFields.get(tag);
        if (fieldList == null) {
          fieldList = new ArrayList<>();
        }
        fieldList.add(current);
        tagToStatsFields.put(tag, fieldList);
      }
      statsFieldMap.put(current.getOutputKey(), current);
      distribStatsValues.put(current.getOutputKey(), 
                             StatsValuesFactory.createStatsValues(current));
    }
  }

  /**
   * Returns an immutable list of {@link StatsField} instances
   * modeling each of the {@link StatsParams#STATS_FIELD} params specified
   * as part of this request
   */
  public List<StatsField> getStatsFields() {
    return Collections.unmodifiableList(statsFields);
  }

  /**
   * Returns the {@link StatsField} associated with the specified (effective) 
   * outputKey, or null if there was no {@link StatsParams#STATS_FIELD} param
   * that would corrispond with that key.
   */
  public StatsField getStatsField(String outputKey) {
    return statsFieldMap.get(outputKey);
  }

  /**
   * Return immutable list of {@link StatsField} instances by string tag local parameter.
   *
   * @param tag tag local parameter
   * @return list of stats fields
   */
  public List<StatsField> getStatsFieldsByTag(String tag) {
    List<StatsField> raw = tagToStatsFields.get(tag);
    if (null == raw) {
      return Collections.emptyList();
    } else {
      return Collections.unmodifiableList(raw);
    }
  }

  /**
   * Returns an immutable map of response key =&gt; {@link StatsValues}
   * instances for the current distributed request.  
   * Depending on where we are in the process of handling this request, 
   * these {@link StatsValues} instances may not be complete -- but they 
   * will never be null.
   */
  public Map<String, StatsValues> getAggregateStatsValues() {
    return Collections.unmodifiableMap(distribStatsValues);
  }

}

