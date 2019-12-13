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
package org.apache.solr.analytics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.analytics.facet.AbstractSolrQueryFacet;
import org.apache.solr.analytics.facet.AbstractSolrQueryFacet.FacetValueQueryExecuter;
import org.apache.solr.analytics.facet.StreamingFacet;
import org.apache.solr.analytics.function.ExpressionCalculator;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.function.ReductionCollectionManager.ReductionDataCollection;
import org.apache.solr.analytics.stream.AnalyticsShardRequestManager;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.Filter;

/**
 * The manager of an entire analytics request.
 */
public class AnalyticsRequestManager {
  private final ReductionCollectionManager ungroupedReductionManager;
  private ReductionDataCollection ungroupedData;

  private final Map<String, AnalyticsGroupingManager> groupingManagers;

  private final Collection<AnalyticsExpression> ungroupedExpressions;
  private final ExpressionCalculator ungroupedExpressionCalculator;

  /**
   * If the request is distributed, the manager for shard requests.
   */
  public String analyticsRequest;
  public AnalyticsShardRequestManager shardStream;
  public boolean sendShards;
  private boolean partialResults = false;

  /**
   * Create an manager with the given ungrouped expressions. This is straightforward in the new
   * style of request, however in the old olap-style requests all groupings' expressions are expected
   * to be ungrouped as well.
   *
   *
   * @param ungroupedReductionManager to manage the reduction collection for all ungrouped expressions
   * @param ungroupedExpressions to compute overall results for
   */
  public AnalyticsRequestManager(ReductionCollectionManager ungroupedReductionManager,
                                 Collection<AnalyticsExpression> ungroupedExpressions) {
    this.ungroupedReductionManager = ungroupedReductionManager;
    this.ungroupedData = ungroupedReductionManager.newDataCollection();
    this.ungroupedReductionManager.addLastingCollectTarget(ungroupedData);

    this.ungroupedExpressions = ungroupedExpressions;
    this.ungroupedExpressionCalculator = new ExpressionCalculator(ungroupedExpressions);
    this.groupingManagers = new HashMap<>();
  }

  /**
   * Get the collection manager for ungrouped expressions, including grouped expressions if
   * the old request notation is used.
   *
   * @return the collection manager for the ungrouped expressions
   */
  public ReductionCollectionManager getUngroupedCollectionManager() {
    return ungroupedReductionManager;
  }

  /**
   * Get the collection manager for all ungrouped expressions, including grouped expressions if
   * the old request notation is used.
   *
   * @return the collection manager for the ungrouped expressions
   */
  public ReductionDataCollection getUngroupedData() {
    return ungroupedData;
  }

  /**
   * Return all ungrouped expressions, including grouped expressions if
   * the old request notation is used.
   *
   * @return an {@link Iterable} of the ungrouped expressions
   */
  public Iterable<AnalyticsExpression> getUngroupedExpressions() {
    return ungroupedExpressions;
  }

  /**
   * Generate the results of all ungrouped expressions, including grouped expressions if
   * the old request notation is used.
   *
   * @param response the response to add the ungrouped results to.
   */
  public void addUngroupedResults(Map<String,Object> response) {
    ungroupedReductionManager.setData(ungroupedData);
    ungroupedExpressionCalculator.addResults(response);
  }

  /**
   * Generate the results of all ungrouped expressions, including grouped expressions if
   * the old request notation is used.
   *
   * @return the map containing the ungrouped results
   */
  public Map<String,Object> getUngroupedResults() {
    ungroupedReductionManager.setData(ungroupedData);
    return ungroupedExpressionCalculator.getResults();
  }

  /**
   * Add a grouping to the request.
   *
   * @param groupingManager that manages the grouping
   */
  public void addGrouping(AnalyticsGroupingManager groupingManager) {
    groupingManagers.put(groupingManager.getName(), groupingManager);
  }

  /**
   * Import the shard data for this request from a bit-stream,
   * exported by the {@link #exportShardData} method in the each of the collection's shards.
   * <p>
   * First the overall data is imported, then the grouping data is imported.
   *
   * @param input The bit-stream to import the shard data from
   * @throws IOException if an exception occurs while reading from the {@link DataInput}
   */
  public synchronized void importShardData(DataInput input) throws IOException {
    ungroupedReductionManager.setShardInput(input);

    // The ungroupedData will not exist for the first shard imported
    if (ungroupedData == null) {
      ungroupedData = ungroupedReductionManager.newDataCollectionIO();
    } else {
      ungroupedReductionManager.prepareReductionDataIO(ungroupedData);
    }
    ungroupedReductionManager.mergeData();

    int size = input.readInt();
    while (--size >= 0) {
      String groupingName = input.readUTF();
      groupingManagers.get(groupingName).importShardData(input);
    }
  }

  /**
   * Export the shard data for this request through a bit-stream,
   * to be imported by the {@link #importShardData} method in the originating shard.
   * <p>
   * First the overall data is exported, then the grouping data is exported.
   *
   * @param output The bit-stream to output the shard data through
   * @throws IOException if an exception occurs while writing to the {@link DataOutput}
   */
  public void exportShardData(DataOutput output) throws IOException {
    ungroupedReductionManager.setShardOutput(output);

    ungroupedReductionManager.prepareReductionDataIO(ungroupedData);
    ungroupedReductionManager.exportData();

    output.writeInt(groupingManagers.size());
    for (Map.Entry<String, AnalyticsGroupingManager> entry : groupingManagers.entrySet()) {
      output.writeUTF(entry.getKey());
      entry.getValue().exportShardData(output);
    }
  }

  /**
   * Consolidate the information of all {@link StreamingFacet}s contained within the request, since
   * they need to be collected along with the overall results during the streaming phase of the
   * {@link AnalyticsDriver}.
   *
   * @return the info for all {@link StreamingFacet}s
   */
  public StreamingInfo getStreamingFacetInfo() {
    StreamingInfo streamingInfo = new StreamingInfo();
    ArrayList<ReductionCollectionManager> groupingCollectors = new ArrayList<>();
    groupingManagers.values().forEach( grouping -> {
      // If a grouping has streaming facets, then that groupings expressions
      // must be collected during the streaming phase.
      if (grouping.getStreamingFacets( facet -> streamingInfo.streamingFacets.add(facet) )) {
        groupingCollectors.add(grouping.getReductionManager());
      }
    });

    // Create an streaming collection manager to manage the collection of all ungrouped expressions and
    // grouped expressions that are calculated over streaming facets.
    streamingInfo.streamingCollectionManager = ungroupedReductionManager.merge(groupingCollectors);
    return streamingInfo;
  }

  /**
   * Class to encapsulate all necessary data for collecting {@link StreamingFacet}s.
   */
  public static class StreamingInfo {
    Collection<StreamingFacet> streamingFacets = new ArrayList<>();
    /**
     * Manages the collection of all expressions needed for streaming facets
     */
    ReductionCollectionManager streamingCollectionManager;
  }

  /**
   * Create the {@link FacetValueQueryExecuter}s for all {@link AbstractSolrQueryFacet}s contained in the request.
   *
   * @param filter representing the overall search query
   * @param queryRequest of the overall search query
   * @return an {@link Iterable} of executers
   */
  public Iterable<FacetValueQueryExecuter> getFacetExecuters(Filter filter, SolrQueryRequest queryRequest) {
    ArrayList<FacetValueQueryExecuter> facetExecutors = new ArrayList<>();
    groupingManagers.values().forEach( grouping -> {
      grouping.getFacetExecuters(filter, queryRequest, executor -> facetExecutors.add(executor));
    });
    return facetExecutors;
  }

  /**
   * Create the response for a request given in the old olap-style format.
   * The old response returned overall expressions within groupings.
   *
   * @return a {@link NamedList} representation of the response
   */
  public NamedList<Object> createOldResponse() {
    NamedList<Object> analyticsResponse = new NamedList<>();
    Map<String,Object> ungroupedResults = getUngroupedResults();
    groupingManagers.forEach( (name, groupingManager) -> {
      analyticsResponse.add(name, groupingManager.createOldResponse(ungroupedResults));
    });

    return analyticsResponse;
  }

  /**
   * Create the response for a request.
   *
   * <p>
   * NOTE: Analytics requests specified in the old olap-style format
   * have their responses generated by {@link #createOldResponse()}.
   *
   * @return a {@link Map} representation of the response
   */
  public Map<String,Object> createResponse() {
    Map<String,Object> analyticsResponse = new HashMap<>();
    if (ungroupedExpressions.size() > 0) {
      addUngroupedResults(analyticsResponse);
    }

    Map<String,Object> groupingsResponse = new HashMap<>();
    groupingManagers.forEach( (name, groupingManager) -> {
      groupingsResponse.put(name, groupingManager.createResponse());
    });

    if (groupingsResponse.size() > 0) {
      analyticsResponse.put(AnalyticsResponseHeadings.GROUPINGS, groupingsResponse);
    }
    return analyticsResponse;
  }

  public void setPartialResults(boolean b) {
    this.partialResults=b;
  }

  public boolean isPartialResults() {
    return partialResults;
  }
  
  
}