/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file inputtributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * inputtributed under the License is inputtributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.analytics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.solr.analytics.facet.AnalyticsFacet;
import org.apache.solr.analytics.facet.PivotFacet;
import org.apache.solr.analytics.facet.AbstractSolrQueryFacet;
import org.apache.solr.analytics.facet.QueryFacet;
import org.apache.solr.analytics.facet.RangeFacet;
import org.apache.solr.analytics.facet.StreamingFacet;
import org.apache.solr.analytics.facet.ValueFacet;
import org.apache.solr.analytics.facet.AbstractSolrQueryFacet.FacetValueQueryExecuter;
import org.apache.solr.analytics.function.ExpressionCalculator;
import org.apache.solr.analytics.function.ReductionCollectionManager;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.Filter;

/**
 * The manager for faceted analytics. This class manages one grouping of facets and expressions to compute
 * over those facets.
 *
 * <p>
 * This class will only manage generating faceted results, not overall results.
 */
public class AnalyticsGroupingManager {
  private final String name;
  private final ReductionCollectionManager reductionCollectionManager;

  private final Collection<AnalyticsExpression> topLevelExpressions;
  private final ExpressionCalculator expressionCalculator;

  private final Map<String, AnalyticsFacet> facets;

  public AnalyticsGroupingManager(String name,
                                  ReductionCollectionManager reductionCollectionManager,
                                  Collection<AnalyticsExpression> topLevelExpressions) {
    this.name = name;
    this.reductionCollectionManager = reductionCollectionManager;

    this.topLevelExpressions = topLevelExpressions;
    this.expressionCalculator = new ExpressionCalculator(topLevelExpressions);

    this.facets = new HashMap<>();
  }

  // This is outside of the method, since it is used in the lambda and cannot be a local non-final variable
  private boolean hasStreamingFacets;

  /**
   * Get the {@link StreamingFacet}s (e.g. {@link ValueFacet} and {@link PivotFacet}) contained within this grouping,
   * returning them through the given consumer.
   *
   * @param cons where the streaming facets are passed to
   * @return whether the grouping contains streaming facets
   */
  public boolean getStreamingFacets(Consumer<StreamingFacet> cons) {
    hasStreamingFacets = false;
    facets.forEach( (name, facet) -> {
      if (facet instanceof StreamingFacet) {
        cons.accept((StreamingFacet)facet);
        hasStreamingFacets = true;
      }
    });
    return hasStreamingFacets;
  }

  /**
   * Create the {@link FacetValueQueryExecuter}s for all {@link AbstractSolrQueryFacet}s
   * (e.g. {@link QueryFacet} and {@link RangeFacet}) contained within this grouping.
   * The executers are returned through the given consumer.
   *
   * <p>
   * One {@link FacetValueQueryExecuter} is created for each facet value to be returned for a facet.
   * Since every {@link AbstractSolrQueryFacet} has discrete and user-defined facet values,
   * unlike {@link StreamingFacet}s, a discrete number of {@link FacetValueQueryExecuter}s are created and returned.
   *
   * @param filter representing the overall Solr Query of the request,
   * will be combined with the facet value queries
   * @param queryRequest from the overall search request
   * @param cons where the executers are passed to
   */
  public void getFacetExecuters(Filter filter, SolrQueryRequest queryRequest, Consumer<FacetValueQueryExecuter> cons) {
    facets.forEach( (name, facet) -> {
      if (facet instanceof AbstractSolrQueryFacet) {
        ((AbstractSolrQueryFacet)facet).createFacetValueExecuters(filter, queryRequest, cons);
      }
    });
  }

  /**
   * Add a facet to the grouping. All expressions in this grouping will be computed over the facet.
   *
   * @param facet to compute expressions over
   */
  public void addFacet(AnalyticsFacet facet) {
    facet.setExpressionCalculator(expressionCalculator);
    facet.setReductionCollectionManager(reductionCollectionManager);
    facets.put(facet.getName(), facet);
  }

  /**
   * Import the shard data for this grouping from a bit-stream,
   * exported by the {@link #exportShardData} method in the each of the collection's shards.
   *
   * @param input The bit-stream to import the grouping data from
   * @throws IOException if an exception occurs while reading from the {@link DataInput}
   */
  public void importShardData(DataInput input) throws IOException {
    // This allows mergeData() to import from the same input everytime it is called
    // while the facets are importing.
    reductionCollectionManager.setShardInput(input);

    int sz = input.readInt();
    for (int i = 0; i < sz; ++i) {
      facets.get(input.readUTF()).importShardData(input);
    }
  }

  /**
   * Export the shard data for this grouping through a bit-stream,
   * to be imported by the {@link #importShardData} method in the originating shard.
   *
   * @param output The bit-stream to output the grouping data through
   * @throws IOException if an exception occurs while writing to the {@link DataOutput}
   */
  public void exportShardData(DataOutput output) throws IOException {
    // This allows exportData() to export to the same output everytime it is called
    // while the facets are exporting.
    reductionCollectionManager.setShardOutput(output);

    output.writeInt(facets.size());
    for (Entry<String,AnalyticsFacet> facet : facets.entrySet()) {
      output.writeUTF(facet.getKey());
      facet.getValue().exportShardData(output);
    }
  }

  /**
   * Get the {@link ReductionCollectionManager} that manages the collection of reduction data for the expressions
   * contained within this grouping.
   *
   * @return the grouping's reduction manager
   */
  public ReductionCollectionManager getReductionManager() {
    return reductionCollectionManager;
  }

  /**
   * Create the response for this grouping, a mapping from each of it's facets' names to the facet's response.
   *
   * @return the named list representation of the response
   */
  public Map<String,Object> createResponse() {
    Map<String,Object> response = new HashMap<>();

    // Add the value facet buckets to the output
    facets.forEach( (name, facet) -> response.put(name, facet.createResponse()) );

    return response;
  }

  /**
   * Create the response for this grouping, but in the old style of response.
   * This response has a bucket for the following if they are contained in the grouping:
   * FieldFacets, RangeFacets and QueryFacets.
   * Each facet's name and response are put into the bucket corresponding to its type.
   * <p>
   * Since groupings in the old notation must also return overall results, the overall results are
   * passed in and the values are used to populate the grouping response.
   *
   * @param overallResults of the expressions to add to the grouping response
   * @return the named list representation of the response
   */
  public NamedList<Object> createOldResponse(Map<String,Object> overallResults) {
    NamedList<Object> response = new NamedList<>();

    topLevelExpressions.forEach( expression -> response.add(expression.getName(), overallResults.get(name + expression.getName())));

    NamedList<Object> fieldFacetResults = new NamedList<>();
    NamedList<Object> rangeFacetResults = new NamedList<>();
    NamedList<Object> queryFacetResults = new NamedList<>();
    // Add the field facet buckets to the output
    facets.forEach( (name, facet) -> {
      // The old style of request only accepts field facets
      // So we can assume that all value facets are field facets
      if (facet instanceof ValueFacet) {
        fieldFacetResults.add(name, facet.createOldResponse());
      } else if (facet instanceof RangeFacet) {
        rangeFacetResults.add(name, facet.createOldResponse());
      } else if (facet instanceof QueryFacet) {
        queryFacetResults.add(name, facet.createOldResponse());
      }
    });
    if (fieldFacetResults.size() > 0) {
      response.add(AnalyticsResponseHeadings.FIELD_FACETS, fieldFacetResults);
    }
    if (rangeFacetResults.size() > 0) {
      response.add(AnalyticsResponseHeadings.RANGE_FACETS, rangeFacetResults);
    }
    if (queryFacetResults.size() > 0) {
      response.add(AnalyticsResponseHeadings.QUERY_FACETS, queryFacetResults);
    }
    return response;
  }

  /**
   * Get the name of the grouping.
   *
   * @return the grouping name
   */
  public String getName() {
    return name;
  }
}