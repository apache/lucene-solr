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

import org.apache.solr.analytics.plugin.AnalyticsStatisticsCollector;
import org.apache.solr.analytics.request.AnalyticsStats;
import org.apache.solr.analytics.util.AnalyticsParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

public class AnalyticsComponent extends SearchComponent implements SolrMetricProducer {
  public static final String COMPONENT_NAME = "analytics";
  private final AnalyticsStatisticsCollector analyticsCollector = new AnalyticsStatisticsCollector();;

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(AnalyticsParams.ANALYTICS,false)) {
      rb.setNeedDocSet( true );
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(AnalyticsParams.ANALYTICS,false)) {
      SolrParams params = rb.req.getParams();
      AnalyticsStats s = new AnalyticsStats(rb.req, rb.getResults().docSet, params, analyticsCollector);
      rb.rsp.add( "stats", s.execute() );
    }
  }
  
  /*
  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }
  
  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    // TODO Auto-generated method stub
    super.modifyRequest(rb, who, sreq);
  }
  
  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    // TODO Auto-generated method stub
    super.handleResponses(rb, sreq);
  }
 
  @Override
  public void finishStage(ResponseBuilder rb) {
    // TODO Auto-generated method stub
    super.finishStage(rb);
  }
  */
  
  @Override
  public String getName() {
    return COMPONENT_NAME;
  }
  
  @Override
  public String getDescription() {
    return "Perform analytics";
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String scope) {
    MetricsMap metrics = new MetricsMap((detailed, map) -> map.putAll(analyticsCollector.getStatistics()));
    manager.registerGauge(this, registry, metrics, true, getClass().getSimpleName(), getCategory().toString(), scope);
  }
}
