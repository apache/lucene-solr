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
package org.apache.solr.prometheus.scraper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.prometheus.collector.MetricSamples;
import org.apache.solr.prometheus.exporter.MetricsQuery;

public class SolrStandaloneScraper extends SolrScraper {

  private final HttpSolrClient solrClient;

  public SolrStandaloneScraper(HttpSolrClient solrClient, ExecutorService executor) {
    super(executor);
    this.solrClient = solrClient;
  }

  @Override
  public Map<String, MetricSamples> pingAllCores(MetricsQuery query) throws IOException {
    return sendRequestsInParallel(getCores(), core -> {
      try {
        return request(solrClient, query.withCore(core));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public Map<String, MetricSamples> pingAllCollections(MetricsQuery query) throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, MetricSamples> metricsForAllHosts(MetricsQuery query) throws IOException {
    Map<String, MetricSamples> samples = new HashMap<>();
    samples.put(solrClient.getBaseURL(), request(solrClient, query));
    return samples;
  }

  @Override
  public MetricSamples search(MetricsQuery query) throws IOException {
    return request(solrClient, query);
  }

  @Override
  public MetricSamples collections(MetricsQuery metricsQuery) {
    return new MetricSamples();
  }

  private Set<String> getCores() throws IOException {
    Set<String> cores = new HashSet<>();

    CoreAdminRequest coreAdminRequest = new CoreAdminRequest();
    coreAdminRequest.setAction(CoreAdminParams.CoreAdminAction.STATUS);
    coreAdminRequest.setIndexInfoNeeded(false);

    NamedList<Object> coreAdminResponse;
    try {
      coreAdminResponse = solrClient.request(coreAdminRequest);
    } catch (SolrServerException e) {
      throw new IOException("Failed to get cores", e);
    }

    JsonNode statusJsonNode = OBJECT_MAPPER.readTree((String) coreAdminResponse.get("response")).get("status");

    for (JsonNode jsonNode : statusJsonNode) {
      cores.add(jsonNode.get("name").textValue());
    }

    return cores;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(solrClient);
  }

}
