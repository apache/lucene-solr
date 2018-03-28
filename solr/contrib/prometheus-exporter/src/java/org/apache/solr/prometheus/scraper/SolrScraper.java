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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.prometheus.exporter.SolrExporter;
import io.prometheus.client.Collector;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * SolrScraper
 */
public class SolrScraper implements Callable<Map<String, Collector.MetricFamilySamples>> {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrClient solrClient;
  private LinkedHashMap conf;

  private List<String> labelNames;
  private List<String> labelValues;

  /**
   * Constructor.
   */
  public SolrScraper(SolrClient solrClient, LinkedHashMap conf) {
    super();

    this.solrClient = solrClient;
    this.conf = conf;

    this.labelNames = new ArrayList<>();
    this.labelValues = new ArrayList<>();
  }

  /**
   * Execute collectResponse
   */
  @Override
  public Map<String, Collector.MetricFamilySamples> call() throws Exception {
    return collectResponse(this.solrClient, this.conf);
  }

  /**
   * Collect facet count.
   */
  public Map<String, Collector.MetricFamilySamples> collectResponse(SolrClient solrClient, LinkedHashMap conf) {
    Map<String, Collector.MetricFamilySamples> metricFamilySamplesMap = new LinkedHashMap<>();

    try {
      // create Solr request parameters
      LinkedHashMap confQuery = (LinkedHashMap) conf.get("query");
      LinkedHashMap confParams = (LinkedHashMap) confQuery.get("params");
      String path = (String) confQuery.get("path");
      String core = (String) confQuery.get("core");
      String collection = (String) confQuery.get("collection");
      ArrayList<String> jsonQueries = (ArrayList<String>) conf.get("jsonQueries");

      ModifiableSolrParams params = new ModifiableSolrParams();
      if (confParams != null) {
        for (Object k : confParams.keySet()) {
          String name = (String) k;
          String value = (String) confParams.get(k);
          params.add(name, value);
        }
      }

      // create Solr queryConfig request
      QueryRequest queryRequest = new QueryRequest(params);
      queryRequest.setPath(path);

      // request to Solr
      NamedList<Object> queryResponse = null;
      try {
        if (core == null && collection == null) {
          queryResponse = solrClient.request(queryRequest);
        } else if (core != null) {
          queryResponse = solrClient.request(queryRequest, core);
        } else if (collection != null) {
          queryResponse = solrClient.request(queryRequest, collection);
        }
      } catch (SolrServerException | IOException e) {
        this.logger.error("failed to request: " + queryRequest.getPath() + " " + e.getMessage());
      }

      ObjectMapper om = new ObjectMapper();

      JsonNode metricsJson = om.readTree((String) queryResponse.get("response"));

      List<JsonQuery> jqs = new ArrayList<>();
      if (jsonQueries != null) {
        for (String jsonQuery : jsonQueries) {
          JsonQuery compiledJsonQuery = JsonQuery.compile(jsonQuery);
          jqs.add(compiledJsonQuery);
        }
      }

      for (int i = 0; i < jqs.size(); i++) {
        JsonQuery q = jqs.get(i);
        try {
          List<JsonNode> results = q.apply(metricsJson);
          for (JsonNode result : results) {
            String type = result.get("type").textValue();
            String name = result.get("name").textValue();
            String help = result.get("help").textValue();
            Double value = result.get("value").doubleValue();
            ArrayList<String> labelNames = new ArrayList<>(this.labelNames);
            ArrayList<String> labelValues = new ArrayList<>(this.labelValues);

            if (solrClient instanceof CloudSolrClient) {
              labelNames.add("zk_host");
              labelValues.add(((CloudSolrClient) solrClient).getZkHost());
            }

            if (collection != null) {
              labelNames.add("collection");
              labelValues.add(collection);
            }

            if (solrClient instanceof HttpSolrClient) {
              labelNames.add("base_url");
              labelValues.add(((HttpSolrClient) solrClient).getBaseURL());
            }

            if (core != null) {
              labelNames.add("core");
              labelValues.add(core);
            }

            for(Iterator<JsonNode> ite = result.get("label_names").iterator();ite.hasNext();){
              JsonNode item = ite.next();
              labelNames.add(item.textValue());
            }
            for(Iterator<JsonNode> ite = result.get("label_values").iterator();ite.hasNext();){
              JsonNode item = ite.next();
              labelValues.add(item.textValue());
            }

            if (labelNames.indexOf("core") < 0 && labelNames.indexOf("collection") >= 0 && labelNames.indexOf("shard") >= 0 && labelNames.indexOf("replica") >= 0) {
              StringBuffer sb = new StringBuffer();
              sb.append(labelValues.get(labelNames.indexOf("collection")))
                  .append("_")
                  .append(labelValues.get(labelNames.indexOf("shard")))
                  .append("_")
                  .append(labelValues.get(labelNames.indexOf("replica")));

              labelNames.add("core");
              labelValues.add(sb.toString());
            }

            if (!metricFamilySamplesMap.containsKey(name)) {
              Collector.MetricFamilySamples metricFamilySamples = new Collector.MetricFamilySamples(
                name,
                Collector.Type.valueOf(type),
                help,
                new ArrayList<>()
              );
              metricFamilySamplesMap.put(name, metricFamilySamples);
            }

            Collector.MetricFamilySamples.Sample sample = new Collector.MetricFamilySamples.Sample(name, labelNames, labelValues, value);

            if (!metricFamilySamplesMap.get(name).samples.contains(sample)) {
              metricFamilySamplesMap.get(name).samples.add(sample);
            }
          }
        } catch (JsonQueryException e) {
          this.logger.error(e.toString() + " " + q.toString());
          SolrExporter.scrapeErrorTotal.inc();
        }
      }
    } catch (HttpSolrClient.RemoteSolrException | IOException e) {
      this.logger.error("failed to request: " + e.toString());
    } catch (Exception e) {
      this.logger.error(e.toString());
      e.printStackTrace();
    }

    return metricFamilySamplesMap;
  }
}
