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
import org.apache.solr.prometheus.scraper.config.SolrQueryConfig;
import org.apache.solr.prometheus.scraper.config.SolrScraperConfig;
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
  private SolrScraperConfig scraperConfig;

  private List<String> labelNames;
  private List<String> labelValues;

  /**
   * Constructor.
   */
  public SolrScraper(SolrClient solrClient, SolrScraperConfig scraperConfig) {
    this(solrClient, scraperConfig, new ArrayList<>(), new ArrayList<>());
  }

  /**
   * Constructor.
   */
  public SolrScraper(SolrClient solrClient, SolrScraperConfig scraperConfig, List<String> labelNames, List<String> labelValues) {
    super();

    this.solrClient = solrClient;
    this.scraperConfig = scraperConfig;

    this.labelNames = labelNames;
    this.labelValues = labelValues;
  }

  /**
   * Execute collectResponse
   */
  @Override
  public Map<String, Collector.MetricFamilySamples> call() throws Exception {
    return collectResponse(this.solrClient, this.scraperConfig);
  }

  /**
   * Collect facet count.
   */
  public Map<String, Collector.MetricFamilySamples> collectResponse(SolrClient solrClient, SolrScraperConfig scraperConfig) {
    Map<String, Collector.MetricFamilySamples> metricFamilySamplesMap = new LinkedHashMap<>();

    try {
      SolrQueryConfig queryConfig = scraperConfig.getQuery();

      // create Solr request parameters
      ModifiableSolrParams params = new ModifiableSolrParams();
      for (Map<String, String> param : queryConfig.getParams()) {
        for (String name : param.keySet()) {
          Object obj = param.get(name);
          if (obj instanceof Number) {
            params.add(name, obj.toString());
          } else {
            params.add(name, param.get(name));
          }
        }
      }

      // create Solr queryConfig request
      QueryRequest queryRequest = new QueryRequest(params);
      queryRequest.setPath(queryConfig.getPath());

      // invoke Solr
      NamedList<Object> queryResponse = null;
      if (queryConfig.getCore().equals("") && queryConfig.getCollection().equals("")) {
        queryResponse = solrClient.request(queryRequest);
      } else if (!queryConfig.getCore().equals("")) {
        queryResponse = solrClient.request(queryRequest, queryConfig.getCore());
      } else if (!queryConfig.getCollection().equals("")) {
        queryResponse = solrClient.request(queryRequest, queryConfig.getCollection());
      }

      ObjectMapper om = new ObjectMapper();

      JsonNode metricsJson = om.readTree((String) queryResponse.get("response"));

      List<JsonQuery> jqs = new ArrayList<>();
      for (String jsonQuery : scraperConfig.getJsonQueries()) {
        JsonQuery compiledJsonQuery = JsonQuery.compile(jsonQuery);
        jqs.add(compiledJsonQuery);
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

            if (!scraperConfig.getQuery().getCollection().equals("")) {
              labelNames.add("collection");
              labelValues.add(scraperConfig.getQuery().getCollection());
            }

            if (solrClient instanceof HttpSolrClient) {
              labelNames.add("base_url");
              labelValues.add(((HttpSolrClient) solrClient).getBaseURL());
            }

            if (!scraperConfig.getQuery().getCore().equals("")) {
              labelNames.add("core");
              labelValues.add(scraperConfig.getQuery().getCore());
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
              if (labelValues.get(labelNames.indexOf("collection")).equals("-") && labelValues.get(labelNames.indexOf("shard")).equals("-") && labelValues.get(labelNames.indexOf("replica")).equals("-")) {
                labelNames.add("core");
                labelValues.add("-");
              } else {
                StringBuffer sb = new StringBuffer();
                sb.append(labelValues.get(labelNames.indexOf("collection")))
                    .append("_")
                    .append(labelValues.get(labelNames.indexOf("shard")))
                    .append("_")
                    .append(labelValues.get(labelNames.indexOf("replica")));

                labelNames.add("core");
                labelValues.add(sb.toString());
              }
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
          logger.error(e.toString() + " " + q.toString());
          SolrExporter.scrapeErrorTotal.inc();
        }
      }
    } catch (HttpSolrClient.RemoteSolrException | SolrServerException | IOException e) {
      logger.error(e.toString());
    } catch (Exception e) {
      logger.error(e.toString());
    }

    return metricFamilySamplesMap;
  }
}
