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
package org.apache.solr.prometheus.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.prometheus.scraper.SolrScraper;
import io.prometheus.client.Collector;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * SolrCollector
 */
public class SolrCollector extends Collector implements Collector.Describable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrClient solrClient;
  private XmlConfigFile config;
  private int numThreads;
  private ExecutorService executorService;
  private static ObjectMapper om = new ObjectMapper();

  /**
   * Constructor.
   */
  public SolrCollector(SolrClient solrClient, XmlConfigFile config, int numThreads) {
    this.solrClient = solrClient;
    this.config = config;
    this.numThreads = numThreads;
  }

  /**
   * Describe scrape status.
   */
  public List<Collector.MetricFamilySamples> describe() {
    List<MetricFamilySamples> metricFamilies = new ArrayList<>();
    metricFamilies.add(new MetricFamilySamples("solr_exporter_duration_seconds", Type.GAUGE, "Time this Solr scrape took, in seconds.", new ArrayList<>()));
    return metricFamilies;
  }

  /**
   * Collect samples.
   */
  public List<MetricFamilySamples> collect() {
    // start time of scraping.
    long startTime = System.nanoTime();

    this.executorService = ExecutorUtil.newMDCAwareFixedThreadPool(numThreads, new DefaultSolrThreadFactory("solr-exporter"));

    Map<String, MetricFamilySamples> metricFamilySamplesMap = new LinkedHashMap<>();

    List<Future<Map<String, MetricFamilySamples>>> futureList = new ArrayList<>();

    try {
      // Ping
      Node pingNode = this.config.getNode("/config/rules/ping", true);
      if (pingNode != null) {
        NamedList pingNL = DOMUtil.childNodesToNamedList(pingNode);
        List<NamedList> requestsNL = pingNL.getAll("request");

        if (this.solrClient instanceof CloudSolrClient) {
          // in SolrCloud mode
          List<HttpSolrClient> httpSolrClients = new ArrayList<>();
          try {
            httpSolrClients = getHttpSolrClients((CloudSolrClient) this.solrClient);
            for (HttpSolrClient httpSolrClient : httpSolrClients) {
              for (NamedList requestNL : requestsNL) {
                String coreName = (String) ((NamedList) requestNL.get("query")).get("core");
                String collectionName = (String) ((NamedList) requestNL.get("query")).get("collection");
                if (coreName == null && collectionName == null) {
                  try {
                    List<String> cores = getCores(httpSolrClient);
                    for (String core : cores) {
                      LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);
                      LinkedHashMap query = (LinkedHashMap) conf.get("query");
                      if (query != null) {
                        query.put("core", core);
                      }

                      SolrScraper scraper = new SolrScraper(httpSolrClient, conf);
                      Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
                      futureList.add(future);
                    }
                  } catch (SolrServerException | IOException e) {
                    this.log.error("failed to get cores: " + e.getMessage());
                  }
                } else if (coreName != null && collectionName == null) {
                  LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);
                  SolrScraper scraper = new SolrScraper(httpSolrClient, conf);
                  Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
                  futureList.add(future);
                }
              }
            }

            // wait for HttpColeClients
            for (Future<Map<String, MetricFamilySamples>> future : futureList) {
              try {
                Map<String, MetricFamilySamples> m = future.get(60, TimeUnit.SECONDS);
                mergeMetrics(metricFamilySamplesMap, m);
              } catch (InterruptedException | ExecutionException | TimeoutException e) {
                this.log.error(e.getMessage());
              }
            }
          } catch (SolrServerException | IOException e) {
            this.log.error("failed to get HttpSolrClients: " + e.getMessage());
          } finally {
            for (HttpSolrClient httpSolrClient : httpSolrClients) {
              try {
                httpSolrClient.close();
              } catch (IOException e) {
                this.log.error("failed to close HttpSolrClient: " + e.getMessage());
              }
            }
          }

          // collection
          for (NamedList requestNL : requestsNL) {
            String coreName = (String) ((NamedList) requestNL.get("query")).get("core");
            String collectionName = (String) ((NamedList) requestNL.get("query")).get("collection");
            if (coreName == null && collectionName == null) {
              try {
                List<String> collections = getCollections((CloudSolrClient) this.solrClient);
                for (String collection : collections) {
                  LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);
                  LinkedHashMap query = (LinkedHashMap) conf.get("query");
                  if (query != null) {
                    query.put("collection", collection);
                  }

                  SolrScraper scraper = new SolrScraper(this.solrClient, conf);
                  Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
                  futureList.add(future);
                }
              } catch (SolrServerException | IOException e) {
                this.log.error("failed to get cores: " + e.getMessage());
              }
            } else if (coreName == null && collectionName != null) {
              LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);

              SolrScraper scraper = new SolrScraper(this.solrClient, conf);
              Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
              futureList.add(future);
            }
          }
        } else {
          // in Standalone mode
          for (NamedList requestNL : requestsNL) {
            String coreName = (String) ((NamedList) requestNL.get("query")).get("core");
            if (coreName == null) {
              try {
                List<String> cores = getCores((HttpSolrClient) this.solrClient);
                for (String core : cores) {
                  LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);
                  LinkedHashMap query = (LinkedHashMap) conf.get("query");
                  if (query != null) {
                    query.put("core", core);
                  }

                  SolrScraper scraper = new SolrScraper(this.solrClient, conf);
                  Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
                  futureList.add(future);
                }
              } catch (SolrServerException | IOException e) {
                this.log.error("failed to get cores: " + e.getMessage());
              }
            } else {
              LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);

              SolrScraper scraper = new SolrScraper(this.solrClient, conf);
              Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
              futureList.add(future);
            }
          }
        }
      }

      // Metrics
      Node metricsNode = this.config.getNode("/config/rules/metrics", false);
      if (metricsNode != null) {
        NamedList metricsNL = DOMUtil.childNodesToNamedList(metricsNode);
        List<NamedList> requestsNL = metricsNL.getAll("request");

        if (this.solrClient instanceof CloudSolrClient) {
          // in SolrCloud mode
          List<HttpSolrClient> httpSolrClients = new ArrayList<>();
          try {
            httpSolrClients = getHttpSolrClients((CloudSolrClient) this.solrClient);
            for (HttpSolrClient httpSolrClient : httpSolrClients) {
              for (NamedList requestNL : requestsNL) {
                LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);

                SolrScraper scraper = new SolrScraper(httpSolrClient, conf);
                Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
                futureList.add(future);
              }
            }

            // wait for HttpColeClients
            for (Future<Map<String, MetricFamilySamples>> future : futureList) {
              try {
                Map<String, MetricFamilySamples> m = future.get(60, TimeUnit.SECONDS);
                mergeMetrics(metricFamilySamplesMap, m);
              } catch (InterruptedException | ExecutionException | TimeoutException e) {
                this.log.error(e.getMessage());
              }
            }
          } catch (SolrServerException | IOException e) {
            this.log.error(e.getMessage());
          } finally {
            for (HttpSolrClient httpSolrClient : httpSolrClients) {
              try {
                httpSolrClient.close();
              } catch (IOException e) {
                this.log.error(e.getMessage());
              }
            }
          }
        } else {
          // in Standalone mode
          for (NamedList requestNL : requestsNL) {
            LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);

            SolrScraper scraper = new SolrScraper(this.solrClient, conf);
            Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
            futureList.add(future);
          }
        }
      }

      // Collections
      Node collectionsNode = this.config.getNode("/config/rules/collections", false);
      if (collectionsNode != null && this.solrClient instanceof CloudSolrClient) {
        NamedList collectionsNL = DOMUtil.childNodesToNamedList(collectionsNode);
        List<NamedList> requestsNL = collectionsNL.getAll("request");

        for (NamedList requestNL : requestsNL) {
          LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);

          SolrScraper scraper = new SolrScraper(this.solrClient, conf);
          Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
          futureList.add(future);
        }
      }

      // Search
      Node searchNode = this.config.getNode("/config/rules/search", false);
      if (searchNode != null) {
        NamedList searchNL = DOMUtil.childNodesToNamedList(searchNode);
        List<NamedList> requestsNL = searchNL.getAll("request");

        for (NamedList requestNL : requestsNL) {
          LinkedHashMap conf = (LinkedHashMap) requestNL.asMap(10);

          SolrScraper scraper = new SolrScraper(this.solrClient, conf);
          Future<Map<String, MetricFamilySamples>> future = this.executorService.submit(scraper);
          futureList.add(future);
        }
      }

      // get future
      for (Future<Map<String, MetricFamilySamples>> future : futureList) {
        try {
          Map<String, MetricFamilySamples> m = future.get(60, TimeUnit.SECONDS);
          mergeMetrics(metricFamilySamplesMap, m);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          this.log.error(e.getMessage());
        }
      }
    } catch (Exception e) {
      this.log.error(e.getMessage());
      e.printStackTrace();
    }

    // return value
    List<MetricFamilySamples> metricFamiliesSamplesList = new ArrayList<>();

    // add solr metrics
    for (String gaugeMetricName : metricFamilySamplesMap.keySet()) {
      MetricFamilySamples metricFamilySamples = metricFamilySamplesMap.get(gaugeMetricName);
      if (metricFamilySamples.samples.size() > 0) {
        metricFamiliesSamplesList.add(metricFamilySamples);
      }
    }

    // add scrape duration metric
    List<MetricFamilySamples.Sample> durationSample = new ArrayList<>();
    durationSample.add(new MetricFamilySamples.Sample("solr_exporter_duration_seconds", new ArrayList<>(), new ArrayList<>(), (System.nanoTime() - startTime) / 1.0E9));
    metricFamiliesSamplesList.add(new MetricFamilySamples("solr_exporter_duration_seconds", Type.GAUGE, "Time this Solr exporter took, in seconds.", durationSample));

    this.executorService.shutdown();

    return metricFamiliesSamplesList;
  }

  /**
   * Merge metrics.
   */
  private Map<String, MetricFamilySamples> mergeMetrics(Map<String, MetricFamilySamples> metrics1, Map<String, MetricFamilySamples> metrics2) {
    // marge MetricFamilySamples
    for (String k : metrics2.keySet()) {
      if (metrics1.containsKey(k)) {
        for (MetricFamilySamples.Sample sample : metrics2.get(k).samples) {
          if (!metrics1.get(k).samples.contains(sample)) {
            metrics1.get(k).samples.add(sample);
          }
        }
      } else {
        metrics1.put(k, metrics2.get(k));
      }
    }

    return metrics1;
  }


  /**
   * Get target cores via CoreAdminAPI.
   */
  public static List<String> getCores(HttpSolrClient httpSolrClient) throws SolrServerException, IOException {
    List<String> cores = new ArrayList<>();

    NoOpResponseParser responseParser = new NoOpResponseParser();
    responseParser.setWriterType("json");

    httpSolrClient.setParser(responseParser);

    CoreAdminRequest coreAdminRequest = new CoreAdminRequest();
    coreAdminRequest.setAction(CoreAdminParams.CoreAdminAction.STATUS);
    coreAdminRequest.setIndexInfoNeeded(false);

    NamedList<Object> coreAdminResponse = httpSolrClient.request(coreAdminRequest);

    JsonNode statusJsonNode = om.readTree((String) coreAdminResponse.get("response")).get("status");

    for (Iterator<JsonNode> i = statusJsonNode.iterator(); i.hasNext(); ) {
      String core = i.next().get("name").textValue();
      if (!cores.contains(core)) {
        cores.add(core);
      }
    }

    return cores;
  }

  /**
   * Get target cores via CollectionsAPI.
   */
  public static List<String> getCollections(CloudSolrClient cloudSolrClient) throws SolrServerException, IOException {
    List<String> collections = new ArrayList<>();

    NoOpResponseParser responseParser = new NoOpResponseParser();
    responseParser.setWriterType("json");

    cloudSolrClient.setParser(responseParser);

    CollectionAdminRequest collectionAdminRequest = new CollectionAdminRequest.List();

    NamedList<Object> collectionAdminResponse = cloudSolrClient.request(collectionAdminRequest);

    JsonNode collectionsJsonNode = om.readTree((String) collectionAdminResponse.get("response")).get("collections");

    for (Iterator<JsonNode> i = collectionsJsonNode.iterator(); i.hasNext(); ) {
      String collection = i.next().textValue();
      if (!collections.contains(collection)) {
        collections.add(collection);
      }
    }

    return collections;
  }

  /**
   * Get base urls via CollectionsAPI.
   */
  private List<String> getBaseUrls(CloudSolrClient cloudSolrClient) throws SolrServerException, IOException {
    List<String> baseUrls = new ArrayList<>();

    NoOpResponseParser responseParser = new NoOpResponseParser();
    responseParser.setWriterType("json");

    cloudSolrClient.setParser(responseParser);

    CollectionAdminRequest collectionAdminRequest = new CollectionAdminRequest.ClusterStatus();

    NamedList<Object> collectionAdminResponse = cloudSolrClient.request(collectionAdminRequest);

    List<JsonNode> baseUrlJsonNode = om.readTree((String) collectionAdminResponse.get("response")).findValues("base_url");

    for (Iterator<JsonNode> i = baseUrlJsonNode.iterator(); i.hasNext(); ) {
      String baseUrl = i.next().textValue();
      if (!baseUrls.contains(baseUrl)) {
        baseUrls.add(baseUrl);
      }
    }

    return baseUrls;
  }

  /**
   * Get HTTP Solr Clients
   */
  private List<HttpSolrClient> getHttpSolrClients(CloudSolrClient cloudSolrClient) throws SolrServerException, IOException {
    List<HttpSolrClient> solrClients = new ArrayList<>();

    for (String baseUrl : getBaseUrls(cloudSolrClient)) {
      NoOpResponseParser responseParser = new NoOpResponseParser();
      responseParser.setWriterType("json");

      HttpSolrClient.Builder builder = new HttpSolrClient.Builder();
      builder.withBaseSolrUrl(baseUrl);

      HttpSolrClient httpSolrClient = builder.build();
      httpSolrClient.setParser(responseParser);

      solrClients.add(httpSolrClient);
    }

    return solrClients;
  }
}
