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
package org.apache.solr.prometheus.collector.config;

import org.apache.solr.prometheus.scraper.config.SolrScraperConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * SolrCollectorConfig
 */
public class SolrCollectorConfig {
  private SolrScraperConfig ping = new SolrScraperConfig();
  private SolrScraperConfig metrics = new SolrScraperConfig();
  private SolrScraperConfig collections = new SolrScraperConfig();
  private List<SolrScraperConfig> queries = new ArrayList<>();

  public SolrScraperConfig getPing() {
    return ping;
  }

  public void setPing(SolrScraperConfig ping) {
    this.ping = ping;
  }

  public SolrScraperConfig getMetrics() {
    return metrics;
  }

  public void setMetrics(SolrScraperConfig metrics) {
    this.metrics = metrics;
  }

  public SolrScraperConfig getCollections() {
    return collections;
  }

  public void setCollections(SolrScraperConfig collections) {
    this.collections = collections;
  }

  public List<SolrScraperConfig> getQueries() {
    return queries;
  }

  public void setQueries(List<SolrScraperConfig> queries) {
    this.queries = queries;
  }
}
