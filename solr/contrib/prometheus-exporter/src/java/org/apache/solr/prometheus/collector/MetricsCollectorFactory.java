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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.prometheus.exporter.MetricsConfiguration;
import org.apache.solr.prometheus.scraper.SolrScraper;

public class MetricsCollectorFactory {

  private final MetricsConfiguration metricsConfiguration;
  private final ExecutorService executor;
  private final int refreshInSeconds;
  private final SolrScraper solrScraper;

  public MetricsCollectorFactory(
      ExecutorService executor,
      int refreshInSeconds,
      SolrScraper solrScraper,
      MetricsConfiguration metricsConfiguration) {
    this.executor = executor;
    this.refreshInSeconds = refreshInSeconds;
    this.solrScraper = solrScraper;
    this.metricsConfiguration = metricsConfiguration;
  }

  public SchedulerMetricsCollector create() {
    Stream<MetricCollector> pings = metricsConfiguration.getPingConfiguration()
        .stream()
        .map(query -> new PingCollector(solrScraper, query));

    Stream<MetricCollector> metrics = metricsConfiguration.getMetricsConfiguration()
        .stream()
        .map(query -> new MetricsCollector(solrScraper, query));

    Stream<MetricCollector> searches = metricsConfiguration.getSearchConfiguration()
        .stream()
        .map(query -> new SearchCollector(solrScraper, query));

    Stream<MetricCollector> collections = metricsConfiguration.getCollectionsConfiguration()
        .stream()
        .map(query -> new CollectionsCollector(solrScraper, query));

    List<MetricCollector> collectors = Stream.of(pings, metrics, searches, collections)
        .reduce(Stream::concat)
        .orElseGet(Stream::empty)
        .collect(Collectors.toList());

    return new SchedulerMetricsCollector(executor, refreshInSeconds, TimeUnit.SECONDS, collectors);
  }

}
