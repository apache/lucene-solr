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

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.prometheus.client.Collector;
import io.prometheus.client.Histogram;
import org.apache.solr.prometheus.exporter.SolrExporter;
import org.apache.solr.prometheus.scraper.Async;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerMetricsCollector implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public interface Observer {
    void metricsUpdated(List<Collector.MetricFamilySamples> samples);
  }

  private final List<MetricCollector> metricCollectors;
  private final int duration;
  private final TimeUnit timeUnit;

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
      1,
      new DefaultSolrThreadFactory("scheduled-metrics-collector"));

  private final Executor executor;

  private final List<Observer> observers = new CopyOnWriteArrayList<>();

  private static final Histogram metricsCollectionTime = Histogram.build()
      .name("solr_exporter_duration_seconds")
      .help("Duration taken to record all metrics")
      .register(SolrExporter.defaultRegistry);

  public SchedulerMetricsCollector(
      Executor executor,
      int duration,
      TimeUnit timeUnit,
      List<MetricCollector> metricCollectors) {
    this.executor = executor;
    this.metricCollectors = metricCollectors;
    this.duration = duration;
    this.timeUnit = timeUnit;
  }

  public void start() {
    scheduler.scheduleWithFixedDelay(this::collectMetrics, 0, duration, timeUnit);
  }

  private void collectMetrics() {

    try (Histogram.Timer timer = metricsCollectionTime.startTimer()) {
      log.info("Beginning metrics collection");

      List<CompletableFuture<MetricSamples>> futures = new ArrayList<>();

      for (MetricCollector metricsCollector : metricCollectors) {
        futures.add(CompletableFuture.supplyAsync(() -> {
          try {
            return metricsCollector.collect();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }, executor));
      }

      try {
        CompletableFuture<List<MetricSamples>> sampleFuture = Async.waitForAllSuccessfulResponses(futures);
        List<MetricSamples> samples = sampleFuture.get();

        MetricSamples metricSamples = new MetricSamples();
        samples.forEach(metricSamples::addAll);

        notifyObservers(metricSamples.asList());

        log.info("Completed metrics collection");
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error while waiting for metric collection to complete", e);
      }
    }

  }

  public void addObserver(Observer observer) {
    this.observers.add(observer);
  }

  public void removeObserver(Observer observer) {
    this.observers.remove(observer);
  }

  private void notifyObservers(List<Collector.MetricFamilySamples> samples) {
    observers.forEach(observer -> observer.metricsUpdated(samples));
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
  }

}
