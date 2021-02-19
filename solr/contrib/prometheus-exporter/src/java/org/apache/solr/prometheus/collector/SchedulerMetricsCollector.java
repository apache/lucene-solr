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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.prometheus.client.Collector;
import io.prometheus.client.Histogram;
import org.apache.solr.prometheus.exporter.SolrExporter;
import org.apache.solr.common.util.SolrNamedThreadFactory;
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
      new SolrNamedThreadFactory("scheduled-metrics-collector"));

  private final ExecutorService executor;

  private final List<Observer> observers = new CopyOnWriteArrayList<>();

  private static final Histogram metricsCollectionTime = Histogram.build()
      .name("solr_exporter_duration_seconds")
      .help("Duration taken to record all metrics")
      .register(SolrExporter.defaultRegistry);

  public SchedulerMetricsCollector(
      ExecutorService executor,
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

  private@SuppressWarnings({"try"})
  void collectMetrics() {

    try (Histogram.Timer timer = metricsCollectionTime.startTimer()) {
      log.info("Beginning metrics collection");

      final List<Future<MetricSamples>> futures = executor.invokeAll(
          metricCollectors.stream()
              .map(metricCollector -> (Callable<MetricSamples>) metricCollector::collect)
              .collect(Collectors.toList())
      );
      MetricSamples metricSamples = new MetricSamples();
      for (Future<MetricSamples> future : futures) {
        try {
          metricSamples.addAll(future.get());
        } catch (ExecutionException e) {
          log.error("Error occurred during metrics collection", e.getCause());//nowarn
          // continue any ways; do not fail
        }
      }

      notifyObservers(metricSamples.asList());

      log.info("Completed metrics collection");
    } catch (InterruptedException e) {
      log.warn("Interrupted waiting for metric collection to complete", e);
      Thread.currentThread().interrupt();
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
