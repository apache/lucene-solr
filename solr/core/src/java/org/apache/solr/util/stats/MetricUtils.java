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
package org.apache.solr.util.stats;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.AggregateMetric;

/**
 * Metrics specific utility functions.
 */
public class MetricUtils {

  public static final String METRIC_NAME = "metric";
  public static final String VALUES = "values";

  static final String MS = "_ms";

  static final String MIN = "min";
  static final String MIN_MS = MIN + MS;
  static final String MAX = "max";
  static final String MAX_MS = MAX + MS;
  static final String MEAN = "mean";
  static final String MEAN_MS = MEAN + MS;
  static final String MEDIAN = "median";
  static final String MEDIAN_MS = MEDIAN + MS;
  static final String STDDEV = "stddev";
  static final String STDDEV_MS = STDDEV + MS;
  static final String SUM = "sum";
  static final String P75 = "p75";
  static final String P75_MS = P75 + MS;
  static final String P95 = "p95";
  static final String P95_MS = P95 + MS;
  static final String P99 = "p99";
  static final String P99_MS = P99 + MS;
  static final String P999 = "p999";
  static final String P999_MS = P999 + MS;

  /**
   * Adds metrics from a Timer to a NamedList, using well-known back-compat names.
   * @param lst The NamedList to add the metrics data to
   * @param timer The Timer to extract the metrics from
   */
  public static void addMetrics(NamedList<Object> lst, Timer timer) {
    Snapshot snapshot = timer.getSnapshot();
    lst.add("avgRequestsPerSecond", timer.getMeanRate());
    lst.add("5minRateRequestsPerSecond", timer.getFiveMinuteRate());
    lst.add("15minRateRequestsPerSecond", timer.getFifteenMinuteRate());
    lst.add("avgTimePerRequest", nsToMs(snapshot.getMean()));
    lst.add("medianRequestTime", nsToMs(snapshot.getMedian()));
    lst.add("75thPcRequestTime", nsToMs(snapshot.get75thPercentile()));
    lst.add("95thPcRequestTime", nsToMs(snapshot.get95thPercentile()));
    lst.add("99thPcRequestTime", nsToMs(snapshot.get99thPercentile()));
    lst.add("999thPcRequestTime", nsToMs(snapshot.get999thPercentile()));
  }

  /**
   * Converts a double representing nanoseconds to a double representing milliseconds.
   *
   * @param ns the amount of time in nanoseconds
   * @return the amount of time in milliseconds
   */
  public static double nsToMs(double ns) {
    return ns / TimeUnit.MILLISECONDS.toNanos(1);
  }

  /**
   * Returns a NamedList representation of the given metric registry. Only those metrics
   * are converted to NamedList which match at least one of the given MetricFilter instances.
   *
   * @param registry      the {@link MetricRegistry} to be converted to NamedList
   * @param shouldMatchFilters a list of {@link MetricFilter} instances.
   *                           A metric must match <em>any one</em> of the filters from this list to be
   *                           included in the output
   * @param mustMatchFilter a {@link MetricFilter}.
   *                        A metric <em>must</em> match this filter to be included in the output.
   * @param skipHistograms discard any {@link Histogram}-s and histogram parts of {@link Timer}-s.
   * @param metadata optional metadata. If not null and not empty then this map will be added under a
   *                 {@code _metadata_} key.
   * @return a {@link NamedList}
   */
  public static NamedList toNamedList(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                                      MetricFilter mustMatchFilter, boolean skipHistograms,
                                      boolean skipAggregateValues,
                                      Map<String, Object> metadata) {
    NamedList result = new NamedList();
    toNamedMaps(registry, shouldMatchFilters, mustMatchFilter, skipHistograms, skipAggregateValues, (k, v) -> {
      result.add(k, new NamedList(v));
    });
    if (metadata != null && !metadata.isEmpty()) {
      result.add("_metadata_", new NamedList(metadata));
    }
    return result;
  }

  /**
   * Returns a representation of the given metric registry as a list of {@link SolrInputDocument}-s.
   Only those metrics
   * are converted to NamedList which match at least one of the given MetricFilter instances.
   *
   * @param registry      the {@link MetricRegistry} to be converted to NamedList
   * @param shouldMatchFilters a list of {@link MetricFilter} instances.
   *                           A metric must match <em>any one</em> of the filters from this list to be
   *                           included in the output
   * @param mustMatchFilter a {@link MetricFilter}.
   *                        A metric <em>must</em> match this filter to be included in the output.
   * @param skipHistograms discard any {@link Histogram}-s and histogram parts of {@link Timer}-s.
   * @param metadata optional metadata. If not null and not empty then this map will be added under a
   *                 {@code _metadata_} key.
   * @return a list of {@link SolrInputDocument}-s
   */
  public static List<SolrInputDocument> toSolrInputDocuments(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                                                             MetricFilter mustMatchFilter, boolean skipHistograms,
                                                             boolean skipAggregateValues,
                                                             Map<String, Object> metadata) {
    List<SolrInputDocument> result = new LinkedList<>();
    toSolrInputDocuments(registry, shouldMatchFilters, mustMatchFilter, skipHistograms,
        skipAggregateValues, metadata, doc -> {
      result.add(doc);
    });
    return result;
  }

  public static void toSolrInputDocuments(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                                          MetricFilter mustMatchFilter, boolean skipHistograms,
                                          boolean skipAggregateValues,
                                          Map<String, Object> metadata, Consumer<SolrInputDocument> consumer) {
    boolean addMetadata = metadata != null && !metadata.isEmpty();
    toNamedMaps(registry, shouldMatchFilters, mustMatchFilter, skipHistograms, skipAggregateValues, (k, v) -> {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField(METRIC_NAME, k);
      toSolrInputDocument(null, doc, v);
      if (addMetadata) {
        toSolrInputDocument(null, doc, metadata);
      }
      consumer.accept(doc);
    });
  }

  public static void toSolrInputDocument(String prefix, SolrInputDocument doc, Map<String, Object> map) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() instanceof Map) { // flatten recursively
        toSolrInputDocument(entry.getKey(), doc, (Map<String, Object>)entry.getValue());
      } else {
        String key = prefix != null ? prefix + "." + entry.getKey() : entry.getKey();
        doc.addField(key, entry.getValue());
      }
    }
  }

  public static void toNamedMaps(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                MetricFilter mustMatchFilter, boolean skipHistograms, boolean skipAggregateValues,
                BiConsumer<String, Map<String, Object>> consumer) {
    Map<String, Metric> metrics = registry.getMetrics();
    SortedSet<String> names = registry.getNames();
    names.stream()
        .filter(s -> shouldMatchFilters.stream().anyMatch(metricFilter -> metricFilter.matches(s, metrics.get(s))))
        .filter(s -> mustMatchFilter.matches(s, metrics.get(s)))
        .forEach(n -> {
          Metric metric = metrics.get(n);
          if (metric instanceof Counter) {
            Counter counter = (Counter) metric;
            consumer.accept(n, counterToMap(counter));
          } else if (metric instanceof Gauge) {
            Gauge gauge = (Gauge) metric;
            consumer.accept(n, gaugeToMap(gauge));
          } else if (metric instanceof Meter) {
            Meter meter = (Meter) metric;
            consumer.accept(n, meterToMap(meter));
          } else if (metric instanceof Timer) {
            Timer timer = (Timer) metric;
            consumer.accept(n, timerToMap(timer, skipHistograms));
          } else if (metric instanceof Histogram) {
            if (!skipHistograms) {
              Histogram histogram = (Histogram) metric;
              consumer.accept(n, histogramToMap(histogram));
            }
          } else if (metric instanceof AggregateMetric) {
            consumer.accept(n, aggregateMetricToMap((AggregateMetric)metric, skipAggregateValues));
          }
        });
  }

  static Map<String, Object> aggregateMetricToMap(AggregateMetric metric, boolean skipAggregateValues) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("count", metric.size());
    response.put(MAX, metric.getMax());
    response.put(MIN, metric.getMin());
    response.put(MEAN, metric.getMean());
    response.put(STDDEV, metric.getStdDev());
    response.put(SUM, metric.getSum());
    if (!(metric.isEmpty() || skipAggregateValues)) {
      Map<String, Object> values = new LinkedHashMap<>();
      response.put(VALUES, values);
      metric.getValues().forEach((k, v) -> {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("value", v.value);
        map.put("updateCount", v.updateCount.get());
        values.put(k, map);
      });
    }
    return response;
  }

  static Map<String, Object> histogramToMap(Histogram histogram) {
    Map<String, Object> response = new LinkedHashMap<>();
    Snapshot snapshot = histogram.getSnapshot();
    response.put("count", histogram.getCount());
    // non-time based values
    addSnapshot(response, snapshot, false);
    return response;
  }

  // optionally convert ns to ms
  static double nsToMs(boolean convert, double value) {
    if (convert) {
      return nsToMs(value);
    } else {
      return value;
    }
  }

  // some snapshots represent time in ns, other snapshots represent raw values (eg. chunk size)
  static void addSnapshot(Map<String, Object> response, Snapshot snapshot, boolean ms) {
    response.put((ms ? MIN_MS: MIN), nsToMs(ms, snapshot.getMin()));
    response.put((ms ? MAX_MS: MAX), nsToMs(ms, snapshot.getMax()));
    response.put((ms ? MEAN_MS : MEAN), nsToMs(ms, snapshot.getMean()));
    response.put((ms ? MEDIAN_MS: MEDIAN), nsToMs(ms, snapshot.getMedian()));
    response.put((ms ? STDDEV_MS: STDDEV), nsToMs(ms, snapshot.getStdDev()));
    response.put((ms ? P75_MS: P75), nsToMs(ms, snapshot.get75thPercentile()));
    response.put((ms ? P95_MS: P95), nsToMs(ms, snapshot.get95thPercentile()));
    response.put((ms ? P99_MS: P99), nsToMs(ms, snapshot.get99thPercentile()));
    response.put((ms ? P999_MS: P999), nsToMs(ms, snapshot.get999thPercentile()));
  }

  static Map<String,Object> timerToMap(Timer timer, boolean skipHistograms) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("count", timer.getCount());
    response.put("meanRate", timer.getMeanRate());
    response.put("1minRate", timer.getOneMinuteRate());
    response.put("5minRate", timer.getFiveMinuteRate());
    response.put("15minRate", timer.getFifteenMinuteRate());
    if (!skipHistograms) {
      // time-based values in nanoseconds
      addSnapshot(response, timer.getSnapshot(), true);
    }
    return response;
  }

  static Map<String, Object> meterToMap(Meter meter) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("count", meter.getCount());
    response.put("meanRate", meter.getMeanRate());
    response.put("1minRate", meter.getOneMinuteRate());
    response.put("5minRate", meter.getFiveMinuteRate());
    response.put("15minRate", meter.getFifteenMinuteRate());
    return response;
  }

  static Map<String, Object> gaugeToMap(Gauge gauge) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("value", gauge.getValue());
    return response;
  }

  static Map<String, Object> counterToMap(Counter counter) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("count", counter.getCount());
    return response;
  }

  /**
   * Returns an instrumented wrapper over the given executor service.
   */
  public static ExecutorService instrumentedExecutorService(ExecutorService delegate, MetricRegistry metricRegistry, String scope)  {
    return new InstrumentedExecutorService(delegate, metricRegistry, scope);
  }
}
