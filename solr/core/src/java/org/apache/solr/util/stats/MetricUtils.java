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

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
import org.apache.solr.common.util.NamedList;

/**
 * Metrics specific utility functions.
 */
public class MetricUtils {

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
  static double nsToMs(double ns) {
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
   * @return a {@link NamedList}
   */
  public static NamedList toNamedList(MetricRegistry registry, List<MetricFilter> shouldMatchFilters, MetricFilter mustMatchFilter) {
    NamedList response = new NamedList();
    Map<String, Metric> metrics = registry.getMetrics();
    SortedSet<String> names = registry.getNames();
    names.stream()
        .filter(s -> shouldMatchFilters.stream().anyMatch(metricFilter -> metricFilter.matches(s, metrics.get(s))))
        .filter(s -> mustMatchFilter.matches(s, metrics.get(s)))
        .forEach(n -> {
      Metric metric = metrics.get(n);
      if (metric instanceof Counter) {
        Counter counter = (Counter) metric;
        response.add(n, counterToNamedList(counter));
      } else if (metric instanceof Gauge) {
        Gauge gauge = (Gauge) metric;
        response.add(n, gaugeToNamedList(gauge));
      } else if (metric instanceof Meter) {
        Meter meter = (Meter) metric;
        response.add(n, meterToNamedList(meter));
      } else if (metric instanceof Timer) {
        Timer timer = (Timer) metric;
        response.add(n, timerToNamedList(timer));
      } else if (metric instanceof Histogram) {
        Histogram histogram = (Histogram) metric;
        response.add(n, histogramToNamedList(histogram));
      }
    });
    return response;
  }

  static NamedList histogramToNamedList(Histogram histogram) {
    NamedList response = new NamedList();
    Snapshot snapshot = histogram.getSnapshot();
    response.add("count", histogram.getCount());
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
  static final String P75 = "p75";
  static final String P75_MS = P75 + MS;
  static final String P95 = "p95";
  static final String P95_MS = P95 + MS;
  static final String P99 = "p99";
  static final String P99_MS = P99 + MS;
  static final String P999 = "p999";
  static final String P999_MS = P999 + MS;

  // some snapshots represent time in ns, other snapshots represent raw values (eg. chunk size)
  static void addSnapshot(NamedList response, Snapshot snapshot, boolean ms) {
    response.add((ms ? MIN_MS: MIN), nsToMs(ms, snapshot.getMin()));
    response.add((ms ? MAX_MS: MAX), nsToMs(ms, snapshot.getMax()));
    response.add((ms ? MEAN_MS : MEAN), nsToMs(ms, snapshot.getMean()));
    response.add((ms ? MEDIAN_MS: MEDIAN), nsToMs(ms, snapshot.getMedian()));
    response.add((ms ? STDDEV_MS: STDDEV), nsToMs(ms, snapshot.getStdDev()));
    response.add((ms ? P75_MS: P75), nsToMs(ms, snapshot.get75thPercentile()));
    response.add((ms ? P95_MS: P95), nsToMs(ms, snapshot.get95thPercentile()));
    response.add((ms ? P99_MS: P99), nsToMs(ms, snapshot.get99thPercentile()));
    response.add((ms ? P999_MS: P999), nsToMs(ms, snapshot.get999thPercentile()));
  }

  static NamedList timerToNamedList(Timer timer) {
    NamedList response = new NamedList();
    response.add("count", timer.getCount());
    response.add("meanRate", timer.getMeanRate());
    response.add("1minRate", timer.getOneMinuteRate());
    response.add("5minRate", timer.getFiveMinuteRate());
    response.add("15minRate", timer.getFifteenMinuteRate());
    // time-based values in nanoseconds
    addSnapshot(response, timer.getSnapshot(), true);
    return response;
  }

  static NamedList meterToNamedList(Meter meter) {
    NamedList response = new NamedList();
    response.add("count", meter.getCount());
    response.add("meanRate", meter.getMeanRate());
    response.add("1minRate", meter.getOneMinuteRate());
    response.add("5minRate", meter.getFiveMinuteRate());
    response.add("15minRate", meter.getFifteenMinuteRate());
    return response;
  }

  static NamedList gaugeToNamedList(Gauge gauge) {
    NamedList response = new NamedList();
    response.add("value", gauge.getValue());
    return response;
  }

  static NamedList counterToNamedList(Counter counter) {
    NamedList response = new NamedList();
    response.add("count", counter.getCount());
    return response;
  }

  /**
   * Returns an instrumented wrapper over the given executor service.
   */
  public static ExecutorService instrumentedExecutorService(ExecutorService delegate, MetricRegistry metricRegistry, String scope)  {
    return new InstrumentedExecutorService(delegate, metricRegistry, scope);
  }
}
