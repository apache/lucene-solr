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
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
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
   * Adds metrics from a Timer to a NamedList, using well-known names.
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
   * Returns a NamedList respresentation of the given metric registry. Only those metrics
   * are converted to NamedList which match at least one of the given MetricFilter instances.
   *
   * @param registry      the {@link MetricRegistry} to be converted to NamedList
   * @param metricFilters a list of {@link MetricFilter} instances
   * @return a {@link NamedList}
   */
  public static NamedList toNamedList(MetricRegistry registry, List<MetricFilter> metricFilters) {
    NamedList response = new NamedList();
    Map<String, Metric> metrics = registry.getMetrics();
    SortedSet<String> names = registry.getNames();
    names.stream().filter(s -> metricFilters.stream().anyMatch(metricFilter -> metricFilter.matches(s, metrics.get(s)))).forEach(n -> {
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
    response.add("requests", histogram.getCount());
    response.add("minTime", nsToMs(snapshot.getMin()));
    response.add("maxTime", nsToMs(snapshot.getMax()));
    response.add("avgTimePerRequest", nsToMs(snapshot.getMean()));
    response.add("medianRequestTime", nsToMs(snapshot.getMedian()));
    response.add("75thPcRequestTime", nsToMs(snapshot.get75thPercentile()));
    response.add("95thPcRequestTime", nsToMs(snapshot.get95thPercentile()));
    response.add("99thPcRequestTime", nsToMs(snapshot.get99thPercentile()));
    response.add("999thPcRequestTime", nsToMs(snapshot.get999thPercentile()));
    return response;
  }

  static NamedList timerToNamedList(Timer timer) {
    NamedList response = new NamedList();
    addMetrics(response, timer);
    return response;
  }

  static NamedList meterToNamedList(Meter meter) {
    NamedList response = new NamedList();
    response.add("requests", meter.getCount());
    response.add("avgRequestsPerSecond", meter.getMeanRate());
    response.add("1minRateRequestsPerSecond", meter.getOneMinuteRate());
    response.add("5minRateRequestsPerSecond", meter.getFiveMinuteRate());
    response.add("15minRateRequestsPerSecond", meter.getFifteenMinuteRate());
    return response;
  }

  static NamedList gaugeToNamedList(Gauge gauge) {
    NamedList response = new NamedList();
    response.add("value", gauge.getValue());
    return response;
  }

  static NamedList counterToNamedList(Counter counter) {
    NamedList response = new NamedList();
    response.add("requests", counter.getCount());
    return response;
  }
}
