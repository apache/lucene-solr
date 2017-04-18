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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.invoke.MethodHandles;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

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
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics specific utility functions.
 */
public class MetricUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String VALUE = "value";

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
   * @param compact use compact representation for counters and gauges.
   * @param metadata optional metadata. If not null and not empty then this map will be added under a
   *                 {@code _metadata_} key.
   * @return a {@link NamedList}
   */
  public static NamedList toNamedList(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                                      MetricFilter mustMatchFilter, boolean skipHistograms,
                                      boolean skipAggregateValues, boolean compact,
                                      Map<String, Object> metadata) {
    NamedList result = new SimpleOrderedMap();
    toMaps(registry, shouldMatchFilters, mustMatchFilter, skipHistograms, skipAggregateValues, compact, (k, v) -> {
      result.add(k, v);
    });
    if (metadata != null && !metadata.isEmpty()) {
      result.add("_metadata_", metadata);
    }
    return result;
  }

  public static void toMaps(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                            MetricFilter mustMatchFilter, boolean skipHistograms, boolean skipAggregateValues,
                            boolean compact,
                            BiConsumer<String, Object> consumer) {
    Map<String, Metric> metrics = registry.getMetrics();
    SortedSet<String> names = registry.getNames();
    names.stream()
        .filter(s -> shouldMatchFilters.stream().anyMatch(metricFilter -> metricFilter.matches(s, metrics.get(s))))
        .filter(s -> mustMatchFilter.matches(s, metrics.get(s)))
        .forEach(n -> {
          Metric metric = metrics.get(n);
          if (metric instanceof Counter) {
            Counter counter = (Counter) metric;
            consumer.accept(n, convertCounter(counter, compact));
          } else if (metric instanceof Gauge) {
            Gauge gauge = (Gauge) metric;
            try {
              consumer.accept(n, convertGauge(gauge, compact));
            } catch (InternalError ie) {
              if (n.startsWith("memory.") && ie.getMessage().contains("Memory Pool not found")) {
                LOG.warn("Error converting gauge '" + n + "', possible JDK bug: SOLR-10362", ie);
                consumer.accept(n, null);
              } else {
                throw ie;
              }
            }
          } else if (metric instanceof Meter) {
            Meter meter = (Meter) metric;
            consumer.accept(n, convertMeter(meter));
          } else if (metric instanceof Timer) {
            Timer timer = (Timer) metric;
            consumer.accept(n, convertTimer(timer, skipHistograms));
          } else if (metric instanceof Histogram) {
            if (!skipHistograms) {
              Histogram histogram = (Histogram) metric;
              consumer.accept(n, convertHistogram(histogram));
            }
          }
        });
  }

  static Map<String, Object> convertHistogram(Histogram histogram) {
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

  static Map<String,Object> convertTimer(Timer timer, boolean skipHistograms) {
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

  static Map<String, Object> convertMeter(Meter meter) {
    Map<String, Object> response = new LinkedHashMap<>();
    response.put("count", meter.getCount());
    response.put("meanRate", meter.getMeanRate());
    response.put("1minRate", meter.getOneMinuteRate());
    response.put("5minRate", meter.getFiveMinuteRate());
    response.put("15minRate", meter.getFifteenMinuteRate());
    return response;
  }

  static Object convertGauge(Gauge gauge, boolean compact) {
    if (compact) {
      return gauge.getValue();
    } else {
      Map<String, Object> response = new LinkedHashMap<>();
      response.put("value", gauge.getValue());
      return response;
    }
  }

  static Object convertCounter(Counter counter, boolean compact) {
    if (compact) {
      return counter.getCount();
    } else {
      Map<String, Object> response = new LinkedHashMap<>();
      response.put("count", counter.getCount());
      return response;
    }
  }

  /**
   * Returns an instrumented wrapper over the given executor service.
   */
  public static ExecutorService instrumentedExecutorService(ExecutorService delegate, MetricRegistry metricRegistry, String scope)  {
    return new InstrumentedExecutorService(delegate, metricRegistry, scope);
  }

  /**
   * Creates a set of metrics (gauges) that correspond to available bean properties for the provided MXBean.
   * @param obj an instance of MXBean
   * @param intf MXBean interface, one of {@link PlatformManagedObject}-s
   * @param consumer consumer for created names and metrics
   * @param <T> formal type
   */
  public static <T extends PlatformManagedObject> void addMXBeanMetrics(T obj, Class<? extends T> intf,
      String prefix, BiConsumer<String, Metric> consumer) {
    if (intf.isInstance(obj)) {
      BeanInfo beanInfo;
      try {
        beanInfo = Introspector.getBeanInfo(intf, intf.getSuperclass(), Introspector.IGNORE_ALL_BEANINFO);
      } catch (IntrospectionException e) {
        LOG.warn("Unable to fetch properties of MXBean " + obj.getClass().getName());
        return;
      }
      for (final PropertyDescriptor desc : beanInfo.getPropertyDescriptors()) {
        final String name = desc.getName();
        // test if it works at all
        try {
          desc.getReadMethod().invoke(obj);
          // worked - consume it
          final Gauge<?> gauge = () -> {
            try {
              return desc.getReadMethod().invoke(obj);
            } catch (InvocationTargetException ite) {
              // ignore (some properties throw UOE)
              return null;
            } catch (IllegalAccessException e) {
              return null;
            }
          };
          String metricName = MetricRegistry.name(prefix, name);
          consumer.accept(metricName, gauge);
        } catch (Exception e) {
          // didn't work, skip it...
        }
      }
    }
  }

  /**
   * These are well-known implementations of {@link java.lang.management.OperatingSystemMXBean}.
   * Some of them provide additional useful properties beyond those declared by the interface.
   */
  public static String[] OS_MXBEAN_CLASSES = new String[] {
      OperatingSystemMXBean.class.getName(),
      "com.sun.management.OperatingSystemMXBean",
      "com.sun.management.UnixOperatingSystemMXBean",
      "com.ibm.lang.management.OperatingSystemMXBean"
  };

  /**
   * Creates a set of metrics (gauges) that correspond to available bean properties for the provided MXBean.
   * @param obj an instance of MXBean
   * @param interfaces interfaces that it may implement. Each interface will be tried in turn, and only
   *                   if it exists and if it contains unique properties then they will be added as metrics.
   * @param prefix optional prefix for metric names
   * @param consumer consumer for created names and metrics
   * @param <T> formal type
   */
  public static <T extends PlatformManagedObject> void addMXBeanMetrics(T obj, String[] interfaces,
      String prefix, BiConsumer<String, Metric> consumer) {
    for (String clazz : interfaces) {
      try {
        final Class<? extends PlatformManagedObject> intf = Class.forName(clazz)
            .asSubclass(PlatformManagedObject.class);
        MetricUtils.addMXBeanMetrics(obj, intf, null, consumer);
      } catch (ClassNotFoundException e) {
        // ignore
      }
    }
  }
}
