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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.AggregateMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics specific utility functions.
 */
public class MetricUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String METRIC_NAME = "metric";
  public static final String VALUE = "value";
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
   * @param compact use compact representation for counters and gauges.
   * @return a {@link NamedList}
   */
  public static NamedList toNamedList(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                                      MetricFilter mustMatchFilter, boolean skipHistograms,
                                      boolean skipAggregateValues, boolean compact) {
    NamedList result = new SimpleOrderedMap();
    toMaps(registry, shouldMatchFilters, mustMatchFilter, skipHistograms, skipAggregateValues, compact, false, (k, v) -> {
      result.add(k, v);
    });
    return result;
  }

  /**
   * Provides a representation of the given metric registry as {@link SolrInputDocument}-s.
   Only those metrics
   * are converted which match at least one of the given MetricFilter instances.
   *
   * @param registry      the {@link MetricRegistry} to be converted
   * @param shouldMatchFilters a list of {@link MetricFilter} instances.
   *                           A metric must match <em>any one</em> of the filters from this list to be
   *                           included in the output
   * @param mustMatchFilter a {@link MetricFilter}.
   *                        A metric <em>must</em> match this filter to be included in the output.
   * @param skipHistograms discard any {@link Histogram}-s and histogram parts of {@link Timer}-s.
   * @param skipAggregateValues discard internal values of {@link AggregateMetric}-s.
   * @param compact use compact representation for counters and gauges.
   * @param metadata optional metadata. If not null and not empty then this map will be added under a
   *                 {@code _metadata_} key.
   * @param consumer consumer that accepts produced {@link SolrInputDocument}-s
   */
  public static void toSolrInputDocuments(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                                          MetricFilter mustMatchFilter, boolean skipHistograms,
                                          boolean skipAggregateValues, boolean compact,
                                          Map<String, Object> metadata, Consumer<SolrInputDocument> consumer) {
    boolean addMetadata = metadata != null && !metadata.isEmpty();
    toMaps(registry, shouldMatchFilters, mustMatchFilter, skipHistograms, skipAggregateValues, compact, false, (k, v) -> {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField(METRIC_NAME, k);
      toSolrInputDocument(null, doc, v);
      if (addMetadata) {
        toSolrInputDocument(null, doc, metadata);
      }
      consumer.accept(doc);
    });
  }

  /**
   * Fill in a SolrInputDocument with values from a converted metric, recursively.
   * @param prefix prefix to add to generated field names, or null if none.
   * @param doc document to fill
   * @param o an instance of converted metric, either a Map or a flat Object
   */
  static void toSolrInputDocument(String prefix, SolrInputDocument doc, Object o) {
    if (!(o instanceof Map)) {
      String key = prefix != null ? prefix : VALUE;
      doc.addField(key, o);
      return;
    }
    Map<String, Object> map = (Map<String, Object>)o;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() instanceof Map) { // flatten recursively
        toSolrInputDocument(entry.getKey(), doc, entry.getValue());
      } else {
        String key = prefix != null ? prefix + "." + entry.getKey() : entry.getKey();
        doc.addField(key, entry.getValue());
      }
    }
  }

  /**
   * Convert selected metrics to maps or to flattened objects.
   * @param registry source of metrics
   * @param shouldMatchFilters metrics must match any of these filters
   * @param mustMatchFilter metrics must match this filter
   * @param skipHistograms discard any {@link Histogram}-s and histogram parts of {@link Timer}-s.
   * @param skipAggregateValues discard internal values of {@link AggregateMetric}-s.
   * @param compact use compact representation for counters and gauges.
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param consumer consumer that accepts produced objects
   */
  static void toMaps(MetricRegistry registry, List<MetricFilter> shouldMatchFilters,
                            MetricFilter mustMatchFilter, boolean skipHistograms, boolean skipAggregateValues,
                            boolean compact, boolean simple,
                            BiConsumer<String, Object> consumer) {
    final Map<String, Metric> metrics = registry.getMetrics();
    final SortedSet<String> names = registry.getNames();
    names.stream()
        .filter(s -> shouldMatchFilters.stream().anyMatch(metricFilter -> metricFilter.matches(s, metrics.get(s))))
        .filter(s -> mustMatchFilter.matches(s, metrics.get(s)))
        .forEach(n -> {
          Metric metric = metrics.get(n);
          convertMetric(n, metric, skipHistograms, skipAggregateValues, compact, simple, consumer);
        });
  }

  /**
   * Convert selected metrics from a registry into a map, with metrics in a compact AND simple format.
   * @param registry registry
   * @param names metric names
   * @return map where keys are metric names (if they were present in the registry) and values are
   * converted metrics in simplified format.
   */
  public static Map<String, Object> convertMetrics(MetricRegistry registry, Collection<String> names) {
    final Map<String, Object> metrics = new HashMap<>();
    convertMetrics(registry, names, false, true, true, true, (k, v) -> metrics.put(k, v));
    return metrics;
  }

  /**
   * Convert selected metrics from a registry into maps (when <code>compact==false</code>) or
   * flattened objects.
   * @param registry registry
   * @param names metric names
   * @param skipHistograms discard any {@link Histogram}-s and histogram parts of {@link Timer}-s.
   * @param skipAggregateValues discard internal values of {@link AggregateMetric}-s.
   * @param compact use compact representation for counters and gauges.
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param consumer consumer that accepts produced objects
   */
  public static void convertMetrics(MetricRegistry registry, Collection<String> names,
                                    boolean skipHistograms, boolean skipAggregateValues,
                                    boolean compact, boolean simple,
                                    BiConsumer<String, Object> consumer) {
    final Map<String, Metric> metrics = registry.getMetrics();
    names.stream()
        .forEach(n -> {
          Metric metric = metrics.get(n);
          convertMetric(n, metric, skipHistograms, skipAggregateValues, compact, simple, consumer);
        });
  }

  /**
   * Convert a single instance of metric into a map or flattened object.
   * @param n metric name
   * @param metric metric instance
   * @param skipHistograms discard any {@link Histogram}-s and histogram parts of {@link Timer}-s.
   * @param skipAggregateValues discard internal values of {@link AggregateMetric}-s.
   * @param compact use compact representation for counters and gauges.
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param consumer consumer that accepts produced objects
   */
  static void convertMetric(String n, Metric metric, boolean skipHistograms, boolean skipAggregateValues,
                              boolean compact, boolean simple, BiConsumer<String, Object> consumer) {
    if (metric instanceof Counter) {
      Counter counter = (Counter) metric;
      consumer.accept(n, convertCounter(counter, compact));
    } else if (metric instanceof Gauge) {
      Gauge gauge = (Gauge) metric;
      try {
        convertGauge(n, gauge, simple, compact, consumer);
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
      convertMeter(n, meter, simple, consumer);
    } else if (metric instanceof Timer) {
      Timer timer = (Timer) metric;
      convertTimer(n, timer, skipHistograms, simple, consumer);
    } else if (metric instanceof Histogram) {
      if (!skipHistograms) {
        Histogram histogram = (Histogram) metric;
        convertHistogram(n, histogram, simple, consumer);
      }
    } else if (metric instanceof AggregateMetric) {
      convertAggregateMetric(n, (AggregateMetric)metric, skipAggregateValues, simple, consumer);
    }
  }

  /**
   * Convert an instance of {@link AggregateMetric}.
   * @param name metric name
   * @param metric an instance of {@link AggregateMetric}
   * @param skipAggregateValues discard internal values of {@link AggregateMetric}-s.
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param consumer consumer that accepts produced objects
   */
  static void convertAggregateMetric(String name, AggregateMetric metric,
      boolean skipAggregateValues, boolean simple, BiConsumer<String, Object> consumer) {
    if (simple) {
      consumer.accept(name + "." + MEAN, metric.getMean());
    } else {
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
      consumer.accept(name, response);
    }
  }

  /**
   * Convert an instance of {@link Histogram}. NOTE: it's assumed that histogram contains non-time
   * based values that don't require unit conversion.
   * @param name metric name
   * @param histogram an instance of {@link Histogram}
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param consumer consumer that accepts produced objects
   */
  static void convertHistogram(String name, Histogram histogram,
                                              boolean simple, BiConsumer<String, Object> consumer) {
    Snapshot snapshot = histogram.getSnapshot();
    if (simple) {
      consumer.accept(name + "." + MEAN, snapshot.getMean());
    } else {
      Map<String, Object> response = new LinkedHashMap<>();
      response.put("count", histogram.getCount());
      // non-time based values
      addSnapshot(response, snapshot, false);
      consumer.accept(name, response);
    }
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

  /**
   * Convert a {@link Timer} to a map.
   * @param name metric name
   * @param timer timer instance
   * @param skipHistograms if true then discard the histogram part of the timer.
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param consumer consumer that accepts produced objects
   */
  public static void convertTimer(String name, Timer timer, boolean skipHistograms,
                                                boolean simple, BiConsumer<String, Object> consumer) {
    if (simple) {
      consumer.accept(name + ".meanRate", timer.getMeanRate());
    } else {
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
      consumer.accept(name, response);
    }
  }

  /**
   * Convert a {@link Meter} to a map.
   * @param name metric name
   * @param meter meter instance
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param consumer consumer that accepts produced objects
   */
  static void convertMeter(String name, Meter meter, boolean simple, BiConsumer<String, Object> consumer) {
    if (simple) {
      consumer.accept(name + ".count", meter.getCount());
    } else {
      Map<String, Object> response = new LinkedHashMap<>();
      response.put("count", meter.getCount());
      response.put("meanRate", meter.getMeanRate());
      response.put("1minRate", meter.getOneMinuteRate());
      response.put("5minRate", meter.getFiveMinuteRate());
      response.put("15minRate", meter.getFifteenMinuteRate());
      consumer.accept(name, response);
    }
  }

  /**
   * Convert a {@link Gauge}.
   * @param name metric name
   * @param gauge gauge instance
   * @param simple use simplified representation for complex metrics - instead of a (name, map)
   *             only the selected (name "." key, value) pairs will be produced.
   * @param compact if true then only return {@link Gauge#getValue()}. If false
   *                then return a map with a "value" field.
   * @param consumer consumer that accepts produced objects
   */
  static void convertGauge(String name, Gauge gauge, boolean simple, boolean compact,
                             BiConsumer<String, Object> consumer) {
    if (compact || simple) {
      Object o = gauge.getValue();
      if (simple && (o instanceof Map)) {
        for (Map.Entry<?, ?> entry : ((Map<?, ?>)o).entrySet()) {
          consumer.accept(name + "." + entry.getKey().toString(), entry.getValue());
        }
      } else {
        consumer.accept(name, o);
      }
    } else {
      Map<String, Object> response = new LinkedHashMap<>();
      response.put("value", gauge.getValue());
      consumer.accept(name, response);
    }
  }

  /**
   * Convert a {@link Counter}
   * @param counter counter instance
   * @param compact if true then only return {@link Counter#getCount()}. If false
   *                then return a map with a "count" field.
   * @return map or object
   */
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
  public static ExecutorService instrumentedExecutorService(ExecutorService delegate, SolrInfoBean info, MetricRegistry metricRegistry, String scope)  {
    if (info != null && info.getMetricNames() != null) {
      info.getMetricNames().add(MetricRegistry.name(scope, "submitted"));
      info.getMetricNames().add(MetricRegistry.name(scope, "running"));
      info.getMetricNames().add(MetricRegistry.name(scope, "completed"));
      info.getMetricNames().add(MetricRegistry.name(scope, "duration"));
    }
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
