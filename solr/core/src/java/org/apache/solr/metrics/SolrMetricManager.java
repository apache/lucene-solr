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
package org.apache.solr.metrics;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.MetricsConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class maintains a repository of named {@link MetricRegistry} instances, and provides several
 * helper methods for managing various aspects of metrics reporting:
 * <ul>
 * <li>registry creation, clearing and removal,</li>
 * <li>creation of most common metric implementations,</li>
 * <li>management of {@link SolrMetricReporter}-s specific to a named registry.</li>
 * </ul>
 * {@link MetricRegistry} instances are automatically created when first referenced by name. Similarly,
 * instances of {@link Metric} implementations, such as {@link Meter}, {@link Counter}, {@link Timer} and
 * {@link Histogram} are automatically created and registered under hierarchical names, in a specified
 * registry, when {@link #meter(SolrInfoBean, String, String, String...)} and other similar methods are called.
 * <p>This class enforces a common prefix ({@link #REGISTRY_NAME_PREFIX}) in all registry
 * names.</p>
 * <p>Solr uses several different registries for collecting metrics belonging to different groups, using
 * {@link org.apache.solr.core.SolrInfoBean.Group} as the main name of the registry (plus the
 * above-mentioned prefix). Instances of {@link SolrMetricManager} are created for each {@link org.apache.solr.core.CoreContainer},
 * and most registries are local to each instance, with the exception of two global registries:
 * <code>solr.jetty</code> and <code>solr.jvm</code>, which are shared between all {@link org.apache.solr.core.CoreContainer}-s</p>
 */
public class SolrMetricManager {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Common prefix for all registry names that Solr uses.
   */
  public static final String REGISTRY_NAME_PREFIX = "solr.";

  /**
   * Registry name for Jetty-specific metrics. This name is also subject to overrides controlled by
   * system properties. This registry is shared between instances of {@link SolrMetricManager}.
   */
  public static final String JETTY_REGISTRY = REGISTRY_NAME_PREFIX + SolrInfoBean.Group.jetty.toString();

  /**
   * Registry name for JVM-specific metrics. This name is also subject to overrides controlled by
   * system properties. This registry is shared between instances of {@link SolrMetricManager}.
   */
  public static final String JVM_REGISTRY = REGISTRY_NAME_PREFIX + SolrInfoBean.Group.jvm.toString();

  private final ConcurrentMap<String, MetricRegistry> registries = new ConcurrentHashMap<>();

  private final Map<String, Map<String, SolrMetricReporter>> reporters = new HashMap<>();

  private final Lock reportersLock = new ReentrantLock();
  private final Lock swapLock = new ReentrantLock();

  public static final int DEFAULT_CLOUD_REPORTER_PERIOD = 60;

  private final MetricsConfig metricsConfig;
  private final MetricRegistry.MetricSupplier<Counter> counterSupplier;
  private final MetricRegistry.MetricSupplier<Meter> meterSupplier;
  private final MetricRegistry.MetricSupplier<Timer> timerSupplier;
  private final MetricRegistry.MetricSupplier<Histogram> histogramSupplier;

  public SolrMetricManager() {
    metricsConfig = new MetricsConfig.MetricsConfigBuilder().build();
    counterSupplier = MetricSuppliers.counterSupplier(null, null);
    meterSupplier = MetricSuppliers.meterSupplier(null, null);
    timerSupplier = MetricSuppliers.timerSupplier(null, null);
    histogramSupplier = MetricSuppliers.histogramSupplier(null, null);
  }

  public SolrMetricManager(SolrResourceLoader loader, MetricsConfig metricsConfig) {
    this.metricsConfig = metricsConfig;
    counterSupplier = MetricSuppliers.counterSupplier(loader, metricsConfig.getCounterSupplier());
    meterSupplier = MetricSuppliers.meterSupplier(loader, metricsConfig.getMeterSupplier());
    timerSupplier = MetricSuppliers.timerSupplier(loader, metricsConfig.getTimerSupplier());
    histogramSupplier = MetricSuppliers.histogramSupplier(loader, metricsConfig.getHistogramSupplier());
  }

  // for unit tests
  public MetricRegistry.MetricSupplier<Counter> getCounterSupplier() {
    return counterSupplier;
  }

  public MetricRegistry.MetricSupplier<Meter> getMeterSupplier() {
    return meterSupplier;
  }

  public MetricRegistry.MetricSupplier<Timer> getTimerSupplier() {
    return timerSupplier;
  }

  public MetricRegistry.MetricSupplier<Histogram> getHistogramSupplier() {
    return histogramSupplier;
  }

  /**
   * Return an object used for representing a null (missing) numeric value.
   */
  public Object nullNumber() {
    return metricsConfig.getNullNumber();
  }

  /**
   * Return an object used for representing a "Not A Number" (NaN) value.
   */
  public Object notANumber() {
    return metricsConfig.getNotANumber();
  }

  /**
   * Return an object used for representing a null (missing) string value.
   */
  public Object nullString() {
    return metricsConfig.getNullString();
  }

  /**
   * Return an object used for representing a null (missing) object value.
   */
  public Object nullObject() {
    return metricsConfig.getNullObject();
  }


  /**
   * An implementation of {@link MetricFilter} that selects metrics
   * with names that start with one of prefixes.
   */
  public static class PrefixFilter implements MetricFilter {
    private final Set<String> prefixes = new HashSet<>();
    private final Set<String> matched = new HashSet<>();
    private boolean allMatch = false;

    /**
     * Create a filter that uses the provided prefixes.
     *
     * @param prefixes prefixes to use, must not be null. If empty then any
     *                 name will match, if not empty then match on any prefix will
     *                 succeed (logical OR).
     */
    public PrefixFilter(String... prefixes) {
      Objects.requireNonNull(prefixes);
      if (prefixes.length > 0) {
        this.prefixes.addAll(Arrays.asList(prefixes));
      }
      if (this.prefixes.isEmpty()) {
        allMatch = true;
      }
    }

    public PrefixFilter(Collection<String> prefixes) {
      Objects.requireNonNull(prefixes);
      this.prefixes.addAll(prefixes);
      if (this.prefixes.isEmpty()) {
        allMatch = true;
      }
    }

    @Override
    public boolean matches(String name, Metric metric) {
      if (allMatch) {
        matched.add(name);
        return true;
      }
      for (String prefix : prefixes) {
        if (name.startsWith(prefix)) {
          matched.add(name);
          return true;
        }
      }
      return false;
    }

    /**
     * Return the set of names that matched this filter.
     *
     * @return matching names
     */
    public Set<String> getMatched() {
      return Collections.unmodifiableSet(matched);
    }

    /**
     * Clear the set of names that matched.
     */
    public void reset() {
      matched.clear();
    }

    @Override
    public String toString() {
      return "PrefixFilter{" +
          "prefixes=" + prefixes +
          '}';
    }
  }

  /**
   * An implementation of {@link MetricFilter} that selects metrics
   * with names that match regular expression patterns.
   */
  public static class RegexFilter implements MetricFilter {
    private final Set<Pattern> compiledPatterns = new HashSet<>();
    private final Set<String> matched = new HashSet<>();
    private boolean allMatch = false;

    /**
     * Create a filter that uses the provided prefix.
     *
     * @param patterns regex patterns to use, must not be null. If empty then any
     *                 name will match, if not empty then match on any pattern will
     *                 succeed (logical OR).
     */
    public RegexFilter(String... patterns) throws PatternSyntaxException {
      this(patterns != null ? Arrays.asList(patterns) : Collections.emptyList());
    }

    public RegexFilter(Collection<String> patterns) throws PatternSyntaxException {
      Objects.requireNonNull(patterns);
      if (patterns.isEmpty()) {
        allMatch = true;
        return;
      }
      patterns.forEach(p -> {
        Pattern pattern = Pattern.compile(p);
        compiledPatterns.add(pattern);
      });
      if (patterns.isEmpty()) {
        allMatch = true;
      }
    }

    @Override
    public boolean matches(String name, Metric metric) {
      if (allMatch) {
        matched.add(name);
        return true;
      }
      for (Pattern p : compiledPatterns) {
        if (p.matcher(name).matches()) {
          matched.add(name);
          return true;
        }
      }
      return false;
    }

    /**
     * Return the set of names that matched this filter.
     *
     * @return matching names
     */
    public Set<String> getMatched() {
      return Collections.unmodifiableSet(matched);
    }

    /**
     * Clear the set of names that matched.
     */
    public void reset() {
      matched.clear();
    }

    @Override
    public String toString() {
      return "RegexFilter{" +
          "compiledPatterns=" + compiledPatterns +
          '}';
    }
  }

  /**
   * An implementation of {@link MetricFilter} that selects metrics
   * that match any filter in a list of filters.
   */
  public static class OrFilter implements MetricFilter {
    List<MetricFilter> filters = new ArrayList<>();

    public OrFilter(Collection<MetricFilter> filters) {
      if (filters != null) {
        this.filters.addAll(filters);
      }
    }

    public OrFilter(MetricFilter... filters) {
      if (filters != null) {
        for (MetricFilter filter : filters) {
          if (filter != null) {
            this.filters.add(filter);
          }
        }
      }
    }

    @Override
    public boolean matches(String s, Metric metric) {
      for (MetricFilter filter : filters) {
        if (filter.matches(s, metric)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * An implementation of {@link MetricFilter} that selects metrics
   * that match all filters in a list of filters.
   */
  public static class AndFilter implements MetricFilter {
    List<MetricFilter> filters = new ArrayList<>();

    public AndFilter(Collection<MetricFilter> filters) {
      if (filters != null) {
        this.filters.addAll(filters);
      }
    }

    public AndFilter(MetricFilter... filters) {
      if (filters != null) {
        for (MetricFilter filter : filters) {
          if (filter != null) {
            this.filters.add(filter);
          }
        }
      }
    }

    @Override
    public boolean matches(String s, Metric metric) {
      for (MetricFilter filter : filters) {
        if (!filter.matches(s, metric)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Return a set of existing registry names.
   */
  public Set<String> registryNames() {
    Set<String> set = new HashSet<>();
    set.addAll(registries.keySet());
    set.addAll(SharedMetricRegistries.names());
    return set;
  }

  /**
   * Check whether a registry with a given name already exists.
   *
   * @param name registry name
   * @return true if this name points to a registry that already exists, false otherwise
   */
  public boolean hasRegistry(String name) {
    Set<String> names = registryNames();
    name = enforcePrefix(name);
    return names.contains(name);
  }

  /**
   * Return set of existing registry names that match a regex pattern
   *
   * @param patterns regex patterns. NOTE: users need to make sure that patterns that
   *                 don't start with a wildcard use the full registry name starting with
   *                 {@link #REGISTRY_NAME_PREFIX}
   * @return set of existing registry names where at least one pattern matched.
   */
  public Set<String> registryNames(String... patterns) throws PatternSyntaxException {
    if (patterns == null || patterns.length == 0) {
      return registryNames();
    }
    List<Pattern> compiled = new ArrayList<>();
    for (String pattern : patterns) {
      compiled.add(Pattern.compile(pattern));
    }
    return registryNames(compiled.toArray(new Pattern[compiled.size()]));
  }

  public Set<String> registryNames(Pattern... patterns) {
    Set<String> allNames = registryNames();
    if (patterns == null || patterns.length == 0) {
      return allNames;
    }
    return allNames.stream().filter(s -> {
      for (Pattern p : patterns) {
        if (p.matcher(s).matches()) {
          return true;
        }
      }
      return false;
    }).collect(Collectors.toSet());
  }

  /**
   * Check for predefined shared registry names. This compares the input name
   * with normalized names of predefined shared registries -
   * {@link #JVM_REGISTRY} and {@link #JETTY_REGISTRY}.
   *
   * @param registry already normalized name
   * @return true if the name matches one of shared registries
   */
  private static boolean isSharedRegistry(String registry) {
    return JETTY_REGISTRY.equals(registry) || JVM_REGISTRY.equals(registry);
  }

  /**
   * Get (or create if not present) a named registry
   *
   * @param registry name of the registry
   * @return existing or newly created registry
   */
  public MetricRegistry registry(String registry) {
    registry = enforcePrefix(registry);
    if (isSharedRegistry(registry)) {
      return SharedMetricRegistries.getOrCreate(registry);
    } else {
      swapLock.lock();
      try {
        return getOrCreateRegistry(registries, registry);
      } finally {
        swapLock.unlock();
      }
    }
  }

  private static MetricRegistry getOrCreateRegistry(ConcurrentMap<String, MetricRegistry> map, String registry) {
    final MetricRegistry existing = map.get(registry);
    if (existing == null) {
      final MetricRegistry created = new MetricRegistry();
      final MetricRegistry raced = map.putIfAbsent(registry, created);
      if (raced == null) {
        return created;
      } else {
        return raced;
      }
    } else {
      return existing;
    }
  }

  /**
   * Remove a named registry.
   *
   * @param registry name of the registry to remove
   */
  public void removeRegistry(String registry) {
    // close any reporters for this registry first
    closeReporters(registry, null);
    // make sure we use a name with prefix
    registry = enforcePrefix(registry);
    if (isSharedRegistry(registry)) {
      SharedMetricRegistries.remove(registry);
    } else {
      swapLock.lock();
      try {
        registries.remove(registry);
      } finally {
        swapLock.unlock();
      }
    }
  }

  /**
   * Swap registries. This is useful eg. during
   * {@link org.apache.solr.core.SolrCore} rename or swap operations. NOTE:
   * this operation is not supported for shared registries.
   *
   * @param registry1 source registry
   * @param registry2 target registry. Note: when used after core rename the target registry doesn't
   *                  exist, so the swap operation will only rename the existing registry without creating
   *                  an empty one under the previous name.
   */
  public void swapRegistries(String registry1, String registry2) {
    registry1 = enforcePrefix(registry1);
    registry2 = enforcePrefix(registry2);
    if (isSharedRegistry(registry1) || isSharedRegistry(registry2)) {
      throw new UnsupportedOperationException("Cannot swap shared registry: " + registry1 + ", " + registry2);
    }
    swapLock.lock();
    try {
      MetricRegistry from = registries.get(registry1);
      MetricRegistry to = registries.get(registry2);
      if (from == to) {
        return;
      }
      MetricRegistry reg1 = registries.remove(registry1);
      MetricRegistry reg2 = registries.remove(registry2);
      if (reg2 != null) {
        registries.put(registry1, reg2);
      }
      if (reg1 != null) {
        registries.put(registry2, reg1);
      }
    } finally {
      swapLock.unlock();
    }
  }

  /**
   * Potential conflict resolution strategies when attempting to register a new metric that already exists
   */
  public enum ResolutionStrategy {
    /**
     * The existing metric will be kept and the new metric will be ignored
     */
    IGNORE,
    /**
     * The existing metric will be removed and replaced with the new metric
     */
    REPLACE,
    /**
     * An exception will be thrown. This is the default implementation behavior.
     */
    ERROR
  }

  /**
   * Register all metrics in the provided {@link MetricSet}, optionally skipping those that
   * already exist.
   *
   * @param registry   registry name
   * @param metrics    metric set to register
   * @param strategy   the conflict resolution strategy to use if the named metric already exists.
   * @param metricPath (optional) additional top-most metric name path elements
   * @throws Exception if a metric with this name already exists.
   */
  public void registerAll(String registry, MetricSet metrics, ResolutionStrategy strategy, String... metricPath) throws Exception {
    MetricRegistry metricRegistry = registry(registry);
    synchronized (metricRegistry) {
      Map<String, Metric> existingMetrics = metricRegistry.getMetrics();
      for (Map.Entry<String, Metric> entry : metrics.getMetrics().entrySet()) {
        String fullName = mkName(entry.getKey(), metricPath);
        if (existingMetrics.containsKey(fullName)) {
          if (strategy == ResolutionStrategy.REPLACE) {
            metricRegistry.remove(fullName);
          } else if (strategy == ResolutionStrategy.IGNORE) {
            continue;
          } // strategy == ERROR will fail when we try to register later
        }
        metricRegistry.register(fullName, entry.getValue());
      }
    }
  }

  /**
   * Remove all metrics from a specified registry.
   *
   * @param registry registry name
   */
  public void clearRegistry(String registry) {
    registry(registry).removeMatching(MetricFilter.ALL);
  }

  /**
   * Remove some metrics from a named registry
   *
   * @param registry   registry name
   * @param metricPath (optional) top-most metric name path elements. If empty then
   *                   this is equivalent to calling {@link #clearRegistry(String)},
   *                   otherwise non-empty elements will be joined using dotted notation
   *                   to form a fully-qualified prefix. Metrics with names that start
   *                   with the prefix will be removed.
   * @return set of metrics names that have been removed.
   */
  public Set<String> clearMetrics(String registry, String... metricPath) {
    PrefixFilter filter;
    if (metricPath == null || metricPath.length == 0) {
      filter = new PrefixFilter("");
    } else {
      String prefix = MetricRegistry.name("", metricPath);
      filter = new PrefixFilter(prefix);
    }
    registry(registry).removeMatching(filter);
    return filter.getMatched();
  }

  /**
   * Retrieve matching metrics and their names.
   *
   * @param registry     registry name.
   * @param metricFilter filter (null is equivalent to {@link MetricFilter#ALL}).
   * @return map of matching names and metrics
   */
  public Map<String, Metric> getMetrics(String registry, MetricFilter metricFilter) {
    if (metricFilter == null || metricFilter == MetricFilter.ALL) {
      return registry(registry).getMetrics();
    }
    return registry(registry).getMetrics().entrySet().stream()
        .filter(entry -> metricFilter.matches(entry.getKey(), entry.getValue()))
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
  }

  /**
   * Create or get an existing named {@link Meter}
   *
   * @param registry   registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Meter}
   */
  public Meter meter(SolrInfoBean info, String registry, String metricName, String... metricPath) {
    final String name = mkName(metricName, metricPath);
    if (info != null) {
      info.registerMetricName(name);
    }
    return registry(registry).meter(name, meterSupplier);
  }

  /**
   * Create or get an existing named {@link Timer}
   *
   * @param registry   registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Timer}
   */
  public Timer timer(SolrInfoBean info, String registry, String metricName, String... metricPath) {
    final String name = mkName(metricName, metricPath);
    if (info != null) {
      info.registerMetricName(name);
    }
    return registry(registry).timer(name, timerSupplier);
  }

  /**
   * Create or get an existing named {@link Counter}
   *
   * @param registry   registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Counter}
   */
  public Counter counter(SolrInfoBean info, String registry, String metricName, String... metricPath) {
    final String name = mkName(metricName, metricPath);
    if (info != null) {
      info.registerMetricName(name);
    }
    return registry(registry).counter(name, counterSupplier);
  }

  /**
   * Create or get an existing named {@link Histogram}
   *
   * @param registry   registry name
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   * @return existing or a newly created {@link Histogram}
   */
  public Histogram histogram(SolrInfoBean info, String registry, String metricName, String... metricPath) {
    final String name = mkName(metricName, metricPath);
    if (info != null) {
      info.registerMetricName(name);
    }
    return registry(registry).histogram(name, histogramSupplier);
  }

  /**
   * Register an instance of {@link Metric}.
   *
   * @param registry   registry name
   * @param metric     metric instance
   * @param force      if true then an already existing metric with the same name will be replaced.
   *                   When false and a metric with the same name already exists an exception
   *                   will be thrown.
   * @param metricName metric name, either final name or a fully-qualified name
   *                   using dotted notation
   * @param metricPath (optional) additional top-most metric name path elements
   */
  public void registerMetric(SolrInfoBean info, String registry, Metric metric, boolean force, String metricName, String... metricPath) {
    MetricRegistry metricRegistry = registry(registry);
    String fullName = mkName(metricName, metricPath);
    if (info != null) {
      info.registerMetricName(fullName);
    }
    synchronized (metricRegistry) { // prevent race; register() throws if metric is already present
      if (force) { // must remove any existing one if present
        metricRegistry.remove(fullName);
      }
      metricRegistry.register(fullName, metric);
    }
  }

  /**
   * This is a wrapper for {@link Gauge} metrics, which are usually implemented as
   * lambdas that often keep a reference to their parent instance. In order to make sure that
   * all such metrics are removed when their parent instance is removed / closed the
   * metric is associated with an instance tag, which can be used then to remove
   * wrappers with the matching tag using {@link #unregisterGauges(String, String)}.
   */
  public static class GaugeWrapper<T> implements Gauge<T> {
    private final Gauge<T> gauge;
    private final String tag;

    public GaugeWrapper(Gauge<T> gauge, String tag) {
      this.gauge = gauge;
      this.tag = tag;
    }

    @Override
    public T getValue() {
      return gauge.getValue();
    }

    public String getTag() {
      return tag;
    }

    public Gauge<T> getGauge() {
      return gauge;
    }
  }
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void registerGauge(SolrInfoBean info, String registry, Gauge<?> gauge, String tag, boolean force, String metricName, String... metricPath) {
    if (!metricsConfig.isEnabled()) {
      gauge = MetricSuppliers.getNoOpGauge(gauge);
    }
    registerMetric(info, registry, new GaugeWrapper(gauge, tag), force, metricName, metricPath);
  }

  public int unregisterGauges(String registryName, String tagSegment) {
    if (tagSegment == null) {
      return 0;
    }
    MetricRegistry registry = registry(registryName);
    if (registry == null) return 0;
    AtomicInteger removed = new AtomicInteger();
    registry.removeMatching((name, metric) -> {
      if (metric instanceof GaugeWrapper) {
        @SuppressWarnings({"rawtypes"})
        GaugeWrapper wrapper = (GaugeWrapper) metric;
        boolean toRemove = wrapper.getTag().contains(tagSegment);
        if (toRemove) {
          removed.incrementAndGet();
        }
        return toRemove;
      }
      return false;

    });
    return removed.get();
  }

  /**
   * This method creates a hierarchical name with arbitrary levels of hierarchy
   *
   * @param name the final segment of the name, must not be null or empty.
   * @param path optional path segments, starting from the top level. Empty or null
   *             segments will be skipped.
   * @return fully-qualified name using dotted notation, with all valid hierarchy
   * segments prepended to the name.
   */
  public static String mkName(String name, String... path) {
    return makeName(path == null || path.length == 0 ? Collections.emptyList() : Arrays.asList(path),
        name);

  }

  public static String makeName(List<String> path, String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name must not be empty");
    }
    if (path == null || path.size() == 0) {
      return name;
    } else {
      StringBuilder sb = new StringBuilder();
      for (String s : path) {
        if (s == null || s.isEmpty()) {
          continue;
        }
        if (sb.length() > 0) {
          sb.append('.');
        }
        sb.append(s);
      }
      if (sb.length() > 0) {
        sb.append('.');
      }
      sb.append(name);
      return sb.toString();
    }
  }

  /**
   * Enforces the leading {@link #REGISTRY_NAME_PREFIX} in a name.
   *
   * @param name input name, possibly without the prefix
   * @return original name if it contained the prefix, or the
   * input name with the prefix prepended.
   */
  public static String enforcePrefix(String name) {
    if (name.startsWith(REGISTRY_NAME_PREFIX)) {
      return name;
    } else {
      return new StringBuilder(REGISTRY_NAME_PREFIX).append(name).toString();
    }
  }

  /**
   * Helper method to construct a properly prefixed registry name based on the group.
   *
   * @param group reporting group
   * @param names optional child elements of the registry name. If exactly one element is provided
   *              and it already contains the required prefix and group name then this value will be used,
   *              and the group parameter will be ignored.
   * @return fully-qualified and prefixed registry name, with overrides applied.
   */
  public static String getRegistryName(SolrInfoBean.Group group, String... names) {
    String fullName;
    String prefix = new StringBuilder(REGISTRY_NAME_PREFIX).append(group.name()).append('.').toString();
    // check for existing prefix and group
    if (names != null && names.length > 0 && names[0] != null && names[0].startsWith(prefix)) {
      // assume the first segment already was expanded
      if (names.length > 1) {
        String[] newNames = new String[names.length - 1];
        System.arraycopy(names, 1, newNames, 0, newNames.length);
        fullName = MetricRegistry.name(names[0], newNames);
      } else {
        fullName = MetricRegistry.name(names[0]);
      }
    } else {
      fullName = MetricRegistry.name(group.toString(), names);
    }
    return enforcePrefix(fullName);
  }

  // reporter management

  /**
   * Create and register {@link SolrMetricReporter}-s specific to a {@link org.apache.solr.core.SolrInfoBean.Group}.
   * Note: reporters that specify neither "group" nor "registry" attributes are treated as universal -
   * they will always be loaded for any group. These two attributes may also contain multiple comma- or
   * whitespace-separated values, in which case the reporter will be loaded for any matching value from
   * the list. If both attributes are present then only "group" attribute will be processed.
   *
   * @param pluginInfos   plugin configurations
   * @param loader        resource loader
   * @param coreContainer core container
   * @param solrCore      optional solr core
   * @param tag           optional tag for the reporters, to distinguish reporters logically created for different parent
   *                      component instances.
   * @param group         selected group, not null
   * @param registryNames optional child registry name elements
   */
  public void loadReporters(PluginInfo[] pluginInfos, SolrResourceLoader loader, CoreContainer coreContainer, SolrCore solrCore, String tag, SolrInfoBean.Group group, String... registryNames) {
    if (pluginInfos == null || pluginInfos.length == 0) {
      return;
    }
    String registryName = getRegistryName(group, registryNames);
    for (PluginInfo info : pluginInfos) {
      String target = info.attributes.get("group");
      if (target == null) { // no "group"
        target = info.attributes.get("registry");
        if (target != null) {
          String[] targets = target.split("[\\s,]+");
          boolean found = false;
          for (String t : targets) {
            t = enforcePrefix(t);
            if (registryName.equals(t)) {
              found = true;
              break;
            }
          }
          if (!found) {
            continue;
          }
        } else {
          // neither group nor registry specified.
          // always register this plugin for all groups and registries
        }
      } else { // check groups
        String[] targets = target.split("[\\s,]+");
        boolean found = false;
        for (String t : targets) {
          if (group.toString().equals(t)) {
            found = true;
            break;
          }
        }
        if (!found) {
          continue;
        }
      }
      try {
        loadReporter(registryName, loader, coreContainer, solrCore, info, tag);
      } catch (Exception e) {
        log.warn("Error loading metrics reporter, plugin info: {}", info, e);
      }
    }
  }

  /**
   * Convenience wrapper for {@link SolrMetricManager#loadReporter(String, SolrResourceLoader, CoreContainer, SolrCore, PluginInfo, String)}
   * passing {@link SolrCore#getResourceLoader()} and {@link SolrCore#getCoreContainer()} as the extra parameters.
   */
  public void loadReporter(String registry, SolrCore solrCore, PluginInfo pluginInfo, String tag) throws Exception {
    loadReporter(registry,
        solrCore.getResourceLoader(),
        solrCore.getCoreContainer(),
        solrCore,
        pluginInfo,
        tag);
  }

  /**
   * Convenience wrapper for {@link SolrMetricManager#loadReporter(String, SolrResourceLoader, CoreContainer, SolrCore, PluginInfo, String)}
   * passing {@link CoreContainer#getResourceLoader()} and null solrCore and tag.
   */
  public void loadReporter(String registry, CoreContainer coreContainer, PluginInfo pluginInfo) throws Exception {
    loadReporter(registry,
        coreContainer.getResourceLoader(),
        coreContainer,
        null,
        pluginInfo,
        null);
  }

  /**
   * Create and register an instance of {@link SolrMetricReporter}.
   *
   * @param registry      reporter is associated with this registry
   * @param loader        loader to use when creating an instance of the reporter
   * @param coreContainer core container
   * @param solrCore      optional solr core
   * @param pluginInfo    plugin configuration. Plugin "name" and "class" attributes are required.
   * @param tag           optional tag for the reporter, to distinguish reporters logically created for different parent
   *                      component instances.
   * @throws Exception if any argument is missing or invalid
   */
  @SuppressWarnings({"rawtypes"})
  public void loadReporter(String registry, SolrResourceLoader loader, CoreContainer coreContainer, SolrCore solrCore, PluginInfo pluginInfo, String tag) throws Exception {
    if (registry == null || pluginInfo == null || pluginInfo.name == null || pluginInfo.className == null) {
      throw new IllegalArgumentException("loadReporter called with missing arguments: " +
          "registry=" + registry + ", loader=" + loader + ", pluginInfo=" + pluginInfo);
    }
    // make sure we use a name with prefix
    registry = enforcePrefix(registry);
    SolrMetricReporter reporter = loader.newInstance(
        pluginInfo.className,
        SolrMetricReporter.class,
        new String[0],
    new Class[]{SolrMetricManager.class, String.class},
        new Object[]{this, registry}
    );
    // prepare MDC for plugins that want to use its properties
    MDCLoggingContext.setCoreDescriptor(coreContainer, solrCore == null ? null : solrCore.getCoreDescriptor());
    if (tag != null) {
      // add instance tag to MDC
      MDC.put("tag", "t:" + tag);
    }
    try {
      if (reporter instanceof SolrCoreReporter) {
        ((SolrCoreReporter) reporter).init(pluginInfo, solrCore);
      } else if (reporter instanceof SolrCoreContainerReporter) {
        ((SolrCoreContainerReporter) reporter).init(pluginInfo, coreContainer);
      } else {
        reporter.init(pluginInfo);
      }
    } catch (IllegalStateException e) {
      throw new IllegalArgumentException("reporter init failed: " + pluginInfo, e);
    } finally {
      MDCLoggingContext.clear();
      MDC.remove("tag");
    }
    registerReporter(registry, pluginInfo.name, tag, reporter);
  }

  private void registerReporter(String registry, String name, String tag, SolrMetricReporter reporter) throws Exception {
    try {
      if (!reportersLock.tryLock(10, TimeUnit.SECONDS)) {
        throw new Exception("Could not obtain lock to modify reporters registry: " + registry);
      }
    } catch (InterruptedException e) {
      throw new Exception("Interrupted while trying to obtain lock to modify reporters registry: " + registry);
    }
    try {
      Map<String, SolrMetricReporter> perRegistry = reporters.get(registry);
      if (perRegistry == null) {
        perRegistry = new HashMap<>();
        reporters.put(registry, perRegistry);
      }
      if (tag != null && !tag.isEmpty()) {
        name = name + "@" + tag;
      }
      SolrMetricReporter oldReporter = perRegistry.get(name);
      if (oldReporter != null) { // close it
        log.info("Replacing existing reporter '{}' in registry'{}': {}", name, registry, oldReporter);
        oldReporter.close();
      }
      perRegistry.put(name, reporter);

    } finally {
      reportersLock.unlock();
    }
  }

  /**
   * Close and unregister a named {@link SolrMetricReporter} for a registry.
   *
   * @param registry registry name
   * @param name     reporter name
   * @param tag      optional tag for the reporter, to distinguish reporters logically created for different parent
   *                 component instances.
   * @return true if a named reporter existed and was closed.
   */
  public boolean closeReporter(String registry, String name, String tag) {
    // make sure we use a name with prefix
    registry = enforcePrefix(registry);
    try {
      if (!reportersLock.tryLock(10, TimeUnit.SECONDS)) {
        log.warn("Could not obtain lock to modify reporters registry: {}", registry);
        return false;
      }
    } catch (InterruptedException e) {
      log.warn("Interrupted while trying to obtain lock to modify reporters registry: {}", registry);
      return false;
    }
    try {
      Map<String, SolrMetricReporter> perRegistry = reporters.get(registry);
      if (perRegistry == null) {
        return false;
      }
      if (tag != null && !tag.isEmpty()) {
        name = name + "@" + tag;
      }
      SolrMetricReporter reporter = perRegistry.remove(name);
      if (reporter == null) {
        return false;
      }
      try {
        reporter.close();
      } catch (Exception e) {
        log.warn("Error closing metric reporter, registry={}, name={}", registry, name, e);
      }
      return true;
    } finally {
      reportersLock.unlock();
    }
  }

  /**
   * Close and unregister all {@link SolrMetricReporter}-s for a registry.
   *
   * @param registry registry name
   * @return names of closed reporters
   */
  public Set<String> closeReporters(String registry) {
    return closeReporters(registry, null);
  }

  /**
   * Close and unregister all {@link SolrMetricReporter}-s for a registry.
   *
   * @param registry registry name
   * @param tag      optional tag for the reporter, to distinguish reporters logically created for different parent
   *                 component instances.
   * @return names of closed reporters
   */
  public Set<String> closeReporters(String registry, String tag) {
    // make sure we use a name with prefix
    registry = enforcePrefix(registry);
    try {
      if (!reportersLock.tryLock(10, TimeUnit.SECONDS)) {
        log.warn("Could not obtain lock to modify reporters registry: {}", registry);
        return Collections.emptySet();
      }
    } catch (InterruptedException e) {
      log.warn("Interrupted while trying to obtain lock to modify reporters registry: {}", registry);
      return Collections.emptySet();
    }
    try {
      log.info("Closing metric reporters for registry={} tag={}", registry, tag);
      Map<String, SolrMetricReporter> perRegistry = reporters.get(registry);
      if (perRegistry != null) {
        Set<String> names = new HashSet<>(perRegistry.keySet());
        Set<String> removed = new HashSet<>();
        names.forEach(name -> {
          if (tag != null && !tag.isEmpty() && !name.endsWith("@" + tag)) {
            return;
          }
          SolrMetricReporter reporter = perRegistry.remove(name);
          try {
            reporter.close();
          } catch (IOException ioe) {
            log.warn("Exception closing reporter {}", reporter, ioe);
          }
          removed.add(name);
        });
        if (removed.size() == names.size()) {
          reporters.remove(registry);
        }
        return removed;
      } else {
        return Collections.emptySet();
      }
    } finally {
      reportersLock.unlock();
      if (log.isDebugEnabled()) {
        log.debug("Finished closing registry={}, tag={}", registry, tag);
      }
    }
  }

  /**
   * Get a map of reporters for a registry. Keys are reporter names, values are reporter instances.
   *
   * @param registry registry name
   * @return map of reporters and their names, may be empty but never null
   */
  public Map<String, SolrMetricReporter> getReporters(String registry) {
    // make sure we use a name with prefix
    registry = enforcePrefix(registry);
    try {
      if (!reportersLock.tryLock(10, TimeUnit.SECONDS)) {
        log.warn("Could not obtain lock to modify reporters registry: {}", registry);
        return Collections.emptyMap();
      }
    } catch (InterruptedException e) {
      log.warn("Interrupted while trying to obtain lock to modify reporters registry: {}", registry);
      return Collections.emptyMap();
    }
    try {
      Map<String, SolrMetricReporter> perRegistry = reporters.get(registry);
      if (perRegistry == null) {
        return Collections.emptyMap();
      } else {
        // defensive copy - the original map may change after we release the lock
        return Collections.unmodifiableMap(new HashMap<>(perRegistry));
      }
    } finally {
      reportersLock.unlock();
    }
  }

  private List<PluginInfo> prepareCloudPlugins(PluginInfo[] pluginInfos, String group,
                                               Map<String, String> defaultAttributes,
                                               Map<String, Object> defaultInitArgs) {
    List<PluginInfo> result = new ArrayList<>();
    if (pluginInfos == null) {
      pluginInfos = new PluginInfo[0];
    }
    for (PluginInfo info : pluginInfos) {
      String groupAttr = info.attributes.get("group");
      if (!group.equals(groupAttr)) {
        continue;
      }
      info = preparePlugin(info, defaultAttributes, defaultInitArgs);
      if (info != null) {
        result.add(info);
      }
    }
    return result;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private PluginInfo preparePlugin(PluginInfo info, Map<String, String> defaultAttributes,
                                   Map<String, Object> defaultInitArgs) {
    if (info == null) {
      return null;
    }
    String classNameAttr = info.attributes.get("class");

    Map<String, String> attrs = new HashMap<>(info.attributes);
    defaultAttributes.forEach((k, v) -> {
      if (!attrs.containsKey(k)) {
        attrs.put(k, v);
      }
    });
    attrs.put("class", classNameAttr);
    Map<String, Object> initArgs = new HashMap<>();
    if (info.initArgs != null) {
      initArgs.putAll(info.initArgs.asMap(10));
    }
    defaultInitArgs.forEach((k, v) -> {
      if (!initArgs.containsKey(k)) {
        initArgs.put(k, v);
      }
    });
    return new PluginInfo(info.type, attrs, new NamedList(initArgs), null);
  }

  public void loadShardReporters(PluginInfo[] pluginInfos, SolrCore core) {
    // don't load for non-cloud cores
    if (core.getCoreDescriptor().getCloudDescriptor() == null) {
      return;
    }
    // prepare default plugin if none present in the config
    Map<String, String> attrs = new HashMap<>();
    attrs.put("name", "shardDefault");
    attrs.put("group", SolrInfoBean.Group.shard.toString());
    Map<String, Object> initArgs = new HashMap<>();
    initArgs.put("period", DEFAULT_CLOUD_REPORTER_PERIOD);

    String registryName = core.getCoreMetricManager().getRegistryName();
    // collect infos and normalize
    List<PluginInfo> infos = prepareCloudPlugins(pluginInfos, SolrInfoBean.Group.shard.toString(),
        attrs, initArgs);
    for (PluginInfo info : infos) {
      try {
        loadReporter(registryName, core, info, core.getMetricTag());
      } catch (Exception e) {
        log.warn("Could not load shard reporter, pluginInfo={}", info, e);
      }
    }
  }

  public void loadClusterReporters(PluginInfo[] pluginInfos, CoreContainer cc) {
    // don't load for non-cloud instances
    if (!cc.isZooKeeperAware()) {
      return;
    }
    Map<String, String> attrs = new HashMap<>();
    attrs.put("name", "clusterDefault");
    attrs.put("group", SolrInfoBean.Group.cluster.toString());
    Map<String, Object> initArgs = new HashMap<>();
    initArgs.put("period", DEFAULT_CLOUD_REPORTER_PERIOD);
    List<PluginInfo> infos = prepareCloudPlugins(pluginInfos, SolrInfoBean.Group.cluster.toString(),
        attrs, initArgs);
    String registryName = getRegistryName(SolrInfoBean.Group.cluster);
    for (PluginInfo info : infos) {
      try {
        loadReporter(registryName, cc, info);
      } catch (Exception e) {
        log.warn("Could not load cluster reporter, pluginInfo={}", info, e);
      }
    }
  }

}
