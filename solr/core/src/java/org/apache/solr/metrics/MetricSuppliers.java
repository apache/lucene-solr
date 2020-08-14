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

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for constructing instances of {@link com.codahale.metrics.MetricRegistry.MetricSupplier}
 * based on plugin configuration. This allows us to customize eg. {@link com.codahale.metrics.Reservoir}
 * implementations and parameters for timers and histograms.
 * <p>Custom supplier implementations must provide a zero-args constructor, and may optionally implement
 * {@link org.apache.solr.util.plugin.PluginInfoInitialized} interface for configuration - if they don't then
 * {@link org.apache.solr.util.SolrPluginUtils#invokeSetters(Object, Iterable, boolean)} will be used for initialization.</p>
 */
public class MetricSuppliers {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Default {@link Counter} supplier. No configuration available.
   */
  public static final class DefaultCounterSupplier implements MetricRegistry.MetricSupplier<Counter> {
    @Override
    public Counter newMetric() {
      return new Counter();
    }
  }

  // back-compat implementation, no longer present in metrics-4
  private static final class CpuTimeClock extends Clock {
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    @Override
    public long getTick() {
      return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }
  }

  private static final Clock CPU_CLOCK = new CpuTimeClock();
  private static final Clock USER_CLOCK = new Clock.UserTimeClock();

  /** Clock type parameter. */
  public static final String CLOCK = "clock";
  /** User-time clock. */
  public static final String CLOCK_USER = "user";
  /** CPU-time clock. */
  public static final String CLOCK_CPU = "cpu";

  /**
   * Default {@link Meter} supplier. The following configuration is available, either as attribute
   * or initArgs:
   * <ul>
   *   <li>clock - (string) can be set to {@link #CLOCK_USER} for {@link com.codahale.metrics.Clock.UserTimeClock} or
   *   {@link #CLOCK_CPU} for {@link CpuTimeClock}. If not set then the value of
   *   {@link Clock#defaultClock()} will be used.</li>
   * </ul>
   */
  public static final class DefaultMeterSupplier implements MetricRegistry.MetricSupplier<Meter>, PluginInfoInitialized {

    public Clock clk = Clock.defaultClock();

    @Override
    public void init(PluginInfo info) {
      clk = getClock(info, CLOCK);
    }

    @Override
    public Meter newMetric() {
      return new Meter(clk);
    }

  }

  private static Clock getClock(PluginInfo info, String param) {
    if (info == null) {
      return Clock.defaultClock();
    }
    String clock = null;
    if (info.attributes != null) {
      clock = info.attributes.get(param);
    }
    if (clock == null && info.initArgs != null) {
      clock = (String)info.initArgs.get(param);
    }
    Clock clk = Clock.defaultClock();
    if (clock != null) {
      if (clock.equalsIgnoreCase(CLOCK_USER)) {
        clk = USER_CLOCK;
      } else if (clock.equalsIgnoreCase(CLOCK_CPU)) {
        clk = CPU_CLOCK;
      }
    }
    return clk;
  }

  /** Implementation class, must implement {@link Reservoir}. Supports non-standard configuration
   * of the implementations available in metrics-core.
   */
  public static final String RESERVOIR = "reservoir";
  /** Size of reservoir. */
  public static final String RESERVOIR_SIZE = "size";
  /** Alpha parameter of {@link ExponentiallyDecayingReservoir}. */
  public static final String RESERVOIR_EDR_ALPHA = "alpha";
  /** Time window in seconds of {@link SlidingTimeWindowReservoir}. */
  public static final String RESERVOIR_WINDOW = "window";

  private static final String EDR_CLAZZ = ExponentiallyDecayingReservoir.class.getName();
  private static final String UNI_CLAZZ = UniformReservoir.class.getName();
  private static final String STW_CLAZZ = SlidingTimeWindowReservoir.class.getName();
  private static final String SW_CLAZZ = SlidingWindowReservoir.class.getName();

  private static final int DEFAULT_SIZE = 1028;
  private static final double DEFAULT_ALPHA = 0.015;
  private static final long DEFAULT_WINDOW = 300;

  private static final Reservoir getReservoir(SolrResourceLoader loader, PluginInfo info) {
    if (info == null) {
      return new ExponentiallyDecayingReservoir();
    }
    Clock clk = getClock(info, CLOCK);
    String clazz = ExponentiallyDecayingReservoir.class.getName();
    int size = -1;
    double alpha = -1;
    long window = -1;
    if (info.initArgs != null) {
      if (info.initArgs.get(RESERVOIR) != null) {
        String val = String.valueOf(info.initArgs.get(RESERVOIR)).trim();
        if (!val.isEmpty()) {
          clazz = val;
        }
      }
      Number n = (Number)info.initArgs.get(RESERVOIR_SIZE);
      if (n != null) {
        size = n.intValue();
      }
      n = (Number)info.initArgs.get(RESERVOIR_EDR_ALPHA);
      if (n != null) {
        alpha = n.doubleValue();
      }
      n = (Number)info.initArgs.get(RESERVOIR_WINDOW);
      if (n != null) {
        window = n.longValue();
      }
    }
    if (size <= 0) {
      size = DEFAULT_SIZE;
    }
    if (alpha <= 0) {
      alpha = DEFAULT_ALPHA;
    }
    // special case for core implementations
    if (clazz.equals(EDR_CLAZZ)) {
      return new ExponentiallyDecayingReservoir(size, alpha, clk);
    } else if (clazz.equals(UNI_CLAZZ)) {
      return new UniformReservoir(size);
    } else if (clazz.equals(STW_CLAZZ)) {
      if (window <= 0) {
        window = DEFAULT_WINDOW; // 5 minutes, comparable to EDR
      }
      return new SlidingTimeWindowReservoir(window, TimeUnit.SECONDS);
    } else if (clazz.equals(SW_CLAZZ)) {
      return new SlidingWindowReservoir(size);
    } else { // custom reservoir
      Reservoir reservoir;
      try {
        reservoir = loader.newInstance(clazz, Reservoir.class);
        if (reservoir instanceof PluginInfoInitialized) {
          ((PluginInfoInitialized)reservoir).init(info);
        } else {
          SolrPluginUtils.invokeSetters(reservoir, info.initArgs, true);
        }
        return reservoir;
      } catch (Exception e) {
        log.warn("Error initializing custom Reservoir implementation (will use default): " + info, e);
        return new ExponentiallyDecayingReservoir(size, alpha, clk);
      }
    }
  }

  /**
   * Default supplier of {@link Timer} instances, with configurable clock and reservoir.
   * See {@link DefaultMeterSupplier} for clock configuration. Reservoir configuration uses
   * {@link #RESERVOIR}, {@link #RESERVOIR_EDR_ALPHA}, {@link #RESERVOIR_SIZE} and
   * {@link #RESERVOIR_WINDOW}.
   */
  public static final class DefaultTimerSupplier implements MetricRegistry.MetricSupplier<Timer>, PluginInfoInitialized {

    public Clock clk = Clock.defaultClock();
    private PluginInfo info;
    private SolrResourceLoader loader;

    public DefaultTimerSupplier(SolrResourceLoader loader) {
      this.loader = loader;
    }

    @Override
    public void init(PluginInfo info) {
      clk = getClock(info, CLOCK);
      this.info = info;
    }

    public Reservoir getReservoir() {
      return MetricSuppliers.getReservoir(loader, info);
    }

    @Override
    public Timer newMetric() {
      return new Timer(getReservoir(), clk);
    }
  }

  /**
   * Default supplier of {@link Histogram} instances, with configurable reservoir.
   */
  public static final class DefaultHistogramSupplier implements MetricRegistry.MetricSupplier<Histogram>, PluginInfoInitialized {

    private PluginInfo info;
    private SolrResourceLoader loader;

    public DefaultHistogramSupplier(SolrResourceLoader loader) {
      this.loader = loader;
    }

    @Override
    public void init(PluginInfo info) {
      this.info = info;
    }

    public Reservoir getReservoir() {
      return MetricSuppliers.getReservoir(loader, info);
    }

    @Override
    public Histogram newMetric() {
      return new Histogram(getReservoir());
    }
  }

  /**
   * Create a {@link Counter} supplier.
   * @param loader resource loader
   * @param info plugin configuration, or null for default
   * @return configured supplier instance, or default instance if configuration was invalid
   */
  public static MetricRegistry.MetricSupplier<Counter> counterSupplier(SolrResourceLoader loader, PluginInfo info) {
    if (info == null || info.className == null || info.className.trim().isEmpty()) {
      return new DefaultCounterSupplier();
    }

    MetricRegistry.MetricSupplier<Counter> supplier;
    try {
      supplier = loader.newInstance(info.className, MetricRegistry.MetricSupplier.class);
    } catch (Exception e) {
      log.warn("Error creating custom Counter supplier (will use default): " + info, e);
      supplier = new DefaultCounterSupplier();
    }
    if (supplier instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized)supplier).init(info);
    } else {
      SolrPluginUtils.invokeSetters(supplier, info.initArgs, true);
    }
    return supplier;
  }

  /**
   * Create a {@link Meter} supplier.
   * @param loader resource loader
   * @param info plugin configuration, or null for default
   * @return configured supplier instance, or default instance if configuration was invalid
   */
  public static MetricRegistry.MetricSupplier<Meter> meterSupplier(SolrResourceLoader loader, PluginInfo info) {
    MetricRegistry.MetricSupplier<Meter> supplier;
    if (info == null || info.className == null || info.className.isEmpty()) {
      supplier = new DefaultMeterSupplier();
    } else {
      try {
        supplier = loader.newInstance(info.className, MetricRegistry.MetricSupplier.class);
      } catch (Exception e) {
        log.warn("Error creating custom Meter supplier (will use default): " + info, e);
        supplier = new DefaultMeterSupplier();
      }
    }
    if (supplier instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized)supplier).init(info);
    } else {
      SolrPluginUtils.invokeSetters(supplier, info.initArgs, true);
    }
    return supplier;
  }

  /**
   * Create a {@link Timer} supplier.
   * @param loader resource loader
   * @param info plugin configuration, or null for default
   * @return configured supplier instance, or default instance if configuration was invalid
   */
  public static MetricRegistry.MetricSupplier<Timer> timerSupplier(SolrResourceLoader loader, PluginInfo info) {
    MetricRegistry.MetricSupplier<Timer> supplier;
    if (info == null || info.className == null || info.className.isEmpty()) {
      supplier = new DefaultTimerSupplier(loader);
    } else {
      try {
        supplier = loader.newInstance(info.className, MetricRegistry.MetricSupplier.class);
      } catch (Exception e) {
        log.warn("Error creating custom Timer supplier (will use default): " + info, e);
        supplier = new DefaultTimerSupplier(loader);
      }
    }
    if (supplier instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized)supplier).init(info);
    } else {
      SolrPluginUtils.invokeSetters(supplier, info.initArgs, true);
    }
    return supplier;
  }

  /**
   * Create a {@link Histogram} supplier.
   * @param info plugin configuration, or null for default
   * @return configured supplier instance, or default instance if configuration was invalid
   */
  public static MetricRegistry.MetricSupplier<Histogram> histogramSupplier(SolrResourceLoader loader, PluginInfo info) {
    MetricRegistry.MetricSupplier<Histogram> supplier;
    if (info == null || info.className == null || info.className.isEmpty()) {
      supplier = new DefaultHistogramSupplier(loader);
    } else {
      try {
        supplier = loader.newInstance(info.className, MetricRegistry.MetricSupplier.class);
      } catch (Exception e) {
        log.warn("Error creating custom Histogram supplier (will use default): " + info, e);
        supplier = new DefaultHistogramSupplier(loader);
      }
    }
    if (supplier instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized)supplier).init(info);
    } else {
      SolrPluginUtils.invokeSetters(supplier, info.initArgs, true);
    }
    return supplier;
  }
}
