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
package org.apache.solr.metrics.reporters.jmx;

import javax.management.Attribute;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.jmx.DefaultObjectNameFactory;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.jmx.ObjectNameFactory;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Timer;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a modified copy of Dropwizard's {@link com.codahale.metrics.jmx.JmxReporter} and classes that it internally uses,
 * with a few important differences:
 * <ul>
 * <li>this class knows that it can directly use {@link MetricsMap} as a dynamic MBean.</li>
 * <li>this class allows us to "tag" MBean instances so that we can later unregister only instances registered with the
 * same tag.</li>
 * <li>this class processes all metrics already existing in the registry at the time when reporter is started.</li>
 * </ul>
 */
public class JmxMetricsReporter implements Reporter, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String INSTANCE_TAG = "_instanceTag";

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * Builder for the {@link JmxMetricsReporter} class.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private MBeanServer mBeanServer;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private ObjectNameFactory objectNameFactory;
    private MetricFilter filter = MetricFilter.ALL;
    private String domain;
    private String tag;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.domain = "metrics";
      this.objectNameFactory = new DefaultObjectNameFactory();
    }

    /**
     * Register MBeans with the given {@link MBeanServer}.
     *
     * @param mBeanServer     an {@link MBeanServer}
     * @return {@code this}
     */
    public Builder registerWith(MBeanServer mBeanServer) {
      this.mBeanServer = mBeanServer;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public Builder createsObjectNamesWith(ObjectNameFactory onFactory) {
      if(onFactory == null) {
        throw new IllegalArgumentException("null objectNameFactory");
      }
      this.objectNameFactory = onFactory;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public Builder inDomain(String domain) {
      this.domain = domain;
      return this;
    }

    public Builder withTag(String tag) {
      this.tag = tag;
      return this;
    }

    public JmxMetricsReporter build() {
      if (tag == null) {
        tag = Integer.toHexString(this.hashCode());
      }
      return new JmxMetricsReporter(mBeanServer, domain, registry, filter, rateUnit, durationUnit, objectNameFactory, tag);
    }

  }

  // MBean interfaces and base classes
  public interface MetricMBean {
    ObjectName objectName();
    // this strange-looking method name is used for producing "_instanceTag" attribute name
    String get_instanceTag();
  }


  private abstract static class AbstractBean implements MetricMBean {
    private final ObjectName objectName;
    private final String instanceTag;

    AbstractBean(ObjectName objectName, String instanceTag) {
      this.objectName = objectName;
      this.instanceTag = instanceTag;
    }

    @Override
    public String get_instanceTag() {
      return instanceTag;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }
  }

  public interface JmxGaugeMBean extends MetricMBean {
    Object getValue();
  }

  private static class JmxGauge extends AbstractBean implements JmxGaugeMBean {
    private final Gauge<?> metric;

    private JmxGauge(Gauge<?> metric, ObjectName objectName, String tag) {
      super(objectName, tag);
      this.metric = metric;
    }

    @Override
    public Object getValue() {
      return metric.getValue();
    }
  }

  public interface JmxCounterMBean extends MetricMBean {
    long getCount();
  }

  private static class JmxCounter extends AbstractBean implements JmxCounterMBean {
    private final Counter metric;

    private JmxCounter(Counter metric, ObjectName objectName, String tag) {
      super(objectName, tag);
      this.metric = metric;
    }

    @Override
    public long getCount() {
      return metric.getCount();
    }
  }

  public interface JmxHistogramMBean extends MetricMBean {
    long getCount();

    long getMin();

    long getMax();

    double getMean();

    double getStdDev();

    double get50thPercentile();

    double get75thPercentile();

    double get95thPercentile();

    double get98thPercentile();

    double get99thPercentile();

    double get999thPercentile();

    long[] values();

    long getSnapshotSize();
  }

  private static class JmxHistogram extends AbstractBean implements JmxHistogramMBean {
    private final Histogram metric;

    private JmxHistogram(Histogram metric, ObjectName objectName, String tag) {
      super(objectName, tag);
      this.metric = metric;
    }

    @Override
    public double get50thPercentile() {
      return metric.getSnapshot().getMedian();
    }

    @Override
    public long getCount() {
      return metric.getCount();
    }

    @Override
    public long getMin() {
      return metric.getSnapshot().getMin();
    }

    @Override
    public long getMax() {
      return metric.getSnapshot().getMax();
    }

    @Override
    public double getMean() {
      return metric.getSnapshot().getMean();
    }

    @Override
    public double getStdDev() {
      return metric.getSnapshot().getStdDev();
    }

    @Override
    public double get75thPercentile() {
      return metric.getSnapshot().get75thPercentile();
    }

    @Override
    public double get95thPercentile() {
      return metric.getSnapshot().get95thPercentile();
    }

    @Override
    public double get98thPercentile() {
      return metric.getSnapshot().get98thPercentile();
    }

    @Override
    public double get99thPercentile() {
      return metric.getSnapshot().get99thPercentile();
    }

    @Override
    public double get999thPercentile() {
      return metric.getSnapshot().get999thPercentile();
    }

    @Override
    public long[] values() {
      return metric.getSnapshot().getValues();
    }

    public long getSnapshotSize() {
      return metric.getSnapshot().size();
    }
  }

  public interface JmxMeterMBean extends MetricMBean {
    long getCount();

    double getMeanRate();

    double getOneMinuteRate();

    double getFiveMinuteRate();

    double getFifteenMinuteRate();

    String getRateUnit();
  }

  private static class JmxMeter extends AbstractBean implements JmxMeterMBean {
    private final Metered metric;
    private final double rateFactor;
    private final String rateUnit;

    private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit, String tag) {
      super(objectName, tag);
      this.metric = metric;
      this.rateFactor = rateUnit.toSeconds(1);
      this.rateUnit = ("events/" + calculateRateUnit(rateUnit)).intern();
    }

    @Override
    public long getCount() {
      return metric.getCount();
    }

    @Override
    public double getMeanRate() {
      return metric.getMeanRate() * rateFactor;
    }

    @Override
    public double getOneMinuteRate() {
      return metric.getOneMinuteRate() * rateFactor;
    }

    @Override
    public double getFiveMinuteRate() {
      return metric.getFiveMinuteRate() * rateFactor;
    }

    @Override
    public double getFifteenMinuteRate() {
      return metric.getFifteenMinuteRate() * rateFactor;
    }

    @Override
    public String getRateUnit() {
      return rateUnit;
    }

    private String calculateRateUnit(TimeUnit unit) {
      final String s = unit.toString().toLowerCase(Locale.US);
      return s.substring(0, s.length() - 1);
    }
  }

  public interface JmxTimerMBean extends JmxMeterMBean {
    double getMin();

    double getMax();

    double getMean();

    double getStdDev();

    double get50thPercentile();

    double get75thPercentile();

    double get95thPercentile();

    double get98thPercentile();

    double get99thPercentile();

    double get999thPercentile();

    long[] values();
    String getDurationUnit();
  }

  private static class JmxTimer extends JmxMeter implements JmxTimerMBean {
    private final Timer metric;
    private final double durationFactor;
    private final String durationUnit;

    private JmxTimer(Timer metric,
                     ObjectName objectName,
                     TimeUnit rateUnit,
                     TimeUnit durationUnit, String tag) {
      super(metric, objectName, rateUnit, tag);
      this.metric = metric;
      this.durationFactor = 1.0 / durationUnit.toNanos(1);
      this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
    }

    @Override
    public double get50thPercentile() {
      return metric.getSnapshot().getMedian() * durationFactor;
    }

    @Override
    public double getMin() {
      return metric.getSnapshot().getMin() * durationFactor;
    }

    @Override
    public double getMax() {
      return metric.getSnapshot().getMax() * durationFactor;
    }

    @Override
    public double getMean() {
      return metric.getSnapshot().getMean() * durationFactor;
    }

    @Override
    public double getStdDev() {
      return metric.getSnapshot().getStdDev() * durationFactor;
    }

    @Override
    public double get75thPercentile() {
      return metric.getSnapshot().get75thPercentile() * durationFactor;
    }

    @Override
    public double get95thPercentile() {
      return metric.getSnapshot().get95thPercentile() * durationFactor;
    }

    @Override
    public double get98thPercentile() {
      return metric.getSnapshot().get98thPercentile() * durationFactor;
    }

    @Override
    public double get99thPercentile() {
      return metric.getSnapshot().get99thPercentile() * durationFactor;
    }

    @Override
    public double get999thPercentile() {
      return metric.getSnapshot().get999thPercentile() * durationFactor;
    }

    @Override
    public long[] values() {
      return metric.getSnapshot().getValues();
    }

    @Override
    public String getDurationUnit() {
      return durationUnit;
    }
  }

  private static class JmxListener implements MetricRegistryListener {

    private final String name;
    private final MBeanServer mBeanServer;
    private final MetricFilter filter;
    private final TimeUnit rateUnit;
    private final TimeUnit durationUnit;
    private final Map<ObjectName, ObjectName> registered;
    private final ObjectNameFactory objectNameFactory;
    private final String tag;
    private final QueryExp exp;

    private JmxListener(MBeanServer mBeanServer, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit,
                        ObjectNameFactory objectNameFactory, String tag) {
      this.mBeanServer = mBeanServer;
      this.name = name;
      this.filter = filter;
      this.rateUnit = rateUnit;
      this.durationUnit = durationUnit;
      this.registered = new ConcurrentHashMap<>();
      this.objectNameFactory = objectNameFactory;
      this.tag = tag;
      this.exp = Query.eq(Query.attr(INSTANCE_TAG), Query.value(tag));
    }

    private void registerMBean(Object mBean, ObjectName objectName) throws InstanceAlreadyExistsException, JMException {
      // remove previous bean if exists
      if (mBeanServer.isRegistered(objectName)) {
        if (log.isDebugEnabled()) {
          Set<ObjectInstance> objects = mBeanServer.queryMBeans(objectName, null);
          log.debug("## removing existing " + objects.size() + " bean(s) for " + objectName.getCanonicalName() + ", current tag=" + tag + ":");
          for (ObjectInstance inst : objects) {
            log.debug("## - tag=" + mBeanServer.getAttribute(inst.getObjectName(), INSTANCE_TAG));
          }
        }
        mBeanServer.unregisterMBean(objectName);
      }
      ObjectInstance objectInstance = mBeanServer.registerMBean(mBean, objectName);
      if (objectInstance != null) {
        // the websphere mbeanserver rewrites the objectname to include
        // cell, node & server info
        // make sure we capture the new objectName for unregistration
        registered.put(objectName, objectInstance.getObjectName());
      } else {
        registered.put(objectName, objectName);
      }
      log.debug("## registered " + objectInstance.getObjectName().getCanonicalName() + ", tag=" + tag);
    }

    private void unregisterMBean(ObjectName originalObjectName) throws InstanceNotFoundException, MBeanRegistrationException {
      ObjectName objectName = registered.remove(originalObjectName);
      if (objectName == null) {
        objectName = originalObjectName;
      }
      Set<ObjectInstance> objects = mBeanServer.queryMBeans(objectName, exp);
      for (ObjectInstance o : objects) {
        log.debug("## Unregistered " + o.getObjectName().getCanonicalName() + ", tag=" + tag);
        mBeanServer.unregisterMBean(o.getObjectName());
      }
    }

    @Override
    public void onGaugeAdded(String name, Gauge<?> gauge) {
      try {
        if (filter.matches(name, gauge)) {
          final ObjectName objectName = createName("gauges", name);
          if (gauge instanceof SolrMetricManager.GaugeWrapper &&
              ((SolrMetricManager.GaugeWrapper)gauge).getGauge() instanceof MetricsMap) {
            MetricsMap mm = (MetricsMap)((SolrMetricManager.GaugeWrapper)gauge).getGauge();
            mm.setAttribute(new Attribute(INSTANCE_TAG, tag));
            // don't wrap it in a JmxGauge, it already supports all necessary JMX attributes
            registerMBean(mm, objectName);
          } else {
            registerMBean(new JmxGauge(gauge, objectName, tag), objectName);
          }
        }
      } catch (InstanceAlreadyExistsException e) {
        log.debug("Unable to register gauge", e);
      } catch (JMException e) {
        log.warn("Unable to register gauge", e);
      }
    }

    @Override
    public void onGaugeRemoved(String name) {
      try {
        final ObjectName objectName = createName("gauges", name);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        log.debug("Unable to unregister gauge", e);
      } catch (MBeanRegistrationException e) {
        log.warn("Unable to unregister gauge", e);
      }
    }

    @Override
    public void onCounterAdded(String name, Counter counter) {
      try {
        if (filter.matches(name, counter)) {
          final ObjectName objectName = createName("counters", name);
          registerMBean(new JmxCounter(counter, objectName, tag), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        log.debug("Unable to register counter", e);
      } catch (JMException e) {
        log.warn("Unable to register counter", e);
      }
    }

    @Override
    public void onCounterRemoved(String name) {
      try {
        final ObjectName objectName = createName("counters", name);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        log.debug("Unable to unregister counter", e);
      } catch (MBeanRegistrationException e) {
        log.warn("Unable to unregister counter", e);
      }
    }

    @Override
    public void onHistogramAdded(String name, Histogram histogram) {
      try {
        if (filter.matches(name, histogram)) {
          final ObjectName objectName = createName("histograms", name);
          registerMBean(new JmxHistogram(histogram, objectName, tag), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        log.debug("Unable to register histogram", e);
      } catch (JMException e) {
        log.warn("Unable to register histogram", e);
      }
    }

    @Override
    public void onHistogramRemoved(String name) {
      try {
        final ObjectName objectName = createName("histograms", name);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        log.debug("Unable to unregister histogram", e);
      } catch (MBeanRegistrationException e) {
        log.warn("Unable to unregister histogram", e);
      }
    }

    @Override
    public void onMeterAdded(String name, Meter meter) {
      try {
        if (filter.matches(name, meter)) {
          final ObjectName objectName = createName("meters", name);
          registerMBean(new JmxMeter(meter, objectName, rateUnit, tag), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        log.debug("Unable to register meter", e);
      } catch (JMException e) {
        log.warn("Unable to register meter", e);
      }
    }

    @Override
    public void onMeterRemoved(String name) {
      try {
        final ObjectName objectName = createName("meters", name);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        log.debug("Unable to unregister meter", e);
      } catch (MBeanRegistrationException e) {
        log.warn("Unable to unregister meter", e);
      }
    }

    @Override
    public void onTimerAdded(String name, Timer timer) {
      try {
        if (filter.matches(name, timer)) {
          final ObjectName objectName = createName("timers", name);
          registerMBean(new JmxTimer(timer, objectName, rateUnit, durationUnit, tag), objectName);
        }
      } catch (InstanceAlreadyExistsException e) {
        log.debug("Unable to register timer", e);
      } catch (JMException e) {
        log.warn("Unable to register timer", e);
      }
    }

    @Override
    public void onTimerRemoved(String name) {
      try {
        final ObjectName objectName = createName("timers", name);
        unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        log.debug("Unable to unregister timer", e);
      } catch (MBeanRegistrationException e) {
        log.warn("Unable to unregister timer", e);
      }
    }

    private ObjectName createName(String type, String name) {
      return objectNameFactory.createName(type, this.name, name);
    }

    void unregisterAll() {
      for (ObjectName name : registered.keySet()) {
        try {
          unregisterMBean(name);
        } catch (InstanceNotFoundException e) {
          log.debug("Unable to unregister metric", e);
        } catch (MBeanRegistrationException e) {
          log.warn("Unable to unregister metric", e);
        }
      }
      registered.clear();
    }
  }

  private final MetricRegistry registry;
  private final JmxListener listener;

  private JmxMetricsReporter(MBeanServer mBeanServer,
                             String domain,
                             MetricRegistry registry,
                             MetricFilter filter,
                             TimeUnit rateUnit,
                             TimeUnit durationUnit,
                             ObjectNameFactory objectNameFactory,
                             String tag) {
    this.registry = registry;
    this.listener = new JmxListener(mBeanServer, domain, filter, rateUnit, durationUnit, objectNameFactory, tag);
  }

  public void start() {
    registry.addListener(listener);
    // process existing metrics
    Map<String, Metric> metrics = new HashMap<>(registry.getMetrics());
    metrics.forEach((k, v) -> {
      if (v instanceof Counter) {
        listener.onCounterAdded(k, (Counter)v);
      } else if (v instanceof Meter) {
        listener.onMeterAdded(k, (Meter)v);
      } else if (v instanceof Histogram) {
        listener.onHistogramAdded(k, (Histogram)v);
      } else if (v instanceof Timer) {
        listener.onTimerAdded(k, (Timer)v);
      } else if (v instanceof Gauge) {
        listener.onGaugeAdded(k, (Gauge)v);
      } else {
        log.warn("Unknown metric type " + v.getClass().getName() + " for metric '" + k + "', ignoring");
      }
    });
  }

  @Override
  public void close() {
    registry.removeListener(listener);
    listener.unregisterAll();
  }
}
