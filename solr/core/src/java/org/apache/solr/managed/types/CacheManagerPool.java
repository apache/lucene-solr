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
package org.apache.solr.managed.types;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.solr.managed.ChangeListener;
import org.apache.solr.managed.ResourceManager;
import org.apache.solr.managed.ResourceManagerPool;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.SolrCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link org.apache.solr.managed.ResourceManagerPool} specific to
 * the management of {@link org.apache.solr.search.SolrCache} instances.
 * <p>This plugin calculates the total size and maxRamMB of all registered cache instances
 * and adjusts each cache's limits so that the aggregated values again fit within the pool limits.</p>
 * <p>In order to avoid thrashing the plugin uses a dead band (by default {@link #DEFAULT_DEAD_BAND}),
 * which can be adjusted using configuration parameter {@link #DEAD_BAND_PARAM}. If monitored values don't
 * exceed the limits +/- the dead band then no forcible adjustment takes place.</p>
 * <p>The management strategy consists of two distinct phases: soft optimization phase and then hard limit phase.</p>
 * <p><b>Soft optimization</b> tries to adjust the resource consumption based on the cache hit ratio.
 * This phase is executed only if there's no total limit exceeded. Also, hit ratio is considered a valid monitored
 * variable only when at least N lookups occurred since the last adjustment (default value is {@link #DEFAULT_LOOKUP_DELTA}).
 * If the hit ratio is higher than a threshold (default value is {@link #DEFAULT_TARGET_HITRATIO}) then the size
 * of the cache can be reduced so that the resource consumption is minimized while still keeping acceptable hit
 * ratio - and vice versa.</p>
 * <p>This optimization phase can only adjust the limits within a {@link #DEFAULT_MAX_ADJUST_RATIO}, i.e. increased
 * or decreased values may not be larger / smaller than this multiple / fraction of the initially configured limit.</p>
 * <p><b>Hard limit</b> phase follows the soft optimization phase and it forcibly reduces resource consumption of all components
 * if the total usage is still above the pool limit after the first phase has completed. Each component's limit is reduced
 * by the same factor, regardless of the actual population or hit ratio.</p>
 */
public class CacheManagerPool extends ResourceManagerPool<SolrCache> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String TYPE = "cache";

  /** Controller dead-band - changes smaller than this fraction of the total limit will be ignored. */
  public static final String DEAD_BAND_PARAM = "deadBand";
  /** Use soft optimization when not under resource shortage. This defaults to true. */
  public static final String OPTIMIZE_PARAM = "optimize";
  /** Use soft optimization to shrink cache sizes when not under resource shortage. This defaults to false. */
  public static final String OPTIMIZE_SHRINK_PARAM = "optimize_shrink";
  /** Use soft optimization to expand cache sizes when not under resource shortage.
   * <p>NOTE: this option defaults to true and in most situations should not be changed because there's
   * no other mechanism to automatically expand cache sizes when additional resources become available.
   * If this option is set to false and there's a resource shortage the cache sizes will be trimmed and
   * they will never automatically bounce back once the shortage is over.</p>
   */
  public static final String OPTIMIZE_EXPAND_PARAM = "optimize_expand";
  /** Target hit ratio - high enough to be useful, low enough to avoid excessive cache size. */
  public static final String TARGET_HIT_RATIO_PARAM = "targetHitRatio";
  /** Minimum delta in the number of lookups before attempting optimization. */
  public static final String LOOKUP_DELTA_PARAM = "lookupDelta";
  /**
   * Maximum allowed adjustment ratio from the initial configuration value. Adjusted value may not be
   * higher than multiple of this factor, and not lower than divided by this factor.
   */
  public static final String MAX_ADJUST_RATIO_PARAM = "maxAdjustRatio";
  /**
   * Minimum number of lookups since last adjustment to consider the reported hitRatio
   *  to be statistically valid.
   */
  public static final String MIN_LOOKUP_DELTA_PARAM = "minLookupDelta";
  /** Default value of dead band (10%). */
  public static final double DEFAULT_DEAD_BAND = 0.1;
  /** Default target hit ratio - a compromise between usefulness and limited resource usage. */
  public static final double DEFAULT_TARGET_HITRATIO = 0.6;
  /**
   * Default minimum number of lookups since the last adjustment. This can be treated as Bernoulli trials
   * that give a 5% confidence about the statistical validity of hit ratio (<code>0.5 / sqrt(lookups)</code>).
   */
  public static final long DEFAULT_LOOKUP_DELTA = 100;
  /**
   * Default maximum adjustment ratio from the initially configured values.
   */
  public static final double DEFAULT_MAX_ADJUST_RATIO = 2.0;
  /**
   * Report being unable to adjust the consumption within limits only after this many attempts.
   */
  public static final int DEFAULT_REPORT_EXCEEDED_THRESHOLD = 3;

  protected static final Map<String, Function<Map<String, Object>, Double>> controlledToMonitored = new HashMap<>();

  static {
    controlledToMonitored.put(SolrCache.MAX_RAM_MB_PARAM, values -> {
      Number ramBytes = (Number) values.get(SolrCache.RAM_BYTES_USED_PARAM);
      return ramBytes != null ? ramBytes.doubleValue() / SolrCache.MB : 0.0;
    });
    controlledToMonitored.put(SolrCache.MAX_SIZE_PARAM, values ->
        ((Number)values.getOrDefault(SolrCache.SIZE_PARAM, -1.0)).doubleValue());
  }

  protected double deadBand = DEFAULT_DEAD_BAND;
  protected double targetHitRatio = DEFAULT_TARGET_HITRATIO;
  protected long lookupDelta = DEFAULT_LOOKUP_DELTA;
  protected double maxAdjustRatio = DEFAULT_MAX_ADJUST_RATIO;
  protected boolean optimize = true;
  protected boolean optimize_shrink = false;
  protected boolean optimize_expand = true;
  protected Map<String, Long> lookups = new HashMap<>();
  protected Map<String, Long> hits = new HashMap<>();
  protected int reportExceededCounter = 0;
  protected int reportExceededThreshold = DEFAULT_REPORT_EXCEEDED_THRESHOLD;

  public CacheManagerPool(String name, String type, ResourceManager resourceManager, Map<String, Object> poolLimits, Map<String, Object> poolParams) {
    super(name, type, resourceManager, poolLimits, poolParams);
    String str = String.valueOf(poolParams.getOrDefault(DEAD_BAND_PARAM, DEFAULT_DEAD_BAND));
    try {
      deadBand = Double.parseDouble(str);
    } catch (Exception e) {
      log.warn("Invalid deadBand parameter value '" + str + "', using default " + DEFAULT_DEAD_BAND);
    }
    str = String.valueOf(poolParams.getOrDefault(TARGET_HIT_RATIO_PARAM, DEFAULT_TARGET_HITRATIO));
    try {
      targetHitRatio = Double.parseDouble(str);
    } catch (Exception e) {
      log.warn("Invalid targetHitRatio parameter value '" + str + "', using default " + DEFAULT_TARGET_HITRATIO);
    }
    str = String.valueOf(poolParams.getOrDefault(LOOKUP_DELTA_PARAM, DEFAULT_LOOKUP_DELTA));
    try {
      lookupDelta = Long.parseLong(str);
    } catch (Exception e) {
      log.warn("Invalid lookupDelta parameter value '" + str + "', using default " + DEFAULT_LOOKUP_DELTA);
    }
    str = String.valueOf(poolParams.getOrDefault(MAX_ADJUST_RATIO_PARAM, DEFAULT_MAX_ADJUST_RATIO));
    try {
      maxAdjustRatio = Double.parseDouble(str);
    } catch (Exception e) {
      log.warn("Invalid maxAdjustRatio parameter value '" + str + "', using default " + DEFAULT_MAX_ADJUST_RATIO);
    }
    str = String.valueOf(poolParams.getOrDefault(OPTIMIZE_PARAM, true));
    try {
      optimize = Boolean.parseBoolean(str);
    } catch (Exception e) {
      log.warn("Invalid optimize parameter value '" + str + "', using default " + true);
    }
    str = String.valueOf(poolParams.getOrDefault(OPTIMIZE_SHRINK_PARAM, false));
    try {
      optimize_shrink = Boolean.parseBoolean(str);
    } catch (Exception e) {
      log.warn("Invalid optimize_shrink parameter value '" + str + "', using default " + false);
    }
    str = String.valueOf(poolParams.getOrDefault(OPTIMIZE_EXPAND_PARAM, true));
    try {
      optimize_expand = Boolean.parseBoolean(str);
    } catch (Exception e) {
      log.warn("Invalid optimize_expand parameter value '" + str + "', using default " + true);
    }
  }

  @Override
  public boolean unregisterComponent(String componentId) {
    lookups.remove(componentId);
    hits.remove(componentId);
    return super.unregisterComponent(componentId);
  }

  @Override
  public Object doSetResourceLimit(SolrCache component, String limitName, Object val) {
    if (!(val instanceof Number)) {
      try {
        val = Long.parseLong(String.valueOf(val));
      } catch (Exception e) {
        throw new IllegalArgumentException("Unsupported value type (not a number) for limit '" + limitName + "': " + val + " (" + val.getClass().getName() + ")");
      }
    }
    Number value = (Number)val;
    if (value.longValue() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Invalid new value for limit '" + limitName +"': " + value);
    }
    switch (limitName) {
      case SolrCache.MAX_SIZE_PARAM:
        component.setMaxSize(value.intValue());
        break;
      case SolrCache.MAX_RAM_MB_PARAM:
        component.setMaxRamMB(value.intValue());
        break;
      default:
        throw new IllegalArgumentException("Unsupported limit name '" + limitName + "'");
    }
    return value.intValue();
  }

  @Override
  public Map<String, Object> getResourceLimits(SolrCache component) {
    Map<String, Object> limits = new HashMap<>();
    limits.put(SolrCache.MAX_SIZE_PARAM, component.getMaxSize());
    limits.put(SolrCache.MAX_RAM_MB_PARAM, component.getMaxRamMB());
    return limits;
  }

  private static final String[] MONITORED_KEYS = new String[] {
      SolrCache.HITS_PARAM,
      SolrCache.HIT_RATIO_PARAM,
      SolrCache.LOOKUPS_PARAM,
      SolrCache.EVICTIONS_PARAM
  };

  @Override
  public Map<String, Object> getMonitoredValues(SolrCache component) throws Exception {
    Map<String, Object> values = new HashMap<>();
    values.put(SolrCache.SIZE_PARAM, component.size());
    values.put(SolrCache.RAM_BYTES_USED_PARAM, component.ramBytesUsed());
    // add also some useful stats for optimization
    SolrMetricsContext metricsContext = component.getSolrMetricsContext();
    if (metricsContext != null) {
      Map<String, Object> metrics = metricsContext.getMetricsSnapshot();
      String keyPrefix = component.getCategory().toString() + "." + metricsContext.getScope() + ".";
      for (String k : MONITORED_KEYS) {
        String key = keyPrefix + k;
        values.put(k, metrics.get(key));
        key = keyPrefix + SolrCache.CUMULATIVE_PREFIX + k;
        values.put(SolrCache.CUMULATIVE_PREFIX + k, metrics.get(key));
      }
    }
    return values;
  }

  @Override
  protected void doManage() throws Exception {
    // a cache of the latest current values
    AtomicReference<Map<String, Map<String, Object>>> currentValuesRef = new AtomicReference<>();

    // pool limits are defined using controlled tags
    poolLimits.forEach((poolLimitName, value) -> {
      List<SolrCache> adjustableComponents = new ArrayList<>();
      components.forEach((name, component) -> {
        Map<String, Object> resourceLimits = getResourceLimits((SolrCache) component);
        Object limit = resourceLimits.get(poolLimitName);
        // XXX we could attempt here to control eg. ramBytesUsed by adjusting maxSize limit
        // XXX and vice versa if the current limit is undefined or unsupported
        if (limit == null || !(limit instanceof Number)) {
          return;
        }
        double currentResourceLimit = ((Number)limit).doubleValue();
        if (currentResourceLimit <= 0) { // undefined or unsupported
          return;
        }
        adjustableComponents.add(component);
      });
      // nothing to manage?
      if (adjustableComponents.isEmpty()) {
        return;
      }
      Map<String, Map<String, Object>> currentValues;
      if (currentValuesRef.get() == null) {
        try {
          currentValues = getCurrentValues();
          currentValuesRef.set(currentValues);
        } catch (InterruptedException e) {
          log.warn("Unable to retrieve current values in pool " + getName() + " / " + getType());
          return;
        }
      } else {
        currentValues = currentValuesRef.get();
      }
      Map<String, Object> totalValues = aggregateTotalValues(currentValues);
      // only numeric limits are supported
      if (value == null || !(value instanceof Number)) {
        return;
      }
      double poolLimitValue = ((Number)value).doubleValue();
      if (poolLimitValue <= 0) {
        return;
      }
      Function<Map<String, Object>, Double> func = controlledToMonitored.get(poolLimitName);
      if (func == null) {
        return;
      }
      Double totalValue = func.apply(totalValues);
      if (totalValue.doubleValue() <= 0.0) {
        return;
      }
      double totalDelta = poolLimitValue - totalValue.doubleValue();

      // dead band to avoid thrashing
      if (Math.abs(totalDelta / poolLimitValue) < deadBand) {
        return;
      }
      // may update currentValuesRef as a side-effect
      adjust(adjustableComponents, currentValuesRef, poolLimitName, poolLimitValue, totalValue.doubleValue());
    });
  }

  /**
   * Manage all eligible components that support this pool limit.
   */
  private void adjust(final List<SolrCache> components, AtomicReference<Map<String, Map<String, Object>>> currentValuesRef, String limitName,
                      double poolLimitValue, double totalValue) {
    log.info("* Pool {} / {} running adjustment for {} components.", getName(), getType(), components.size());
    // changeRatio > 1.0 means there are available free resources
    // changeRatio < 1.0 means there's shortage of resources
    final AtomicReference<Double> changeRatio = new AtomicReference<>(poolLimitValue / totalValue);
    log.info("-- initial changeRatio={}, initial total usage: {}", changeRatio.get(), totalValue);
    AtomicBoolean optAdjusted = new AtomicBoolean();
    AtomicBoolean forceAdjusted = new AtomicBoolean();

    // ========================== OPTIMIZATION ==============================
    // if the situation is not critical (ie. total consumption is less than max)
    // try to proactively optimize by reducing the size of caches with too high hitRatio
    // (because a lower hit ratio is still acceptable if it means saving resources) and
    // expand the size of caches with too low hitRatio
    components.forEach(component -> {
      if (!optimize) {
        return;
      }
      long currentLookups = ((Number)currentValuesRef.get().get(component.getManagedComponentId().toString()).get(SolrCache.CUMULATIVE_PREFIX + SolrCache.LOOKUPS_PARAM)).longValue();
      long lastLookups = lookups.computeIfAbsent(component.getManagedComponentId().toString(), k -> 0L);
      if (currentLookups < lastLookups + lookupDelta) {
        // too little data, skip the optimization
        return;
      }
      Map<String, Object> resourceLimits = getResourceLimits(component);
      double currentLimit = ((Number)resourceLimits.get(limitName)).doubleValue();

      // calculate the hit ratio since the last adjustment.
      // NOTE: we don't use the hitratio reported by the cache because it's either a cumulative total
      // or a short-term value since the last commit. We want a value that represents the period since the
      // last optimization
      long currentHits = ((Number)currentValuesRef.get().get(component.getManagedComponentId().toString()).get(SolrCache.CUMULATIVE_PREFIX + SolrCache.HITS_PARAM)).longValue();
      long lastHits = hits.computeIfAbsent(component.getManagedComponentId().toString(), k -> 0L);
      long currentHitsDelta = currentHits - lastHits;
      long currentLookupsDelta = currentLookups - lastLookups;
      double currentHitRatio = (double)currentHitsDelta / (double)currentLookupsDelta;

      Number initialLimit = (Number)initialComponentLimits.get(component.getManagedComponentId().toString()).get(limitName);
      if (initialLimit == null) {
        // can't optimize because we don't know how far off we are from the initial setting
        return;
      }
      if (currentHitRatio < targetHitRatio) {
        // EXPAND ?
        if (!optimize_expand) {
          return;
        }
        if (changeRatio.get() <= 1.0) {
          // don't expand if we're already short on resources
          return;
        }
        long currentEvictions = ((Number)currentValuesRef.get().get(component.getManagedComponentId().toString()).get(SolrCache.EVICTIONS_PARAM)).longValue();
        if (currentEvictions <= 0) {
          // don't expand - there are no evictions yet so all items fit in the current size
          return;
        }
        // expand to increase the hitRatio, but not more than maxAdjustRatio from the initialLimit
        double newLimit = currentLimit * changeRatio.get();
        if (newLimit > initialLimit.doubleValue() * maxAdjustRatio) {
          // don't expand ad infinitum
          newLimit = initialLimit.doubleValue() * maxAdjustRatio;
        }
        if (newLimit > poolLimitValue) {
          // don't expand above the total pool limit
          newLimit = poolLimitValue;
        }
        if (newLimit <= currentLimit) {
          return;
        }
        try {
          Number actualNewLimit = (Number)setResourceLimit(component, limitName, newLimit, ChangeListener.Reason.OPTIMIZATION);
          log.info("--- optimized {}: {} -> {}", component.getManagedComponentId(), currentLimit, actualNewLimit);
          lookups.put(component.getManagedComponentId().toString(), currentLookups);
          hits.put(component.getManagedComponentId().toString(), currentHits);
          optAdjusted.set(true);
        } catch (Exception e) {
          log.warn("Failed to set managed limit " + limitName +
              " from " + currentLimit + " to " + newLimit + " on " + component.getManagedComponentId(), e);
        }
      } else {
        // SHRINK ?
        if (!optimize_shrink) {
          return;
        }
        // shrink to release some resources but not more than maxAdjustRatio from the initialLimit
        double newLimit = targetHitRatio / currentHitRatio * currentLimit;
        if (newLimit * maxAdjustRatio < initialLimit.doubleValue()) {
          // don't shrink ad infinitum
          return;
        }
        if (newLimit < 1.0) {
          newLimit = 1.0;
        }
        if (newLimit >= currentLimit) {
          return;
        }
        try {
          Number actualNewLimit = (Number)setResourceLimit(component, limitName, newLimit, ChangeListener.Reason.OPTIMIZATION);
          log.info("--- optimized {}: {} -> {}", component.getManagedComponentId(), currentLimit, actualNewLimit);
          lookups.put(component.getManagedComponentId().toString(), currentLookups);
          hits.put(component.getManagedComponentId().toString(), currentHits);
          optAdjusted.set(true);
        } catch (Exception e) {
          log.warn("Failed to set managed limit " + limitName +
              " from " + currentLimit + " to " + newLimit + " on " + component.getManagedComponentId(), e);
        }
      }
    });
    if (optAdjusted.get()) {
      // update total value
      try {
        Map<String, Map<String, Object>> newCurrentValues = getCurrentValues();
        currentValuesRef.set(newCurrentValues);
        Map<String, Object> newTotalValues = aggregateTotalValues(currentValuesRef.get());
        totalValue = controlledToMonitored.get(limitName).apply(newTotalValues);
      } catch (Exception e) {
        log.warn("Unable to retrieve updated current values for pool " + getName() + " / " + getType(), e);
      }
      log.info("--- components limit " + limitName + " optimized, new total usage: " + totalValue);
    } else {
      log.info("--- components limit " + limitName + " optimization skipped.");
    }

    // ======================== HARD LIMIT ================
    // now re-calculate the new changeRatio based on possible
    // optimizations made above
    double totalDelta = poolLimitValue - totalValue;

    // dead band to avoid thrashing
    if (Math.abs(totalDelta / poolLimitValue) < deadBand) {
      log.info("-- delta " + totalDelta + " within deadband, skipping...");
      return;
    }

    changeRatio.set(poolLimitValue / totalValue);
    log.info("-- updated changeRatio=" + changeRatio.get());
    if (changeRatio.get() >= 1.0) { // there's no resource shortage
      log.info("--- no shortage, skipping forcible adjustment...");
      return;
    }
    // forcibly trim each resource limit (evenly) to fit within the total pool limit
    components.forEach(component -> {
      Map<String, Object> resourceLimits = getResourceLimits(component);
      double currentLimit = ((Number)resourceLimits.get(limitName)).doubleValue();
      double newLimit = currentLimit * changeRatio.get();
      // don't shrink it to 0
      if (newLimit < 1.0) {
        newLimit = 1.0;
      }
      try {
        Number actualNewLimit = (Number) setResourceLimit(component, limitName, newLimit, ChangeListener.Reason.ABOVE_TOTAL_LIMIT);
        log.info("--- forced " + component.getManagedComponentId() + " / " + limitName + ": " + currentLimit + " -> " + actualNewLimit);
        long currentLookups = ((Number)currentValuesRef.get().get(component.getManagedComponentId().toString()).get(SolrCache.CUMULATIVE_PREFIX + SolrCache.LOOKUPS_PARAM)).longValue();
        long currentHits = ((Number)currentValuesRef.get().get(component.getManagedComponentId().toString()).get(SolrCache.CUMULATIVE_PREFIX + SolrCache.HITS_PARAM)).longValue();
        lookups.put(component.getManagedComponentId().toString(), currentLookups);
        hits.put(component.getManagedComponentId().toString(), currentHits);
        forceAdjusted.set(true);
      } catch (Exception e) {
        log.warn("Failed to set managed limit " + limitName +
            " from " + currentLimit + " to " + newLimit + " on " + component.getManagedComponentId(), e);
      }
    });
    if (forceAdjusted.get()) {
      // update total value
      try {
        Map<String, Map<String, Object>> newCurrentValues = getCurrentValues();
        currentValuesRef.set(newCurrentValues);
        Map<String, Object> newTotalValues = aggregateTotalValues(currentValuesRef.get());
        totalValue = controlledToMonitored.get(limitName).apply(newTotalValues);
      } catch (Exception e) {
        log.warn("Unable to retrieve updated current values for pool " + getName() + " / " + getType(), e);
      }
      log.info("-- components limit " + limitName + " forcibly adjusted, new total usage: " + totalValue);
    } else {
      log.info("-- components limit " + limitName + " unchanged");
    }
    // check that the adjustments were overall successful
    // NOTE: since controlled != monitored we may need a few rounds to actually observe the
    // effect of the changes, so don't panic immediately ;)
    if (poolLimitValue < totalValue) {
      reportExceededCounter++;
      if (reportExceededCounter >= reportExceededThreshold && (reportExceededCounter % reportExceededThreshold) == 0) {
        log.warn("Pool {} / {}: after {} attempts still unable to force the total {} resource usage {} to fit in the total pool limit of {} !",
            getName(), getType(), reportExceededCounter, limitName, totalValue, poolLimitValue);
      }
    } else {
      reportExceededCounter = 0;
    }
  }
}
