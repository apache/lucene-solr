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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

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
 * which can be adjusted using configuration parameter {@link #DEAD_BAND}. If monitored values don't
 * exceed the limits +/- the dead band then no action is taken.</p>
 */
public class CacheManagerPool extends ResourceManagerPool<SolrCache> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String TYPE = "cache";

  public static final String DEAD_BAND = "deadBand";
  public static final double DEFAULT_DEAD_BAND = 0.1;

  protected static final Map<String, Function<Map<String, Object>, Double>> controlledToMonitored = new HashMap<>();

  static {
    controlledToMonitored.put(SolrCache.MAX_RAM_MB_PARAM, values -> {
      Number ramBytes = (Number) values.get(SolrCache.RAM_BYTES_USED_PARAM);
      return ramBytes != null ? ramBytes.doubleValue() / SolrCache.MB : 0.0;
    });
    controlledToMonitored.put(SolrCache.MAX_SIZE_PARAM, values ->
        ((Number)values.getOrDefault(SolrCache.MAX_SIZE_PARAM, -1.0)).doubleValue());
  }

  protected double deadBand = DEFAULT_DEAD_BAND;

  public CacheManagerPool(String name, String type, ResourceManager resourceManager, Map<String, Object> poolLimits, Map<String, Object> poolParams) {
    super(name, type, resourceManager, poolLimits, poolParams);
    String deadBandStr = String.valueOf(poolParams.getOrDefault(DEAD_BAND, DEFAULT_DEAD_BAND));
    try {
      deadBand = Double.parseDouble(deadBandStr);
    } catch (Exception e) {
      log.warn("Invalid deadBand parameter value '" + deadBandStr + "', using default " + DEFAULT_DEAD_BAND);
    }
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

  @Override
  public Map<String, Object> getMonitoredValues(SolrCache component) throws Exception {
    Map<String, Object> values = new HashMap<>();
    values.put(SolrCache.SIZE_PARAM, component.size());
    values.put(SolrCache.RAM_BYTES_USED_PARAM, component.ramBytesUsed());
    SolrMetricsContext metricsContext = component.getSolrMetricsContext();
    if (metricsContext != null) {
      Map<String, Object> metrics = metricsContext.getMetricsSnapshot();
      String hitRatioKey = component.getCategory().toString() + "." + metricsContext.getScope() + "." + SolrCache.HIT_RATIO_PARAM;
      values.put(SolrCache.HIT_RATIO_PARAM, metrics.get(hitRatioKey));
    }
    return values;
  }

  @Override
  protected void doManage() throws Exception {
    Map<String, Map<String, Object>> currentValues = getCurrentValues();
    Map<String, Object> totalValues = aggregateTotalValues(currentValues);
    // pool limits are defined using controlled tags
    poolLimits.forEach((poolLimitName, value) -> {
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

      double changeRatio = poolLimitValue / totalValue.doubleValue();
      // modify evenly every component's current limits by the changeRatio
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
        double newLimit = currentResourceLimit * changeRatio;
        try {
          setResourceLimit((SolrCache) component, poolLimitName, newLimit);
        } catch (Exception e) {
          log.warn("Failed to set managed limit " + poolLimitName +
              " from " + currentResourceLimit + " to " + newLimit + " on " + component.getManagedComponentId(), e);
        }
      });
    });
  }
}
